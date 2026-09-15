# Adobe Analytics 2.0 API 호출과 DB 적재.
#
# 요청 페이로드 조립, 호출 한도 대응, 재시도, 응답 정규화, 테이블 적재까지.
# 수집 단위 계산은 actRunner, 작업 실행은 actExecute가 맡는다.
#
# encoding, create_engine(), apply & map, limit function
# 2nd Breakdown w/ total value, jsonToDB + refinedFrame to refinedFrame1
# EmptyDataError
# 251212 worker_refine_common 추가, RetryableServerError() 추가
# 260417 DB 엔진 싱글톤 패턴 적용 및 커넥션 풀 최적화
# 260727 아래 항목 수정
#   - 시각 지정 시 조회 구간/라벨이 하루 밀리던 문제 (TimeWindow로 통합)
#   - jsonDateChange가 세그먼트 필터까지 오염시키던 문제
#   - capacityMetadata 없는 JSON에서 KeyError
#   - 재시도 시 중복 적재 (stackTodb 멱등 적재 옵션)
#   - dataInitiator 매 호출 재인증
#   - worker_refine_common 인자 불일치 (호출 즉시 TypeError)
#   - unicodeCompile_df 전 컬럼 셀 단위 순회
#   - 데이터 없음(EmptyDataError)과 API 오류를 구분, 0 채우기 지원
#   - ChangeItemID 다단(N단) breakdown 지원
#   - hourly 수집 및 시간대 완결성 보장

import contextlib
import hashlib
import io
import json
import logging
import os
import random
import re
import sys
import unicodedata
import threading
import time
from copy import deepcopy
from datetime import datetime, timedelta

import aanalytics2 as api2
import pandas as pd
from sqlalchemy import create_engine, inspect as sa_inspect, pool, text
from sqlalchemy.pool import NullPool

try:
    from .actRunner import (TimeWindow, buildDateRange, periodSlicer,
                            expandSiteCodes, _parse_date, _parse_hour,
                            DEPRECATED_SITE_CODES)
except ImportError:
    from actRunner import (TimeWindow, buildDateRange, periodSlicer,
                           expandSiteCodes, _parse_date, _parse_hour,
                           DEPRECATED_SITE_CODES)

# ---------------------------------------------------------------------------
# 프로파일로 주입되는 값 (profile.load_profile 참고)
# ---------------------------------------------------------------------------
# 전체 통합 리포트스위트. 여러 국가가 한 RS로 들어오는 경우 site_code를
# 응답에서 뽑아야 하므로 일반 RS와 처리 경로가 다르다.
TOTAL_RSID = ""
TOTAL_SITE_CODE = ""

# EPP 트래픽이 일반 RS에 통합돼 들어오는 리포트스위트 (is_epp_integ = Y)
EPP_INTEGRATED_RSIDS = ()

# 리포트가 site_code를 차원으로 돌려주는 리포트스위트.
# 이 RS들은 site_code를 rsid에서 추론하지 않고 응답에서 뽑는다.
SITE_CODE_DIMENSION_RSIDS = ()

# Adobe 인증 설정 파일(JSON) 경로. 환경변수 AA_AUTH_CONFIG로도 지정할 수 있다.
AUTH_CONFIG_PATH = ""


def _site_code_of(rsid):
    """rsid -> site_code. 프로파일이 주입되면 교체된다."""
    return str(rsid)


def _isTotalRsid(rsid):
    """전체 통합 RS인지. 프로파일이 없으면 항상 False다."""
    return bool(TOTAL_RSID) and rsid == TOTAL_RSID


def _authConfigPath():
    path = AUTH_CONFIG_PATH or os.environ.get("AA_AUTH_CONFIG", "")
    if not path:
        raise RuntimeError(
            "Adobe 인증 설정 파일 경로가 없습니다. 환경변수 AA_AUTH_CONFIG에 "
            "설정 JSON 경로를 넣거나 actModuler.AUTH_CONFIG_PATH를 지정하세요.")
    return path


logger = logging.getLogger("act")
# 자체 핸들러를 붙이므로 루트로 전파하지 않는다.
# (사용자가 logging.basicConfig()를 부르면 같은 줄이 두 번 찍힌다)
logger.propagate = False
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)

# Adobe 조직(company) ID. 환경변수 ACT_COMPANY_ID 또는 프로파일로 지정한다.
COMPANY_ID = os.environ.get("ACT_COMPANY_ID", "")

# ---------------------------------------------------------------------------
# 통합(MST) 리포트 필터용 site_code 목록 — 프로파일로 주입된다
# ---------------------------------------------------------------------------
# 통합 리포트는 모든 국가 행을 한꺼번에 돌려주므로, 원하는 site_code만
# 남기려면 유효한 코드 목록이 필요하다. 어떤 코드가 있는지는 조직마다
# 다르므로 비어 있는 채로 배포하고 프로파일이 채운다.
# (비어 있으면 filterSiteCode는 아무것도 남기지 않으니 주의)

_DEFAULT_SITE_CODES = []



# ---------------------------------------------------------------------------
# 예외
# ---------------------------------------------------------------------------

class RetryableServerError(Exception):
    """재시도하면 성공할 수 있는 오류 (5xx, 타임아웃, 스로틀링)."""


class EmptyDataError(Exception):
    """호출은 성공했으나 데이터가 없음. 오류가 아니라 정상 상태다."""


class TransientAPIError(RetryableServerError):
    """서버·네트워크 문제로 실패. 재시도 대상.

    RetryableServerError 를 상속하므로 기존 except 절이 그대로 잡는다.
    """


class TokenError(RetryableServerError):
    """인증 실패(401/403). 토큰을 다시 받은 뒤 재시도한다."""


class PermanentError(Exception):
    """재시도해도 결과가 달라지지 않는 오류 (스키마/설정/인증)."""


class PartialDataError(Exception):
    """페이지네이션이 중간에 끊겨 데이터가 불완전하다.

    aanalytics2가 다음을 출력하는 상황이다.
        Warning : No data returned & lastPage is False.
        Exit the loop - no save file & empty dataframe.

    lastPage가 False인데 응답이 비었다는 것은 '데이터가 없음'이 아니라
    '더 가져올 페이지가 있는데 못 가져왔음'이다. EmptyDataError로 처리해
    건너뛰면 데이터를 조용히 잃으므로, 이 호출만 재시도한다.
    """


# 재호출로 해결되지 않는 예외 유형. 이것들을 5회 반복하는 것은 시간 낭비다.
PERMANENT_TYPES = (ValueError, KeyError, TypeError, AttributeError,
                   IndexError, FileNotFoundError, json.JSONDecodeError)

# 위 유형이지만 실제로는 일시적 장애인 경우.
#
# aanalytics2는 응답이 JSON이 아니면(500 HTML 페이지, 게이트웨이 타임아웃,
# 토큰 만료 리다이렉트, 429 본문 없음) postData가 requests.Response 객체를
# 그대로 돌려주고, 라이브러리가 res.get(...)을 호출하다 AttributeError를 낸다.
#
#     AttributeError: 'Response' object has no attribute 'get'
#
# 타입만 보면 코드 버그처럼 보이지만 원인은 서버·인증 쪽이라 재시도하면 풀린다.
# 영구 오류로 분류하면 그 구간이 통째로 건너뛰어진다.
_TRANSIENT_MSG = re.compile(
    r"'Response' object has no attribute"      # 응답이 JSON이 아님
    r"|object is not subscriptable"            # 같은 원인, 다른 표기
    r"|Expecting value"                        # 빈 본문 JSON 파싱 실패
    r"|Extra data"                             # 잘린 JSON
    r"|timed?\s*out|timeout"
    r"|connection (aborted|reset|refused)"
    r"|remote end closed"
    r"|502|503|504|429",
    re.IGNORECASE)

# 인증 만료로 보이는 신호. 재시도 전에 토큰을 다시 받는다.
_AUTH_MSG = re.compile(r"401|403|unauthorized|forbidden|token|expired",
                       re.IGNORECASE)


def _looksTransient(exc):
    return bool(_TRANSIENT_MSG.search(f"{type(exc).__name__}: {exc}"))


def _looksAuth(exc):
    return bool(_AUTH_MSG.search(f"{type(exc).__name__}: {exc}"))


# ---------------------------------------------------------------------------
# 초기화 / 호출 제어
# ---------------------------------------------------------------------------

_init_lock = threading.Lock()
_initialized = False


def dataInitiator(force=False):
    """[수정] 프로세스당 1회만 인증한다.

    기존에는 모든 수집 함수가 매 호출마다 이 함수를 불러
    설정 파일을 다시 읽고 재인증했다. 병렬 10워커 x 수백 태스크에서
    가장 큰 오버헤드였다.
    """
    global _initialized
    if _initialized and not force:
        return
    with _init_lock:
        if _initialized and not force:
            return
        api2.importConfigFile(_authConfigPath())
        logger_obj = api2.Login()
        logger_obj.connector.config
        _initialized = True


class _TokenBucket:
    """Adobe 2.0 API 호출 한도 대응 (스레드 안전)."""

    def __init__(self, rate_per_minute=100):
        self._cap = float(rate_per_minute)
        self._tokens = float(rate_per_minute)
        self._rate = rate_per_minute / 60.0
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self._cap, self._tokens + (now - self._last) * self._rate)
                self._last = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                wait = (1 - self._tokens) / self._rate
            time.sleep(wait)


_bucket = _TokenBucket(int(os.environ.get("ACT_RATE_PER_MINUTE", "100")))


def setRateLimit(rate_per_minute):
    global _bucket
    _bucket = _TokenBucket(rate_per_minute)


def _sleepBeforeRetry(exc, attempt, max_retries, base_delay, label, note=""):
    """지수 백오프 + 지터로 대기한다. 인증 문제면 토큰을 다시 받는다."""
    if isinstance(exc, TokenError) or _looksAuth(exc):
        logger.warning("[%s] 인증 만료로 보입니다. 토큰을 다시 받습니다.", label)
        try:
            dataInitiator(force=True)
        except Exception as re_err:
            logger.error("[%s] 재인증 실패: %s", label, re_err)
    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
    logger.warning("[%s] 일시 오류 (%d/%d), %.1fs 후 재시도: %s: %s%s",
                   label, attempt + 1, max_retries, delay,
                   type(exc).__name__, exc, f" | {note}" if note else "")
    time.sleep(delay)


def callWithRetry(func, *args, max_retries=5, base_delay=2.0, label="", **kwargs):
    """[수정] 오류를 분류하고 지수 백오프로 재시도한다.

    기존 패턴:
        except Exception as e: print("Error occurred."); retry_count += 1
      - 예외 내용을 버려 원인 파악 불가 (토큰 만료도, 컬럼 불일치도 같은 문구)
      - 컬럼 길이 불일치 같은 영구 오류를 5회 반복
      - 백오프가 없어 429 스로틀링을 악화
    """
    last = None
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except EmptyDataError:
            raise                                   # 데이터 없음은 재시도 대상이 아니다
        except PERMANENT_TYPES as e:
            # 타입은 영구 오류지만 메시지가 서버·네트워크 문제를 가리키면 재시도한다.
            if not _looksTransient(e):
                logger.error("[%s] 영구 오류 — 재시도 중단: %s: %s",
                             label, type(e).__name__, e)
                raise PermanentError(f"{type(e).__name__}: {e}") from e
            last = e
            if attempt >= max_retries:
                break
            _sleepBeforeRetry(e, attempt, max_retries, base_delay, label,
                              note="응답이 JSON이 아님 — 서버/인증 문제로 보임")
        except Exception as e:
            last = e
            if attempt >= max_retries:
                break
            _sleepBeforeRetry(e, attempt, max_retries, base_delay, label)
    logger.error("[%s] 재시도 %d회 소진: %s: %s", label, max_retries,
                 type(last).__name__, last)
    raise RetryableServerError(f"[{label}] {type(last).__name__}: {last}")


# ---------------------------------------------------------------------------
# 데이터 조회
# ---------------------------------------------------------------------------

def dataReportSuites():
    dataInitiator()
    ags = api2.Analytics(COMPANY_ID)
    ags.header
    rsids = ags.getReportSuites()
    print(rsids)
    return rsids


def _isEmpty(df):
    return df is None or (hasattr(df, "empty") and df.empty)


class PaginationError(Exception):
    """페이지네이션 결과가 불완전하다 (누락 또는 과다)."""


# ---------------------------------------------------------------------------
# 안전 페이지네이션
# ---------------------------------------------------------------------------
# 왜 필요한가
# -----------
# aanalytics2._readData 는 응답 행을 dimension value 를 키로 한 dict 로 합친다.
#     dict_data = {row['value']: row['data'] for row in data_rows}
# Adobe 리포트는 metric 기준으로 정렬되는데, metric 값이 같은 행이 많으면
# (0, 1, 2 같은 값은 수만 건이 동점) 요청마다 동점 구간의 순서가 달라진다.
# 그러면 페이지 경계가 매번 다른 곳을 자르게 되어
#   - 어떤 행은 두 페이지에 걸쳐 중복 수신되고
#   - 어떤 행은 어느 페이지에도 나오지 않는다
# dict 병합이 중복을 조용히 덮어쓰기 때문에 최종 행 수는 그럴듯해 보이지만
# 누락된 행은 그대로 사라진다. 오류도 경고도 나지 않는다.
#
# 21만 행 리포트 모의 결과: 34,879건(16.5%) 손실, 로그상 정상 종료.
#
# 대응
# ----
#   1) dimensionSort 로 정렬 기준을 dimension 값으로 바꿔 동점 자체를 없앤다
#   2) 페이지를 직접 돌고 value 가 아니라 itemId 로 병합한다
#   3) 응답의 totalElements 와 수집 결과를 대조해 누락을 검출한다

SAFE_PAGING = os.environ.get("ACT_SAFE_PAGING", "1") not in ("0", "false", "False")


_AUTH_CODES = (401, 403)


def inspectResponse(res, label=""):
    """Adobe 응답을 검사해 dict 면 그대로, 아니면 적절한 예외로 바꾼다.

    aanalytics2 는 응답이 JSON 이 아니면 requests.Response 를 그대로 돌려주고,
    라이브러리가 res.get(...) 을 부르다 AttributeError 를 낸다. 그 전에 잡는다.

        401/403      -> TokenError        (토큰 재발급 후 재시도)
        그 외 4xx/5xx -> TransientAPIError (백오프 후 재시도)
    """
    # requests.Response 형태 (dict 가 아닌데 status_code 를 가짐)
    if not isinstance(res, dict) and hasattr(res, "status_code"):
        code = res.status_code
        body = (getattr(res, "text", "") or "")[:200].replace("\n", " ")
        if code in _AUTH_CODES:
            raise TokenError(f"[{label}] HTTP {code}: 토큰 재발급 필요 — {body}")
        raise TransientAPIError(f"[{label}] HTTP {code}: {body}")

    if not isinstance(res, dict):
        raise TransientAPIError(
            f"[{label}] 예상치 못한 반환형 {type(res).__name__} — "
            f"응답이 JSON 이 아닙니다.")

    # dict 인데 오류를 담고 있는 경우
    code = res.get("status_code", res.get("statusCode"))
    try:
        code = int(code) if code is not None else None
    except (TypeError, ValueError):
        code = None
    if code is not None and code >= 400:
        body = str(res.get("errorDescription")
                   or res.get("error_description") or res)[:200]
        if code in _AUTH_CODES:
            raise TokenError(f"[{label}] HTTP {code}: 토큰 재발급 필요 — {body}")
        raise TransientAPIError(f"[{label}] HTTP {code}: {body}")

    if res.get("errorCode") or res.get("error_code"):
        ec = res.get("errorCode") or res.get("error_code")
        body = str(res.get("errorDescription")
                   or res.get("error_description") or "")[:200]
        if str(ec).lower() in ("unauthorized", "forbidden", "invalid_token",
                               "expired_token"):
            raise TokenError(f"[{label}] {ec}: 토큰 재발급 필요 — {body}")
        raise TransientAPIError(f"[{label}] Adobe 오류 {ec}: {body}")

    return res


def _postReport(ags, body):
    """aanalytics2 커넥터로 /reports 를 직접 호출한다 (페이징은 우리가 제어)."""
    conn = getattr(ags, "connector", None)
    if conn is None or not hasattr(conn, "postData"):
        raise PermanentError(
            "aanalytics2 내부 구조가 예상과 다릅니다(connector.postData 없음). "
            "safe_paging=False 로 기존 경로를 쓰거나 라이브러리 버전을 확인하세요.")
    endpoint = getattr(ags, "endpoint_company", "") or ""
    return conn.postData(endpoint + "/reports", data=body, headers=ags.header)


def _postReportChecked(ags, body, label=""):
    """응답을 받아 즉시 검사한다. 여기서 Response 를 잡으므로
    라이브러리 내부의 AttributeError 로 번지지 않는다."""
    return inspectResponse(_postReport(ags, body), label)


def fetchAllRows(payload, page_size=50000, stable_sort="auto", max_pages=500,
                 label="", strict=False):
    """페이지를 직접 돌며 원시 행을 모은다.

    반환: (rows, meta)
      rows : [{"itemId":..., "value":..., "data":[...]}, ...]  itemId 기준 중복 제거
      meta : {"expected","received","unique","duplicated","missing","pages",...}

    strict=True 면 누락이 있을 때 PaginationError 를 던진다.
    """
    dataInitiator()
    ags = api2.Analytics(COMPANY_ID)
    ags.header

    body = deepcopy(payload)
    settings = body.setdefault("settings", {})
    settings["limit"] = page_size
    settings.setdefault("countRepeatInstances", True)

    # 정렬 기준을 dimension 값으로 바꾸면 동점이 사라져 페이지 경계가 안정된다.
    # 다만 이는 리포트의 정렬 순서를 바꾸므로 상위 N개를 뽑는 용도에서는
    # "metric 상위 N"이 "값 오름차순 N"으로 바뀌어 결과가 달라진다.
    #
    #   stable_sort="auto"(기본) : 1페이지로 끝나면 원래 정렬 유지(top-N 의미 보존),
    #                              여러 페이지면 그때만 안정 정렬로 다시 받는다
    #   True / False             : 강제 지정
    force_stable = (stable_sort is True)
    auto = (stable_sort == "auto")
    if force_stable:
        settings["dimensionSort"] = "asc"

    rows, seen, dup = [], {}, 0
    expected = None
    page = 0
    restarted = False

    while page < max_pages:
        settings["page"] = page
        _bucket.acquire()
        # 응답을 받는 즉시 검사한다 (Response 객체 / 4xx / 5xx / errorCode)
        resp = _postReportChecked(ags, body, label)

        if expected is None:
            expected = resp.get("totalElements")

        page_rows = resp.get("rows") or []
        for r in page_rows:
            key = r.get("itemId") or r.get("value")
            if key in seen:
                dup += 1
                continue
            seen[key] = True
            rows.append(r)

        if resp.get("lastPage", True) or not page_rows:
            break

        # 여러 페이지가 확정된 시점에만 안정 정렬로 전환한다.
        # 1페이지로 끝나는 리포트는 경계 문제가 없으므로 정렬을 건드리지 않는다.
        if auto and not restarted:
            restarted = True
            settings["dimensionSort"] = "asc"
            logger.info("[%s] 총 %s행 — 여러 페이지이므로 안정 정렬(dimensionSort)로 "
                        "다시 받습니다. 상위 N 추출이 목적이면 stable_sort=False 로 "
                        "원래 정렬을 유지하세요.",
                        label, f"{expected:,}" if isinstance(expected, int) else "?")
            rows, seen, dup, page = [], {}, 0, 0
            continue

        page += 1
    else:
        logger.warning("[%s] 최대 페이지 수(%d)에 도달했습니다. 결과가 잘렸을 수 있습니다.",
                       label, max_pages)

    received = len(rows) + dup
    missing = (expected - len(rows)) if isinstance(expected, int) else None
    meta = {"expected": expected, "received": received, "unique": len(rows),
            "duplicated": dup, "missing": missing, "pages": page + 1,
            "stable_sort": bool(force_stable or restarted)}

    if dup:
        logger.info("[%s] 페이지 간 중복 %s건 제거 (itemId 기준)", label, f"{dup:,}")
    if missing:
        msg = (f"[{label}] 행 누락 {missing:,}건 "
               f"(Adobe 총계 {expected:,} / 수집 {len(rows):,}). "
               f"metric 동점으로 페이지 경계가 흔들렸을 가능성이 큽니다. "
               f"기간을 더 잘게 나누거나(period='daily') "
               f"page_size 를 줄여 다시 시도하세요.")
        if strict:
            raise PaginationError(msg)
        logger.error(msg)
    elif isinstance(expected, int):
        logger.info("[%s] 완결성 확인: Adobe 총계 %s = 수집 %s (페이지 %d)",
                    label, f"{expected:,}", f"{len(rows):,}", meta["pages"])
    return rows, meta


def auditReportCompleteness(jsonFile, startDate, endDate, period="all",
                            start_hour="00:00", end_hour="00:00",
                            page_size=50000, label=""):
    """이 리포트가 페이지 손실에 노출되는지 진단한다 (적재 없음).

    Adobe 가 알려주는 총 행 수(totalElements)와, 안정 정렬 없이 받았을 때
    실제로 몇 건이 빠지는지를 비교한다. 과거 적재분의 손실 규모를 가늠하는 용도다.

        auditReportCompleteness("precampaign.json", "2026-08-01", "2026-08-31")
    """
    window = makeWindow(startDate, endDate, period, start_hour, end_hour)
    payload = applyWindow(jsonFile, window)
    label = label or payload.get("rsid", "")

    _, unstable = fetchAllRows(payload, page_size, stable_sort=False, label=label)
    total = unstable["expected"]
    if not isinstance(total, int):
        logger.warning("[%s] totalElements 를 확인할 수 없어 진단이 불가합니다.", label)
        return unstable

    if total <= page_size:
        logger.info("[%s] 총 %s행 — 1페이지로 끝나므로 손실 위험 없음.",
                    label, f"{total:,}")
        return unstable

    _, stable = fetchAllRows(payload, page_size, stable_sort=True, label=label)
    lost = unstable.get("missing") or 0
    logger.warning(
        "[%s] 총 %s행 / %d페이지 — 기존 방식에서 %s행(%.1f%%) 손실 추정. "
        "안정 정렬 사용 시 %s행 수집.",
        label, f"{total:,}", unstable["pages"], f"{lost:,}",
        100.0 * lost / total, f"{stable['unique']:,}")
    return {"total": total, "legacy_unique": unstable["unique"],
            "legacy_missing": lost, "safe_unique": stable["unique"],
            "pages": unstable["pages"],
            "loss_pct": round(100.0 * lost / total, 2)}


def buildFrameFromRows(rows, item_id=False):
    """원시 행을 DataFrame으로. 열 구성은 기존 경로와 동일하게 맞춘다.

        [dimension value, metric1, ..., metricN] (+ item_id)
    """
    records = []
    for r in rows:
        rec = [r.get("value", "missing_value")] + list(r.get("data", []))
        if item_id:
            rec.append(r.get("itemId"))
        records.append(rec)
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records, columns=list(range(len(records[0]))))


# aanalytics2가 stdout으로 출력하는 페이지네이션 중단 경고
_PARTIAL_PAT = re.compile(r"lastPage is False|Exit the loop", re.I)


class _ThreadLocalCapture:
    """스레드별로 stdout 을 가로채는 프록시.

    contextlib.redirect_stdout 은 전역 sys.stdout 을 바꾼다. 병렬 워커에서
    쓰면 A 스레드가 가로챈 동안 B 스레드의 print 가 A 의 버퍼로 들어가고,
    복원 순서가 엇갈리면 sys.stdout 이 죽은 StringIO 로 남아
    **이후 모든 출력이 조용히 사라진다.** (병렬 실행 후 로그가 안 찍히는 원인)

    이 프록시는 sys.stdout 을 한 번만 대체하고, 버퍼를 설정한 스레드의
    쓰기만 버퍼로 보낸다. 나머지 스레드는 원래 stdout 으로 그대로 나간다.
    """

    def __init__(self, target):
        self._target = target
        self._local = threading.local()

    def _buf(self):
        return getattr(self._local, "buf", None)

    def write(self, data):
        buf = self._buf()
        return buf.write(data) if buf is not None else self._target.write(data)

    def flush(self):
        buf = self._buf()
        (buf if buf is not None else self._target).flush()

    def isatty(self):
        try:
            return self._target.isatty()
        except Exception:
            return False

    def __getattr__(self, name):
        return getattr(self._target, name)

    @contextlib.contextmanager
    def capture(self):
        prev = self._buf()
        self._local.buf = io.StringIO()
        try:
            yield self._local.buf
        finally:
            self._local.buf = prev


_stdout_proxy = None
_proxy_lock = threading.Lock()


def _captureStdout():
    """현재 스레드의 stdout 만 가로채는 컨텍스트."""
    global _stdout_proxy
    if _stdout_proxy is None:
        with _proxy_lock:
            if _stdout_proxy is None:
                _stdout_proxy = _ThreadLocalCapture(sys.stdout)
                sys.stdout = _stdout_proxy
    return _stdout_proxy.capture()


def _callGetReport(jsonFile, limit, item_id):
    """[수정] 라이브러리가 출력하는 경고를 가로채 불완전 응답을 구분한다.

    기존에는 이 경고가 그냥 화면에 찍히고 빈 DataFrame이 반환되어,
    호출부가 '데이터 없음'으로 오인하거나 일반 오류로 처리해
    작업 전체를 처음부터 다시 돌렸다.
    """
    dataInitiator()
    _bucket.acquire()
    ags = api2.Analytics(COMPANY_ID)
    ags.header
    try:
        with _captureStdout() as buf:
            report = ags.getReport(jsonFile, limit=limit, n_results='inf',
                                   item_id=item_id)
            noise = buf.getvalue().strip()
    except Exception:
        noise = ""
        raise
    if noise:
        logger.debug("aanalytics2: %s", noise.replace("\n", " | "))

    # [방어] 라이브러리가 dict 가 아닌 것(주로 requests.Response)을 돌려주는 경우.
    # 안전 페이징 경로와 같은 검사기를 태워 예외 타입을 통일한다.
    inspectResponse(report, returnRsID(jsonFile) if not isinstance(jsonFile, dict)
                    else jsonFile.get("rsid", ""))
    if 'data' not in report:
        raise TransientAPIError(
            "Adobe 응답에 data 가 없습니다. 토큰 만료·스로틀링·5xx 가능성이 큽니다.")

    df = report['data']
    if _PARTIAL_PAT.search(noise):
        raise PartialDataError(
            "페이지네이션이 끊겼습니다 (lastPage=False인데 응답 없음). "
            "데이터가 불완전하므로 이 호출만 다시 시도합니다.")
    if _isEmpty(df):
        raise EmptyDataError("응답에 행이 없습니다.")
    return df


def _retrieve(jsonFile, limit, item_id, safe_paging=None, strict_paging=False,
              label=""):
    """단일 취득 지점.

    safe_paging=True(기본)면 페이지를 직접 돌며 itemId 로 병합하고
    Adobe 총계와 대조한다. 라이브러리 기본 경로는 dimension value 로 dict 병합해
    페이지 경계가 흔들릴 때 행을 조용히 잃는다.
    환경변수 ACT_SAFE_PAGING=0 으로 전체 비활성화할 수 있다.
    """
    use_safe = SAFE_PAGING if safe_paging is None else safe_paging
    if not use_safe:
        return _callGetReport(jsonFile, limit, item_id)

    payload = readJson(jsonFile)
    label = label or payload.get("rsid", "")
    try:
        rows, meta = fetchAllRows(payload, page_size=limit, label=label,
                                  strict=strict_paging)
    except (PermanentError, AttributeError, TypeError) as e:
        # 라이브러리 내부 구조가 다르면 기존 경로로 되돌린다 (동작은 하되 손실 위험 있음)
        logger.warning("안전 페이지네이션을 쓸 수 없어 기존 경로로 대체합니다: %s: %s",
                       type(e).__name__, e)
        return _callGetReport(jsonFile, limit, item_id)

    if not rows:
        raise EmptyDataError("응답에 행이 없습니다.")
    return buildFrameFromRows(rows, item_id=item_id)


def dataretriever_data(jsonFile, limit=50000, safe_paging=None,
                       strict_paging=False):
    return _retrieve(jsonFile, limit, False, safe_paging, strict_paging)


def dataretriever_data_breakdown(jsonFile, limit=50000, safe_paging=None,
                                 strict_paging=False):
    return _retrieve(jsonFile, limit, True, safe_paging, strict_paging)


def exportToCSV(dataSet, fileName):
    dataSet.to_csv(fileName, sep=',', index=False, encoding='utf-8-sig')


# ---------------------------------------------------------------------------
# JSON 처리
# ---------------------------------------------------------------------------

def readJson(jsonFile):
    """[수정] capacityMetadata가 없는 JSON도 허용한다.

    기존에는 pop에 기본값이 없어 Workspace 추출본이 아닌 JSON은 전부 KeyError였다.
    """
    if isinstance(jsonFile, dict):
        data = deepcopy(jsonFile)
    else:
        with open(jsonFile, 'r', encoding='UTF-8') as f:
            data = json.load(f)
    data.pop("capacityMetadata", None)
    return data


def returnRsID(jsonFile):
    return readJson(jsonFile)['rsid']


def EndDateCalculation(startDate, endDate):
    """[유지·비권장] 하위 호환용. 신규 코드는 TimeWindow를 사용할 것.

    이 함수의 +1일 보정이 시각 지정 경로에도 적용되면서
    조회 구간과 라벨이 하루씩 밀리는 원인이었다.
    """
    endDate = datetime.strptime(str(endDate), '%Y-%m-%d').date() + timedelta(days=1)
    return str(startDate), str(endDate)


def timeChanger(time_obj, start):
    """[유지·비권장] 하위 호환용."""
    if start is True:
        return str('T' + time_obj + ':00.000/')
    t = datetime.strptime(time_obj, "%H:%M") + timedelta(minutes=1)
    return str('T' + t.strftime("%H:%M") + ':00.000')


def makeWindow(startDate, endDate, period="all", start_hour="00:00", end_hour="00:00",
               end_mode="legacy"):
    whole = (start_hour == "00:00" and end_hour == "00:00")
    return TimeWindow(_parse_date(startDate), _parse_date(endDate),
                      _parse_hour(start_hour), _parse_hour(end_hour),
                      period, end_mode, whole)


def applyWindow(jsonFile, window):
    """[수정] dateRange 타입 필터만 갱신한다.

    기존 jsonDateChange는 globalFilters 전체를 순회하며 dateRange 키를 넣어
    세그먼트 필터를 {"type":"segment","segmentId":...,"dateRange":...}로 오염시켰다.
    """
    data = readJson(jsonFile)
    touched = 0
    for gf in data.get('globalFilters', []):
        if gf.get('type') == 'dateRange':
            gf['dateRange'] = window.date_range
            touched += 1
    if touched == 0:
        data.setdefault('globalFilters', []).append(
            {"type": "dateRange", "dateRange": window.date_range})
        logger.warning("JSON에 dateRange 필터가 없어 새로 추가했습니다.")
    return data


def jsonDateChange(startDate, endDate, jsonFile, start_hour="00:00", end_hour="00:00",
                   end_mode="legacy"):
    """[수정] 시각 지정 시 종료일에 +1일이 잘못 더해지던 문제 해결.

        기존: 09:00~18:00 -> 2026-06-01T09:00 / 2026-06-02T18:01  (34개 시간대)
        수정: 09:00~18:00 -> 2026-06-01T09:00 / 2026-06-01T18:01  (10개 시간대)

    날짜 전체(00:00~00:00) 조회 결과는 기존과 완전히 동일하다.
    """
    return applyWindow(jsonFile, makeWindow(startDate, endDate, "all",
                                            start_hour, end_hour, end_mode))


def addStartEndDateColumn(startDate, endDate, rowNum):
    return [startDate] * rowNum, [endDate] * rowNum


def checkSiteCode(dimension):
    return dimension in ("variables/prop1", "variables/evar1", "variables/entryprop1")


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def _db_url():
    """[수정] 접속 정보를 환경변수에서 읽는다.

    접속 정보가 소스에 박혀 있으면 패키지 배포가 곧 자격증명 배포가 된다.
    그래서 기본값을 두지 않고 ACT_DB_URL에서만 읽는다.
    """
    url = os.environ.get("ACT_DB_URL", "")
    if not url:
        raise RuntimeError(
            "DB 접속 정보가 없습니다. 환경변수 ACT_DB_URL을 설정하세요. "
            "예: mysql+pymysql://<user>:<password>@<host>:<port>/<db>?charset=utf8mb4")
    return url


_db_engine = None
_engine_lock = threading.Lock()
_table_lock = threading.Lock()


def get_db_engine(pool_size=None, max_overflow=2):
    """[수정] 풀 크기를 워커 수에 맞춰 조정할 수 있게 했다.

    기존 pool_size=5, max_overflow=2 (동시 7)에 max_workers=10을 쓰면
    워커 3개가 항상 커넥션을 기다리다 QueuePool 타임아웃으로 실패했다.
    그 예외조차 "Error occurred."로만 보였다.
    """
    global _db_engine
    if _db_engine is None:
        with _engine_lock:
            if _db_engine is None:
                size = pool_size or int(os.environ.get("ACT_DB_POOL_SIZE", "10"))
                _db_engine = create_engine(
                    _db_url(), pool_size=size, max_overflow=max_overflow,
                    pool_recycle=3600, pool_pre_ping=True)
    return _db_engine


def resetDbEngine():
    """멀티프로세스 fork 후 자식 프로세스에서 반드시 호출할 것."""
    global _db_engine
    with _engine_lock:
        if _db_engine is not None:
            _db_engine.dispose()
        _db_engine = None


def create_connection_pool():
    return create_engine(_db_url(), poolclass=pool.QueuePool,
                         pool_size=20, max_overflow=10)


def stackTodb(dataFrame, dbTableName, key_columns=None, verbose=False):
    """[수정] key_columns를 주면 멱등 적재(delete-then-insert)한다.

    기존은 if_exists='append' 단독이라
      - 재시도 시 부분 적재분이 남아 중복
      - 같은 날짜 재수집 시 행이 2배
    가 되며 이를 막는 장치가 없었다. 같은 트랜잭션에서 DELETE + INSERT 하므로
    중간 실패 시 원자적으로 롤백된다.

    key_columns 예: ["site_code", "start_date", "end_date"]
    None이면 기존 동작(단순 append)을 유지한다.
    """
    if dataFrame is None or dataFrame.empty:
        logger.info("%s: 적재할 행이 없습니다.", dbTableName)
        return 0

    dataFrame = unicodeCompile_df(dataFrame)
    engine = get_db_engine()

    # [병렬 대응] 여러 워커가 동시에 첫 적재를 하면 to_sql 이 각자 CREATE TABLE 을
    # 시도해 하나가 "table already exists" 로 실패한다. 그 실패가 재시도로 이어지면
    # (key_columns 없이 쓸 때) 같은 데이터가 두 번 들어간다.
    # 테이블이 없을 때만 락을 잡고 한 번만 만든다.
    if not sa_inspect(engine).has_table(dbTableName):
        with _table_lock:
            if not sa_inspect(engine).has_table(dbTableName):
                with engine.begin() as conn:
                    dataFrame.head(0).to_sql(name=dbTableName, con=conn,
                                             if_exists="append", index=False)

    if key_columns:
        exists = sa_inspect(engine).has_table(dbTableName)
        with engine.begin() as conn:
            if exists:
                uniq = dataFrame[list(key_columns)].drop_duplicates()
                where = " AND ".join(f"`{c}` = :{c}" for c in key_columns)
                stmt = text(f"DELETE FROM `{dbTableName}` WHERE {where}")
                for rec in uniq.to_dict("records"):
                    conn.execute(stmt, rec)
            dataFrame.to_sql(name=dbTableName, con=conn, if_exists='append',
                             index=False, chunksize=1000, method='multi')
    else:
        with engine.begin() as conn:
            dataFrame.to_sql(name=dbTableName, con=conn, if_exists='append',
                             index=False, chunksize=1000, method='multi')

    if verbose:
        logger.info("%s: %d행 적재", dbTableName, len(dataFrame))
    return len(dataFrame)


# 하위 호환 별칭 (기존 스크립트가 이 이름들을 import할 수 있다)
stackTodb_RB = stackTodb
stackTodb1 = stackTodb
stackTodb_RB1 = stackTodb


_NON_BMP = re.compile("[\U00010000-\U0010FFFF]+", flags=re.UNICODE)


def unicodeCompile_df(df):
    """[수정] 문자열 컬럼만 벡터화 처리한다.

    기존 df.apply(lambda col: col.map(...))는 숫자 컬럼까지 셀 단위로 순회했다.
    """
    out = df.copy()
    for col in out.columns:
        dt = str(out[col].dtype)
        if dt == "object" or dt.startswith("str"):
            mask = out[col].notna()
            out[col] = out[col].astype(str).str.replace(_NON_BMP, "", regex=True)
            out[col] = out[col].where(mask, None)
    return out


# MST 리포트가 EPP/앱 행을 어떤 문자열로 돌려주는지에 따라 달라진다.
# 기본값은 "_epp" / "-app" 이며, 실제 값은 discoverMstSiteCodes()로 확인할 것.
# 값이 다르면 EPP 행이 전부 필터에서 탈락하는데도 오류 없이 조용히 사라진다.
MST_EPP_SUFFIX = "_epp"
MST_APP_SUFFIX = "-app"

# '-app' 짝이 없는 코드. 기반 목록이 '웹 + {코드}-app' 쌍 구조인데 일부
# 코드만 웹 항목밖에 없는 경우, 여기 적으면 앱 짝을 만들어 붙인다.
MST_APP_MISSING = []

# 위 규칙으로 만들어지지 않는 값을 직접 추가할 자리
MST_EXTRA_SITE_CODES = []


def _withEppVariants(codes, epp_suffix=None, app_suffix=None):
    """각 site_code의 EPP 변형을 함께 포함시킨다.

    통합 리포트는 EPP 사이트 코드를 접미사가 붙은 형태로 돌려주는데,
    기반 목록에 그 변형이 없으면 EPP 행이 조용히 전부 걸러진다.
        uk      -> uk,      uk_epp
        uk-app  -> uk-app,  uk_epp-app
    """
    epp_suffix = epp_suffix or MST_EPP_SUFFIX
    app_suffix = app_suffix or MST_APP_SUFFIX
    n = len(app_suffix)
    out, seen = [], set()
    for c in codes:
        if c.endswith(epp_suffix) or c.endswith(epp_suffix + app_suffix):
            variants = (c,)                      # 이미 EPP 코드면 중복 부착 금지
        elif c.endswith(app_suffix):
            variants = (c, c[:-n] + epp_suffix + app_suffix)
        else:
            variants = (c, c + epp_suffix)
        for v in variants:
            if v not in seen:
                seen.add(v)
                out.append(v)
    return out


def buildMstFilterCodes(base=None, include_epp=True, fill_missing_app=True,
                        exclude_deprecated=True, extra=None,
                        epp_suffix=None, app_suffix=None):
    """MST 필터에 쓸 site_code 목록을 만든다.

    기반 목록에 대해 세 가지를 보정한다.
      1) EPP 변형 추가 (기존에는 하나도 없어 EPP 행이 전부 탈락)
      2) MST_APP_MISSING에 적힌 코드의 '-app' 짝 보완
      3) DEPRECATED_SITE_CODES에 등록된 종료 사이트 제거
    """
    epp_suffix = epp_suffix or MST_EPP_SUFFIX
    app_suffix = app_suffix or MST_APP_SUFFIX
    codes = list(base if base is not None else _DEFAULT_SITE_CODES)

    if fill_missing_app:
        have = set(codes)
        codes += [c + app_suffix for c in MST_APP_MISSING
                  if c in have and c + app_suffix not in have]

    if include_epp:
        codes = _withEppVariants(codes, epp_suffix, app_suffix)

    codes += list(extra if extra is not None else MST_EXTRA_SITE_CODES)

    if exclude_deprecated:
        drop = set()
        for d in DEPRECATED_SITE_CODES:
            drop |= {d, d + app_suffix, d + epp_suffix, d + epp_suffix + app_suffix}
        codes = [c for c in codes if c not in drop]

    seen, out = set(), []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


MST_SITE_CODES = buildMstFilterCodes()


def discoverMstSiteCodes(startDate, endDate, jsonFile, top=5000):
    """MST 리포트가 실제로 돌려주는 site_code 값을 조회해 필터와 대조한다.

    EPP 행이 'de_epp'인지 'de-epp'인지 'deepp'인지는 리포트스위트 설정에 달려 있어
    코드로는 확정할 수 없다. 추측한 접미사가 틀리면 EPP 행이 전부 필터에서
    탈락하는데도 오류가 나지 않으므로, 한 번은 이 함수로 실제 값을 확인할 것.

    반환: {"returned", "missing_from_filter", "unused_in_filter"}
    missing_from_filter가 비어 있지 않으면 그 값들은 지금 조용히 버려지고 있다.
    """
    window = makeWindow(startDate, endDate, "all")
    payload = applyWindow(jsonFile, window)
    df = dataretriever_data(payload, limit=top)
    returned = [str(v) for v in df.iloc[:, 0].tolist()]

    current = set(MST_SITE_CODES)
    missing = sorted({v for v in returned if v not in current})
    unused = sorted(current - set(returned))

    if missing:
        logger.warning("MST 응답에 있으나 필터에 없는 site_code %d개 "
                       "(현재 수집에서 탈락 중): %s", len(missing), missing[:40])
    else:
        logger.info("MST 응답의 site_code가 모두 필터에 포함되어 있습니다.")
    logger.info("필터에만 있고 응답에 없는 코드 %d개 (정상일 수 있음)", len(unused))
    return {"returned": returned, "missing_from_filter": missing,
            "unused_in_filter": unused}


def filterSiteCode(dataframe, site_code):
    """MST 리포트 결과를 유효한 site_code로 거른다.

    site_code 인자에는 프로파일이 정의한 지역/법인 이름도 넣을 수 있다.
    """
    if site_code != "" and site_code is not None:
        codes = expandSiteCodes(site_code)
        codes = _withEppVariants(codes)
        return dataframe.loc[dataframe['site_code'].isin(codes)]
    return dataframe.loc[dataframe['site_code'].isin(MST_SITE_CODES)]


# ---------------------------------------------------------------------------
# 체크포인트 — 중단된 지점부터 이어서 실행
# ---------------------------------------------------------------------------

class BreakdownCheckpoint:
    """breakdown 작업의 진행 상황을 파일에 남긴다.

    해결하는 문제
    -------------
    기존에는 재시도가 breakdown 작업 '전체'를 감쌌다. 항목 하나가 실패하면
    이미 성공해 적재까지 끝낸 항목들을 처음부터 다시 호출하고 다시 적재했다.
    실패가 결정론적이면(특정 itemId가 항상 실패) 그 지점을 영원히 넘지 못해
    뒤쪽 항목은 한 번도 조회되지 않았다.

        level-0 재조회 6회 / KR·DE 각 6회 재호출 / DB에 6행씩 중복
        FR에서 매번 막힘 -> IT는 호출 0회 (데이터 전체 손실)

    checkpoint_dir을 주면 level-0 목록과 완료된 itemId를 저장하므로
    다시 실행할 때 끊긴 지점부터 이어서 돈다.
    """

    VERSION = 1

    def __init__(self, job_key, directory=None):
        self.job_key = job_key
        self.enabled = bool(directory)
        self.path = None
        self.data = {"version": self.VERSION, "job_key": job_key,
                     "level0": None, "done": [], "failed": {}}
        if self.enabled:
            os.makedirs(directory, exist_ok=True)
            digest = hashlib.md5(job_key.encode("utf-8")).hexdigest()[:12]
            self.path = os.path.join(directory, f"ckpt_{digest}.json")
            self._load()

    def _load(self):
        if not (self.path and os.path.exists(self.path)):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if loaded.get("job_key") == self.job_key:
                self.data = loaded
                logger.info("체크포인트 발견: 완료 %d건, 실패 %d건 (%s)",
                            len(self.data.get("done", [])),
                            len(self.data.get("failed", {})), self.path)
        except Exception as e:
            logger.warning("체크포인트를 읽지 못해 새로 시작합니다: %s", e)

    def save(self):
        if not self.enabled:
            return
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False)
            os.replace(tmp, self.path)          # 원자적 교체
        except Exception as e:
            logger.warning("체크포인트 저장 실패(작업은 계속): %s", e)

    @property
    def level0(self):
        return self.data.get("level0")

    def setLevel0(self, items):
        self.data["level0"] = [list(x) for x in items]
        self.save()

    @property
    def done(self):
        return set(self.data.get("done", []))

    @property
    def failed(self):
        return dict(self.data.get("failed", {}))

    def markDone(self, item_id):
        if item_id not in self.data["done"]:
            self.data["done"].append(item_id)
        self.data["failed"].pop(item_id, None)
        self.save()

    def markFailed(self, item_id, error):
        self.data["failed"][item_id] = str(error)[:300]
        self.save()

    def clear(self):
        if self.path and os.path.exists(self.path):
            try:
                os.remove(self.path)
                logger.info("작업 완료, 체크포인트 삭제: %s", self.path)
            except Exception:
                pass


def _jobKey(rsid, dimension, window, dbTableName, kind):
    return f"{kind}|{rsid}|{dimension}|{window.start_label}|{window.end_label}|{dbTableName}"


def _displayWidth(text):
    """터미널에서 차지하는 칸 수. 한글·한자는 2칸이다.

    len()으로 패딩을 계산하면 한글이 섞인 줄에서 이전 출력이 지워지지 않고
    잔상이 남는다.
    """
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
               for ch in text)


def _supportsInline(stream=None):
    """같은 줄 덮어쓰기(\r)가 통하는 환경인지 판단한다.

    파일 리다이렉트나 ECS/CloudWatch 같은 수집형 로그에서는 \r이 그대로
    남아 한 줄이 뒤엉키므로, 그런 환경에서는 일반 로그로 되돌린다.
    """
    stream = stream or sys.stdout
    try:
        if stream.isatty():
            return True
    except Exception:
        pass
    # Jupyter / IPython 커널은 isatty()가 False지만 \r 덮어쓰기가 동작한다
    try:
        from IPython import get_ipython
        ip = get_ipython()
        if ip is not None and type(ip).__name__ == "ZMQInteractiveShell":
            return True
    except Exception:
        pass
    return False


class _Progress:
    """breakdown 진행 상황을 한 줄에 덮어쓰며 표시한다.

    항목당 API 호출이 최소 1회라 전체가 수백 회에 이르는데, 기존에는 끝날 때까지
    아무 출력이 없어 멈춘 것인지 도는 중인지 알 수 없었다.
    매 항목을 새 줄로 찍으면 이번에는 로그가 수백 줄로 불어난다.
    그래서 진행 상황은 한 줄을 갱신하고, 실패처럼 남아야 할 사건만 위에 쌓는다.

        [uk 2026-06-01 bd] [████████░░░░] 12/8 (67%) IT 2행 · 남은 00:04

    mode
      "auto"   터미널·Jupyter면 덮어쓰기, 그 외(파일·ECS 로그)는 일반 로그
      "inline" 항상 덮어쓰기
      "log"    항상 일반 로그 (한 줄씩 누적)
      "off"    진행 표시 없음 (종료 요약은 유지)
    """

    BAR_WIDTH = 12

    def __init__(self, total, label, every=1, unit="행", mode="auto", stream=None):
        self.total = total
        self.label = label
        self.every = max(int(every or 0), 0)
        self.unit = unit
        self.stream = stream or sys.stdout
        self.mode = mode
        self.inline = (mode == "inline" or
                       (mode == "auto" and _supportsInline(self.stream)))
        self.n = 0
        self.ok = self.skipped = self.failed = 0
        self.rows = 0
        self.t0 = time.time()
        self._last_w = 0

    # -- 출력 -------------------------------------------------------------
    @staticmethod
    def _hms(sec):
        sec = int(max(sec, 0))
        if sec >= 3600:
            return f"{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"
        return f"{sec // 60:02d}:{sec % 60:02d}"

    def _bar(self, pct):
        done = int(self.BAR_WIDTH * pct / 100)
        return "█" * done + "░" * (self.BAR_WIDTH - done)

    def _write(self, text):
        w = _displayWidth(text)
        pad = max(self._last_w - w, 0)
        try:
            self.stream.write("\r" + text + " " * pad)
            self.stream.flush()
        except Exception:
            return
        self._last_w = w

    def clearLine(self):
        """덮어쓰기 줄을 지운다. 위에 다른 로그를 남기기 전에 호출한다."""
        if self.inline and self._last_w:
            try:
                self.stream.write("\r" + " " * self._last_w + "\r")
                self.stream.flush()
            except Exception:
                pass
            self._last_w = 0

    def log(self, level, msg, *args):
        """진행 줄을 지우고 로그를 남긴 뒤 진행 줄을 다시 그린다.

        이걸 거치지 않고 바로 logger를 부르면 진행 줄과 로그가 한 줄에 뒤엉킨다.
        """
        self.clearLine()
        logger.log(level, msg, *args)
        if self.inline and self._pending:
            self._write(self._pending)

    _pending = ""

    # -- 진행 -------------------------------------------------------------
    def step(self, name, status="ok", rows=None, detail="", quiet=False):
        self.n += 1
        if status == "ok":
            self.ok += 1
        elif status == "skip":
            self.skipped += 1
        else:
            self.failed += 1
        if rows:
            self.rows += rows

        if self.mode == "off" or not self.every:
            return
        if not logger.isEnabledFor(logging.INFO):
            return
        if quiet and self.inline:
            # 이어하기에서 이미 끝난 항목. 줄은 갱신하되 로그로 쌓지는 않는다.
            pass
        elif quiet:
            return
        if self.n % self.every and self.n != self.total:
            return

        elapsed = time.time() - self.t0
        pct = 100.0 * self.n / self.total if self.total else 100.0
        eta = (elapsed / self.n) * (self.total - self.n) if self.n else 0
        mark = {"ok": "", "skip": "건너뜀 ", "fail": "실패 "}[status]
        extra = f" {rows}{self.unit}" if rows else ""
        tail = f" {detail}" if detail else ""

        if self.inline:
            text = (f"[{self.label}] {self._bar(pct)} {self.n}/{self.total} "
                    f"({pct:.0f}%) {mark}{name}{extra}{tail} · 남은 {self._hms(eta)}")
            self._pending = text
            self._write(text)
        else:
            logger.info("[%s] (%d/%d, %.0f%%) %s%s%s%s | 경과 %s · 남은시간 ~%s",
                        self.label, self.n, self.total, pct, mark, name, extra,
                        tail, self._hms(elapsed), self._hms(eta))

    def finish(self):
        elapsed = time.time() - self.t0
        self.clearLine()
        self._pending = ""
        logger.info("[%s] 종료 — 완료 %d / 건너뜀 %d / 실패 %d (전체 %d), "
                    "%s%s, 소요 %s",
                    self.label, self.ok, self.skipped, self.failed, self.total,
                    self.rows, self.unit, self._hms(elapsed))
        if self.skipped and self.ok == 0 and self.failed == 0:
            logger.info("[%s] 모든 항목이 이미 완료되어 API 호출이 없었습니다. "
                        "다시 받으려면 체크포인트를 지우거나 resume=False.",
                        self.label)


def _runItems(items, work, cp, item_retries, resume, retry_failed, label_prefix,
              skip_threshold=0.5, progress_every=1, progress_mode="auto"):
    """항목 목록을 돌면서 항목 단위로 재시도하고, 소진되면 건너뛴다.

    [핵심] 재시도 경계가 '작업 전체'가 아니라 '항목 하나'다.
    한 항목이 끝내 실패해도 그 항목만 기록하고 다음으로 넘어가므로
    뒤쪽 항목이 조회되지 못하는 일이 없다.

    work(value, item_id) -> 처리 결과. 예외를 던지면 재시도 후 건너뛴다.
    """
    done, failed = cp.done, cp.failed
    total = len(items)
    pr = _Progress(total, label_prefix, progress_every, mode=progress_mode)

    for value, item_id in items:
        if resume and item_id in done:
            # 이어하기에서 이미 끝난 항목까지 한 줄씩 찍으면 로그가 묻힌다.
            # 세지만 출력하지 않고, 총계는 종료 요약에 나온다.
            pr.step(value, "skip", quiet=True)
            continue
        if item_id in failed and not retry_failed:
            pr.step(value, "skip",
                    detail=f"(이전 실패: {failed[item_id][:60]})")
            continue
        try:
            rows = callWithRetry(work, value, item_id, max_retries=item_retries,
                                 label=f"{label_prefix} {value}")
            cp.markDone(item_id)
            pr.step(value, "ok", rows if isinstance(rows, int) else None)
        except EmptyDataError:
            cp.markDone(item_id)                 # 데이터 없음은 정상 완료로 본다
            pr.step(value, "ok", detail="(데이터 없음)")
        except Exception as e:
            cp.markFailed(item_id, f"{type(e).__name__}: {e}")
            pr.log(logging.ERROR, "[%s] %s 건너뜀 — %s: %s",
                   label_prefix, value, type(e).__name__, e)
            pr.step(value, "fail", detail=f"— {type(e).__name__}")

    pr.finish()
    ok, skipped, newly_failed = pr.ok, pr.skipped, pr.failed

    if total and newly_failed / total > skip_threshold:
        logger.error("[%s] 실패 비율이 %.0f%%입니다. 인증 만료나 세그먼트 삭제 등 "
                     "공통 원인일 수 있으니 확인하세요.",
                     label_prefix, 100.0 * newly_failed / total)
    elif newly_failed == 0 and not cp.failed:
        cp.clear()
    return ok, skipped, newly_failed


# ---------------------------------------------------------------------------
# breakdown itemID
# ---------------------------------------------------------------------------

def ChangeItemID(itemID, breakdownJson):
    """[확장] 단일 itemID(기존) 또는 (dimension, itemId) 체인(신규)을 받는다.

    기존 구현은 모든 metricFilter에 같은 itemId를 넣었기 때문에
    필터가 2개 필요한 2단 breakdown이 구조적으로 불가능했다.
    체인을 주면 dimension을 우선 매칭해 각 필터에 서로 다른 itemId를 배정한다.

        ChangeItemID("id_kr", bd)                                   # 기존 방식
        ChangeItemID([("variables/evar1","id_kr"),
                      ("variables/evar2","id_mo")], bd)             # 2단
    """
    patched = deepcopy(breakdownJson)
    filters = patched.get('metricContainer', {}).get('metricFilters', [])

    if not isinstance(itemID, (list, tuple)):
        for f in filters:
            if "itemId" in f:
                f["itemId"] = itemID
        return patched

    chain = [tuple(x) for x in itemID]
    bd_filters = [f for f in filters if f.get('type') == 'breakdown'] or filters

    remaining = list(chain)
    for f in bd_filters:                                  # 1차: dimension 정확 매칭
        for i, (dim, iid) in enumerate(remaining):
            if f.get('dimension') == dim:
                f['itemId'] = iid
                remaining.pop(i)
                break
    unassigned = [f for f in bd_filters
                  if f.get('itemId') in (None, "", "__ITEM__")]
    for f, (dim, iid) in zip(unassigned, remaining):      # 2차: 순서대로
        f['itemId'] = iid

    return patched


def buildBreakdownJson(baseJson, newDimension):
    """상위 리포트 JSON에서 breakdown JSON을 자동 생성한다.

    현재 dimension을 type=breakdown metricFilter로 내리고 모든 metric에 적용한 뒤
    dimension을 newDimension으로 교체한다. Workspace에서 breakdown JSON을
    따로 추출할 필요가 없어진다.
    """
    bd = readJson(baseJson)
    old_dim = bd['dimension']
    mc = bd.setdefault('metricContainer', {})
    mf = mc.setdefault('metricFilters', [])
    fid = f"bf{len(mf)}"
    mf.append({"id": fid, "type": "breakdown", "dimension": old_dim, "itemId": "__ITEM__"})
    for m in mc.get('metrics', []):
        m.setdefault('filters', []).append(fid)
    bd['dimension'] = newDimension
    return bd


# Adobe dimension id 형태인지 판별. "variables/evar3", "variables/prop1" 등.
_DIM_PAT = re.compile(r"^variables/[\w.\-]+$", re.IGNORECASE)


def normalizeDimension(spec):
    """breakdown 지정값을 정규화한다.

    받는 형태
      "path/to/bd.json"     -> 그대로 (기존 템플릿 방식)
      "variables/evar3"     -> 그대로 (권장)
      "evar3"               -> "variables/evar3" 로 보정
      dict                  -> 그대로 (이미 payload)

    Workspace 에서 뽑은 리포트 JSON 의 "dimension" 필드에 적힌 문자열을
    그대로 쓰는 것이 가장 확실하다.
    """
    if isinstance(spec, dict):
        return spec
    text = str(spec).strip()
    if text.lower().endswith(".json"):
        return text
    if "/" not in text:
        # evar3, prop1 처럼 접두사를 빼먹는 경우가 잦다
        fixed = "variables/" + text
        logger.info("breakdown dimension '%s' 을 '%s' 로 보정했습니다.", text, fixed)
        return fixed
    if not _DIM_PAT.match(text):
        logger.warning("breakdown dimension 형식이 예상과 다릅니다: %r "
                       "(예: 'variables/evar3'). 그대로 사용합니다.", text)
    return text


def _loadOrBuildBreakdown(baseJson, spec):
    """spec이 .json 경로면 템플릿 로드, dimension 문자열이면 자동 생성.

    dict 를 주면 그대로 payload 로 쓴다.
    """
    spec = normalizeDimension(spec)
    if isinstance(spec, dict):
        return readJson(spec)
    if spec.lower().endswith(".json"):
        return readJson(spec)
    return buildBreakdownJson(baseJson, spec)


def returnItemID(startDate, endDate, jsonItemID, start_hour, end_hour, site_code):
    itemIDjson = jsonDateChange(startDate, endDate, jsonItemID, start_hour, end_hour)
    try:
        itemIDdf = dataretriever_data_breakdown(itemIDjson)
    except EmptyDataError:
        logger.info("itemID 조회 결과가 없습니다: %s", itemIDjson.get('dimension'))
        return []

    columnList = list(map(str, range(itemIDdf.shape[1])))
    columnList[0] = 'site_code'
    columnList[-1] = 'item_id'
    itemIDdf.columns = columnList

    if checkSiteCode(itemIDjson["dimension"]):
        itemIDdf = filterSiteCode(itemIDdf, site_code)
    return itemIDdf[['site_code', 'item_id']].values.tolist()


def returnItemID_rs(jsonItemID):
    try:
        itemIDdf = dataretriever_data_breakdown(jsonItemID)
    except EmptyDataError:
        logger.info("itemID 조회 결과가 없습니다.")
        return []
    columnList = list(map(str, range(itemIDdf.shape[1])))
    columnList[0] = 'site_code'
    columnList[-1] = 'item_id'
    itemIDdf.columns = columnList
    return itemIDdf[['site_code', 'item_id']].values.tolist()


def rsIDchange(jsonFile, rsID):
    temp = deepcopy(jsonFile)
    temp['rsid'] = rsID
    return temp


def ReturnJsonchanged(startDate, endDate, jsonFile, jsonFilebreakdown,
                      start_hour, end_hour, site_code):
    itemIDList = returnItemID(startDate, endDate, jsonFile, start_hour, end_hour, site_code)
    base = jsonDateChange(startDate, endDate, jsonFile, start_hour, end_hour)
    bd = jsonDateChange(startDate, endDate,
                        _loadOrBuildBreakdown(base, jsonFilebreakdown),
                        start_hour, end_hour)
    return [(code, ChangeItemID(iid, bd)) for code, iid in itemIDList]


# ---------------------------------------------------------------------------
# 프레임 조립 (기존 insert 반복을 한 곳으로 모음)
# ---------------------------------------------------------------------------

def _siteCodeFromRsid(rsid):
    """rsid에서 site_code를 구한다.

    리포트스위트 이름 규칙은 조직마다 다르므로 프로파일이 정하고,
    여기서는 그때그때 주입된 함수로 넘긴다.
    """
    return _site_code_of(rsid)


def _zeroRow(nMetrics, fill_value=0):
    return [fill_value] * nMetrics


def _decorate(dataFrame, window, period, epp, site_code=None, breakdown_total=None,
              extra="", extra1="", if_site_code=False, site_code_rs=False,
              epp_col="is_epp"):
    """site_code / period / start_date / end_date / is_epp 컬럼을 붙인다.

    [수정] end_date 라벨이 window.end_label에서 나온다.
    기존에는 EndDateCalculation("0", endDate)[1] 즉 종료일+1을 넣어
    시각 지정 시 DB 라벨이 하루 밀렸다 (동일 코드가 10곳에 복제되어 있었다).
    """
    df = dataFrame
    pos = 0
    if site_code is not None:
        df.insert(0, "site_code", site_code, True)
        pos = 1

    if breakdown_total is not None:
        df.insert(pos, "breakdown", breakdown_total, True)
        pos += 1

    base = 1 if (if_site_code or site_code_rs) else 2
    if breakdown_total is not None:
        base = pos
    df.insert(base, "period", period, True)
    df.insert(base + 1, "start_date", window.start_label, True)
    df.insert(base + 2, "end_date", window.end_label, True)
    df.insert(base + 3, epp_col, epp, True)
    return df


# ---------------------------------------------------------------------------
# 1단 수집
# ---------------------------------------------------------------------------

def refinedFrame(startDate, endDate, period, jsonFile, epp, if_site_code,
                 site_code_rs, start_hour, end_hour, end_mode="legacy"):
    dataInitiator()
    window = makeWindow(startDate, endDate, period, start_hour, end_hour, end_mode)
    dateChange = applyWindow(jsonFile, window)
    dataFrame = dataretriever_data(dateChange)

    if not _isTotalRsid(dateChange['rsid']):
        dataFrame.columns = list(range(dataFrame.shape[1]))
        if site_code_rs is True:
            dataFrame = dataFrame.drop(0, axis=1)
        code = _siteCodeFromRsid(dateChange['rsid'])
        if 'epp' in dateChange['rsid'].split('4')[-1]:
            epp = "Y"
        dataFrame.insert(0, "site_code", code, True)

    if if_site_code is True or site_code_rs is True:
        dataFrame.insert(1, "period", period, True)
        dataFrame.insert(2, "start_date", window.start_label, True)
        dataFrame.insert(3, "end_date", window.end_label, True)
        dataFrame.insert(4, "is_epp", epp, True)
    else:
        if _isTotalRsid(dateChange['rsid']):
            dataFrame.insert(0, "site_code", "MST", True)
        dataFrame.insert(2, "period", period, True)
        dataFrame.insert(3, "start_date", window.start_label, True)
        dataFrame.insert(4, "end_date", window.end_label, True)
        dataFrame.insert(5, "is_epp", epp, True)

    return dataFrame


def refinedFrame1(startDate, endDate, period, jsonFile, tbColumn, dbTableName, epp,
                  if_site_code, site_code_rs, limit, extra, extra1,
                  start_hour, end_hour, site_code, key_columns=None,
                  end_mode="legacy", on_empty="skip"):
    """1단 수집 + 적재.

    on_empty : "skip"(기존 동작) | "fill"(모든 지표 0인 행 1건 적재) | "raise"
    """
    try:
        df = refinedFrame(startDate, endDate, period, jsonFile, epp, if_site_code,
                          site_code_rs, start_hour, end_hour, end_mode)
    except EmptyDataError:
        if on_empty == "raise":
            raise
        if on_empty == "skip":
            logger.info("데이터 없음, 건너뜁니다: %s %s~%s",
                        returnRsID(jsonFile), startDate, endDate)
            return 0
        df = _emptyFrame(startDate, endDate, period, jsonFile, tbColumn, epp,
                         if_site_code, site_code_rs, start_hour, end_hour, end_mode)
        return stackTodb(df, dbTableName, key_columns)

    df.columns = tbColumn
    if extra != "":
        df.insert(5, "extra", extra, True)
    if extra1 != "":
        df.insert(6, "extra1", extra1, True)

    if if_site_code is True and _isTotalRsid(returnRsID(jsonFile)):
        df = filterSiteCode(df, site_code)
    if limit:
        df = df.head(limit)

    return stackTodb(df, dbTableName, key_columns)


def _emptyFrame(startDate, endDate, period, jsonFile, tbColumn, epp, if_site_code,
                site_code_rs, start_hour, end_hour, end_mode="legacy",
                fill_value=0, dimension_fill="(none)"):
    """데이터가 없을 때 넣을 0 채움 행 1건.

    지표는 0, 차원 자리는 "(none)"으로 채운다.
    적재 자체를 건너뛰면 그 구간이 '수집 안 됨'인지 '데이터 0'인지 구분되지 않는다.
    """
    window = makeWindow(startDate, endDate, period, start_hour, end_hour, end_mode)
    rsid = returnRsID(jsonFile)
    fixed = {"site_code": _siteCodeFromRsid(rsid), "dimension": dimension_fill,
             "breakdown": dimension_fill, "breakdown2": dimension_fill,
             "period": period, "start_date": window.start_label,
             "end_date": window.end_label, "is_epp": epp, "is_epp_integ": "N",
             "extra": "", "extra1": ""}
    row = {c: fixed.get(c, fill_value) for c in tbColumn}
    return pd.DataFrame([row], columns=list(tbColumn))


def refinedFrameTotal(startDate, endDate, period, jsonFile, tbColumn, dbTableName,
                      epp, if_site_code, site_code_rs, limit, extra, extra1,
                      start_hour, end_hour, site_code, key_columns=None,
                      end_mode="legacy"):
    dataInitiator()
    window = makeWindow(startDate, endDate, period, start_hour, end_hour, end_mode)
    dateChange = applyWindow(jsonFile, window)
    try:
        dataFrame = dataretriever_data(dateChange)
    except EmptyDataError:
        logger.info("Total 데이터 없음: %s %s", dateChange['rsid'], startDate)
        return 0

    if not _isTotalRsid(dateChange['rsid']):
        dataFrame.columns = list(range(dataFrame.shape[1]))
        if site_code_rs is True:
            dataFrame = dataFrame.drop(0, axis=1)
        if 'epp' in dateChange['rsid'].split('4')[-1]:
            epp = "Y"
        dataFrame.insert(0, "site_code", _siteCodeFromRsid(dateChange['rsid']), True)

    if if_site_code is True or site_code_rs is True:
        dataFrame.insert(1, "breakdown", "Total", True)
        dataFrame.insert(2, "period", period, True)
        dataFrame.insert(3, "start_date", window.start_label, True)
        dataFrame.insert(4, "end_date", window.end_label, True)
        dataFrame.insert(5, "is_epp", epp, True)
    else:
        if _isTotalRsid(dateChange['rsid']):
            dataFrame.insert(0, "site_code", "MST", True)
        dataFrame.insert(2, "period", period, True)
        dataFrame.insert(3, "start_date", window.start_label, True)
        dataFrame.insert(4, "end_date", window.end_label, True)
        dataFrame.insert(5, "is_epp", epp, True)

    dataFrame.columns = tbColumn
    if extra != "":
        dataFrame.insert(6, "extra", extra, True)
    if extra1 != "":
        dataFrame.insert(7, "extra1", extra1, True)

    if if_site_code is True and _isTotalRsid(returnRsID(jsonFile)):
        dataFrame = filterSiteCode(dataFrame, site_code)
    if limit:
        dataFrame = dataFrame.head(limit)

    return stackTodb(dataFrame, dbTableName, key_columns)


def jsonToDb(startDate, endDate, period, jsonLocation, tbColumn, dbTableName, epp,
             if_site_code, site_code_rs, limit, extra, extra1, start_hour, end_hour,
             site_code):
    """[유지] 하위 호환용. 신규 코드는 refinedFrame1을 사용할 것."""
    return refinedFrame1(startDate, endDate, period, jsonLocation, tbColumn,
                         dbTableName, epp, if_site_code, site_code_rs, limit,
                         extra, extra1, start_hour, end_hour, site_code)


# ---------------------------------------------------------------------------
# RS 단위 수집
# ---------------------------------------------------------------------------

def refineRsIDChange(startDate, endDate, jsonFile, rsList, period, tbColumn,
                     dbTableName, epp, limit, extra, extra1, start_hour, end_hour,
                     key_columns=None, end_mode="legacy", on_empty="skip"):
    dataInitiator()
    window = makeWindow(startDate, endDate, period, start_hour, end_hour, end_mode)
    rschanged = rsIDchange(applyWindow(jsonFile, window), rsList[1])

    try:
        dataFrame = dataretriever_data(rschanged)
    except EmptyDataError:
        if on_empty == "raise":
            raise
        logger.info("데이터 없음: %s %s~%s", rsList[1], startDate, endDate)
        return 0

    if limit:
        dataFrame = dataFrame.head(limit)
    dataFrame.columns = list(range(dataFrame.shape[1]))

    dataFrame.insert(0, "site_code", rsList[0], True)
    dataFrame.insert(2, "period", period, True)
    dataFrame.insert(3, "start_date", window.start_label, True)
    dataFrame.insert(4, "end_date", window.end_label, True)
    dataFrame.insert(5, "is_epp", "Y" if epp is True else "N", True)
    dataFrame.insert(6, "is_epp_integ",
                     "Y" if rsList[1] in EPP_INTEGRATED_RSIDS else "N", True)

    dataFrame.columns = tbColumn
    if extra != "":
        dataFrame.insert(7, "extra", extra, True)
    if extra1 != "":
        dataFrame.insert(8, "extra1", extra1, True)

    n = stackTodb(dataFrame, dbTableName, key_columns)
    logger.info("%s %s~%s : %d행", rsList[1], startDate, endDate, n)
    return n


def refineRsIDChangeRB(startDate, endDate, jsonFile, rsList, period, tbColumn,
                       dbTableName, epp, limit, Biz_type, Device_type, Division,
                       Category, site_code_ae, start_hour, end_hour,
                       key_columns=None, end_mode="legacy", on_empty="skip"):
    dataInitiator()
    window = makeWindow(startDate, endDate, period, start_hour, end_hour, end_mode)
    rschanged = rsIDchange(applyWindow(jsonFile, window), rsList[1])

    try:
        dataFrame = dataretriever_data(rschanged)
    except EmptyDataError:
        if on_empty == "raise":
            raise
        logger.info("데이터 없음: %s %s", rsList[1], startDate)
        return 0

    if limit:
        dataFrame = dataFrame.head(limit)
    dataFrame.columns = list(range(dataFrame.shape[1]))

    dataFrame.insert(0, "site_code", site_code_ae if site_code_ae != "" else rsList[0], True)
    dataFrame.insert(1, "RS ID", rsList[1], True)
    dataFrame.insert(2, "Biz_type", Biz_type, True)
    dataFrame.insert(3, "Division", Division, True)
    dataFrame.insert(4, "Category", Category, True)
    dataFrame.insert(5, "Device_type", Device_type, True)
    dataFrame.insert(6, "Date", startDate, True)
    dataFrame.columns = tbColumn

    n = stackTodb(dataFrame, dbTableName, key_columns)
    logger.info("%s %s %s/%s/%s/%s : %d행", rsList[1], startDate, Biz_type,
                Device_type, Division, Category, n)
    return n


def refineRsIDChangeTotal(startDate, endDate, jsonFile, rsList, period, tbColumn,
                          dbTableName, epp, limit, extra, extra1, start_hour, end_hour,
                          key_columns=None, end_mode="legacy"):
    dataInitiator()
    window = makeWindow(startDate, endDate, period, start_hour, end_hour, end_mode)
    rschanged = rsIDchange(applyWindow(jsonFile, window), rsList[1])
    try:
        dataFrame = dataretriever_data(rschanged)
    except EmptyDataError:
        logger.info("Total 데이터 없음: %s %s", rsList[1], startDate)
        return 0

    if limit:
        dataFrame = dataFrame.head(limit)

    dataFrame.insert(0, "site_code", rsList[0], True)
    dataFrame.insert(2, "breakdown", "Total", True)
    dataFrame.insert(3, "period", period, True)
    dataFrame.insert(4, "start_date", window.start_label, True)
    dataFrame.insert(5, "end_date", window.end_label, True)
    dataFrame.insert(6, "epp", "Y" if epp is True else "N", True)
    if extra != "":
        dataFrame.insert(7, "extra", extra, True)
    if extra1 != "":
        dataFrame.insert(8, "extra1", extra1, True)
    dataFrame.columns = tbColumn

    return stackTodb(dataFrame, dbTableName, key_columns)


# ---------------------------------------------------------------------------
# 2단(1st breakdown) 수집
# ---------------------------------------------------------------------------

def StackbreakValue(startDate, endDate, period, jsonFile, jsonFilebreakdown, tbColumn,
                    dbTableName, epp, limit1, limit2, extra, extra1,
                    start_hour, end_hour, site_code, key_columns=None,
                    end_mode="legacy", on_empty="skip",
                    checkpoint_dir=None, resume=True, item_retries=3,
                    retry_failed=False, progress_every=1, progress_mode="auto"):
    dataInitiator()
    window = makeWindow(startDate, endDate, period, start_hour, end_hour, end_mode)
    total = 0

    if _isTotalRsid(returnRsID(jsonFile)):
        # [수정] 항목 단위 재시도 + 체크포인트.
        # 기존에는 여기서 예외가 나면 상위 재시도가 level-0부터 전부 다시 돌렸다.
        base = applyWindow(jsonFile, window)
        bd = applyWindow(_loadOrBuildBreakdown(base, jsonFilebreakdown), window)
        cp = BreakdownCheckpoint(
            _jobKey(base['rsid'], base.get('dimension', ''), window, dbTableName, "mst"),
            checkpoint_dir)

        itemIDList = cp.level0 if (resume and cp.level0) else None
        if itemIDList is None:
            try:
                itemIDList = callWithRetry(
                    returnItemID, startDate, endDate, jsonFile, start_hour, end_hour,
                    site_code, max_retries=item_retries, label="MST level0")
            except EmptyDataError:
                logger.info("MST level-0 데이터 없음: %s", startDate)
                return 0
            cp.setLevel0(itemIDList)
        if limit1:
            itemIDList = itemIDList[:limit1]

        counter = {"rows": 0}

        def work(code, item_id):
            payload = ChangeItemID(item_id, bd)
            try:
                dataFrame = dataretriever_data(payload)
            except EmptyDataError:
                if on_empty != "fill":
                    raise
                df = _emptyFrame(startDate, endDate, period, jsonFile, tbColumn,
                                 epp, True, False, start_hour, end_hour, end_mode)
                df.iloc[0, df.columns.get_loc("site_code")] = code
                n = stackTodb(df, dbTableName, key_columns)
                counter["rows"] += n
                return n

            if limit2:
                dataFrame = dataFrame.head(limit2)
            dataFrame.insert(0, "site_code", code, True)
            dataFrame.insert(2, "period", period, True)
            dataFrame.insert(3, "start_date", window.start_label, True)
            dataFrame.insert(4, "end_date", window.end_label, True)
            dataFrame.insert(5, "is_us_epp", epp, True)
            dataFrame.columns = tbColumn
            if extra != "":
                dataFrame.insert(6, "extra", extra, True)
            if extra1 != "":
                dataFrame.insert(7, "extra1", extra1, True)
            n = stackTodb(dataFrame, dbTableName, key_columns)
            counter["rows"] += n
            return n

        _runItems(itemIDList, work, cp, item_retries, resume, retry_failed,
                  f"MST {startDate} bd", progress_every=progress_every,
                  progress_mode=progress_mode)
        return counter["rows"]

    # 비-MST: 상위 리포트 한 건만 조회한다 (기존 동작 유지)
    logger.info("비-MST 리포트(%s)이므로 breakdown 없이 상위 리포트만 수집합니다.",
                returnRsID(jsonFile))
    dateChange = applyWindow(jsonFile, window)
    try:
        dataFrame = dataretriever_data(dateChange)
    except EmptyDataError:
        if on_empty == "fill":
            df = _emptyFrame(startDate, endDate, period, jsonFile, tbColumn, epp,
                             True, False, start_hour, end_hour, end_mode)
            return stackTodb(df, dbTableName, key_columns)
        return 0

    dataFrame.columns = list(range(dataFrame.shape[1]))
    if limit2:
        dataFrame = dataFrame.head(limit2)
    dataFrame.insert(0, "site_code", _siteCodeFromRsid(dateChange['rsid']), True)
    dataFrame.insert(2, "period", period, True)
    dataFrame.insert(3, "start_date", window.start_label, True)
    dataFrame.insert(4, "end_date", window.end_label, True)
    dataFrame.insert(5, "is_us_epp", epp, True)
    dataFrame.columns = tbColumn
    if extra != "":
        dataFrame.insert(6, "extra", extra, True)
    if extra1 != "":
        dataFrame.insert(7, "extra1", extra1, True)
    return stackTodb(dataFrame, dbTableName, key_columns)


def secondCaller(startDate, endDate, jsonFile, jsonFilebreakdown, rsList, period,
                 tbColumn, dbTableName, epp, limit1, limit2, extra="", extra1="",
                 start_hour="00:00", end_hour="00:00", key_columns=None,
                 end_mode="legacy", on_empty="skip",
                 checkpoint_dir=None, resume=True, item_retries=3,
                 retry_failed=False, progress_every=1, progress_mode="auto"):
    """1st breakdown 수집.

    [수정] 재시도 경계를 작업 전체에서 항목 하나로 내렸다.
    항목이 끝내 실패하면 그 항목만 건너뛰고 다음으로 진행한다.
    checkpoint_dir을 주면 중단 지점부터 이어서 실행한다.

        checkpoint_dir="./ckpt"  -> level-0 목록과 완료 itemId를 파일에 기록
        resume=True              -> 이미 완료된 항목은 재호출하지 않음
        item_retries=3           -> 항목당 재시도 횟수
        retry_failed=True        -> 이전에 실패로 기록된 항목을 다시 시도

    중복 적재를 막으려면 key_columns에 dimension까지 포함할 것.
        key_columns=["site_code", "dimension", "start_date", "end_date"]
    """
    dataInitiator()
    window = makeWindow(startDate, endDate, period, start_hour, end_hour, end_mode)
    base = rsIDchange(applyWindow(jsonFile, window), rsList[1])
    # jsonFilebreakdown 은 .json 경로뿐 아니라 dimension 이름도 받는다.
    # "variables/evar3" 을 주면 base 로부터 breakdown payload 를 자동 생성한다.
    bd = rsIDchange(applyWindow(_loadOrBuildBreakdown(base, jsonFilebreakdown),
                                window), rsList[1])

    cp = BreakdownCheckpoint(
        _jobKey(rsList[1], base.get('dimension', ''), window, dbTableName, "bd"),
        checkpoint_dir)

    # level-0 목록은 한 번만 조회하고 체크포인트에 보관한다.
    # 기존에는 재시도마다 이 목록을 다시 받아왔다.
    itemIDList = cp.level0 if (resume and cp.level0) else None
    if itemIDList is None:
        try:
            itemIDList = callWithRetry(returnItemID_rs, base, max_retries=item_retries,
                                       label=f"{rsList[0]} level0")
        except EmptyDataError:
            logger.info("level-0 데이터 없음: %s %s", rsList[1], startDate)
            return 0
        cp.setLevel0(itemIDList)
    else:
        logger.info("[%s %s] level-0 목록 재사용 (%d건)",
                    rsList[0], startDate, len(itemIDList))

    if limit1:
        itemIDList = itemIDList[:limit1]

    counter = {"rows": 0}

    def work(value, item_id):
        payload = ChangeItemID(item_id, bd)
        try:
            dataFrame = dataretriever_data(payload)
        except EmptyDataError:
            if on_empty != "fill":
                raise
            df = _emptyFrame(startDate, endDate, period, jsonFile, tbColumn, epp,
                             False, False, start_hour, end_hour, end_mode)
            df.iloc[0, df.columns.get_loc("site_code")] = rsList[0]
            if "dimension" in df.columns:
                df.iloc[0, df.columns.get_loc("dimension")] = value
            n = stackTodb(df, dbTableName, key_columns)
            counter["rows"] += n
            return n

        if limit2:
            dataFrame = dataFrame.head(limit2)
        dataFrame.insert(0, "site_code", rsList[0], True)
        dataFrame.insert(1, "dimension", value, True)
        dataFrame.insert(3, "period", period, True)
        dataFrame.insert(4, "start_date", window.start_label, True)
        dataFrame.insert(5, "end_date", window.end_label, True)
        dataFrame.insert(6, "epp", epp, True)
        if extra != "":
            dataFrame.insert(7, "extra", extra, True)
        if extra1 != "":
            dataFrame.insert(8, "extra1", extra1, True)
        dataFrame.columns = tbColumn
        n = stackTodb(dataFrame, dbTableName, key_columns)
        counter["rows"] += n
        return n

    _runItems(itemIDList, work, cp, item_retries, resume, retry_failed,
              f"{rsList[0]} {startDate} bd", progress_every=progress_every,
              progress_mode=progress_mode)
    return counter["rows"]


def secondCaller1(startDate, endDate, jsonFile, jsonFilebreakdown, rsList, limit,
                  period, tbColumn, dbTableName, epp, extra="", extra1="",
                  start_hour="00:00", end_hour="00:00", **kw):
    """[유지] 하위 호환용. secondCaller(limit1=0, limit2=limit)와 같다."""
    return secondCaller(startDate, endDate, jsonFile, jsonFilebreakdown, rsList,
                        period, tbColumn, dbTableName, epp, 0, limit,
                        extra, extra1, start_hour, end_hour, **kw)


# ---------------------------------------------------------------------------
# 3단(2nd breakdown) 수집 — 신규
# ---------------------------------------------------------------------------

def assertMetricCount(jsonFile, tbColumn):
    """tbColumn 개수와 리포트 metric 개수가 맞는지 수집 시작 전에 확인한다.

    불일치는 설정 오류라 모든 구간에서 똑같이 실패한다. 루프 안에서 터지면
    _runTask가 로그만 남기고 None을 돌려주어 호출자가 눈치채기 어려우므로,
    첫 API 호출 전에 예외를 올려 즉시 멈춘다.
    """
    n = len(tbColumn)
    reported = len(readJson(jsonFile).get('metricContainer', {}).get('metrics', []))
    if reported and reported != n:
        raise PermanentError(
            f"tbColumn 개수({n})가 리포트의 metric 개수({reported})와 다릅니다. "
            f"JSON의 metricContainer.metrics 를 확인하세요. tbColumn={list(tbColumn)}")
    return reported


def thirdLevelFrame(startDate, endDate, period, jsonFile, breakdown1, breakdown2,
                    rsList, tbColumn, epp, limit0=0, limit1=0, limit2=0,
                    extra="", extra1="", start_hour="00:00", end_hour="00:00",
                    end_mode="legacy", on_empty="fill", fill_value=0,
                    dimension_fill="(none)", item_retries=3, progress_every=1,
                    progress_mode="auto"):
    """dimension > breakdown1 > breakdown2 를 조회해 DataFrame으로 돌려준다.

    breakdown1/2는 dimension 이름("variables/evar3")이면 JSON을 자동 생성하고,
    ".json" 경로면 기존처럼 템플릿 파일을 사용한다.
    """
    dataInitiator()
    window = makeWindow(startDate, endDate, period, start_hour, end_hour, end_mode)

    base = applyWindow(jsonFile, window)
    bd1 = applyWindow(_loadOrBuildBreakdown(base, breakdown1), window)
    bd2 = applyWindow(_loadOrBuildBreakdown(bd1, breakdown2), window)
    if rsList:
        base = rsIDchange(base, rsList[1])
        bd1 = rsIDchange(bd1, rsList[1])
        bd2 = rsIDchange(bd2, rsList[1])
        site_code = rsList[0]
    else:
        site_code = _siteCodeFromRsid(base['rsid'])

    dim0, dim1 = base['dimension'], bd1['dimension']
    nMetrics = len(tbColumn)

    # [방어] tbColumn 개수와 리포트 metric 개수가 다르면 조용히 망가진다.
    #   적게 주면  -> 뒤쪽 지표가 잘린 채 적재됨 (경고 없음)
    #   많이 주면  -> DataFrame 생성 실패 후 상위에서 None으로 삼켜짐
    # 재호출로 해결될 문제가 아니므로 PermanentError로 즉시 중단한다.
    reportMetrics = len(base.get('metricContainer', {}).get('metrics', []))
    if reportMetrics and reportMetrics != nMetrics:
        raise PermanentError(
            f"tbColumn 개수({nMetrics})가 리포트의 metric 개수({reportMetrics})와 "
            f"다릅니다. JSON의 metricContainer.metrics 를 확인하세요. "
            f"tbColumn={list(tbColumn)}")
    prefix_extra = ([extra] if extra != "" else []) + ([extra1] if extra1 != "" else [])

    def rec(d0, d1, d2, metrics):
        return ([site_code, d0, d1, d2, period, window.start_label,
                 window.end_label, epp] + prefix_extra + list(metrics))

    records = []
    try:
        l0 = callWithRetry(returnItemID_rs, base, max_retries=item_retries,
                           label="3rd level0")
    except EmptyDataError:
        l0 = []
    if limit0:
        l0 = l0[:limit0]
    if not l0:
        if on_empty == "fill":
            records.append(rec(dimension_fill, dimension_fill, dimension_fill,
                               _zeroRow(nMetrics, fill_value)))
        return _thirdFrame(records, tbColumn, extra, extra1)

    # [수정] 각 항목을 개별적으로 재시도하고, 소진되면 그 항목만 건너뛴다.
    # 기존에는 예외가 위로 전파되어 상위 재시도가 level-0부터 전부 다시 돌렸다.
    skipped = []
    pr = _Progress(len(l0), f"3rd {startDate}", progress_every,
                   unit="행", mode=progress_mode)

    for v0, id0 in l0:
        try:
            df1 = callWithRetry(dataretriever_data_breakdown,
                                ChangeItemID([(dim0, id0)], bd1),
                                max_retries=item_retries, label=f"3rd L1 {v0}")
        except EmptyDataError:
            filled = 0
            if on_empty == "fill":
                records.append(rec(v0, dimension_fill, dimension_fill,
                                   _zeroRow(nMetrics, fill_value)))
                filled = 1
            pr.step(v0, "ok", rows=filled, detail="(1단 데이터 없음)")
            continue
        except Exception as e:
            skipped.append((v0, None, f"{type(e).__name__}: {e}"))
            pr.log(logging.ERROR, "[3rd] %s 건너뜀 — %s: %s",
                   v0, type(e).__name__, e)
            if on_empty == "fill":
                records.append(rec(v0, dimension_fill, dimension_fill,
                                   _zeroRow(nMetrics, fill_value)))
            pr.step(v0, "fail", rows=1 if on_empty == "fill" else 0,
                    detail=f"— {type(e).__name__}")
            continue

        cols = list(map(str, range(df1.shape[1])))
        cols[0], cols[-1] = 'value', 'item_id'
        df1.columns = cols
        l1 = df1[['value', 'item_id']].values.tolist()
        if limit1:
            l1 = l1[:limit1]

        before = len(records)
        for j, (v1, id1) in enumerate(l1, 1):
            if progress_every and len(l1) > 1:
                logger.debug("[3rd %s] %s 하위 (%d/%d) %s",
                             startDate, v0, j, len(l1), v1)
            try:
                df2 = callWithRetry(dataretriever_data,
                                    ChangeItemID([(dim0, id0), (dim1, id1)], bd2),
                                    max_retries=item_retries,
                                    label=f"3rd L2 {v0}>{v1}")
            except EmptyDataError:
                if on_empty == "fill":
                    records.append(rec(v0, v1, dimension_fill,
                                       _zeroRow(nMetrics, fill_value)))
                continue
            except Exception as e:
                skipped.append((v0, v1, f"{type(e).__name__}: {e}"))
                pr.log(logging.ERROR, "[3rd] %s > %s 건너뜀 — %s: %s",
                       v0, v1, type(e).__name__, e)
                if on_empty == "fill":
                    records.append(rec(v0, v1, dimension_fill,
                                       _zeroRow(nMetrics, fill_value)))
                continue
            if limit2:
                df2 = df2.head(limit2)
            got = df2.shape[1] - 1                     # 첫 칼럼은 dimension 값
            if got != nMetrics:
                raise PermanentError(
                    f"응답 지표 {got}개 vs tbColumn {nMetrics}개 불일치 "
                    f"({v0} > {v1}). 리포트 JSON과 tbColumn을 맞추세요.")
            for row in df2.values.tolist():
                records.append(rec(v0, v1, row[0], row[1:1 + nMetrics]))

        pr.step(v0, "ok", rows=len(records) - before,
                detail=f"(하위 {len(l1)}개)")

    pr.finish()
    if skipped:
        logger.warning("[3rd] 건너뛴 항목 %d건: %s", len(skipped),
                       [f"{a}>{b}" if b else a for a, b, _ in skipped[:8]])
    return _thirdFrame(records, tbColumn, extra, extra1)


def _thirdFrame(records, tbColumn, extra, extra1):
    cols = ["site_code", "dimension", "breakdown", "breakdown2",
            "period", "start_date", "end_date", "is_epp"]
    if extra != "":
        cols.append("extra")
    if extra1 != "":
        cols.append("extra1")
    cols += list(tbColumn)
    df = pd.DataFrame(records, columns=cols)
    for c in tbColumn:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


# ---------------------------------------------------------------------------
# hourly 수집 — 신규
# ---------------------------------------------------------------------------

def hourlyFrame(startDate, endDate, jsonFile, rsList, tbColumn, epp,
                start_hour="00:00", end_hour="00:00", limit=0,
                extra="", extra1="", end_mode="legacy",
                if_site_code=False, on_empty="fill", fill_value=0,
                dimension_fill="(none)", on_error="fill"):
    """1시간짜리 호출 N개로 수집하고, 요청한 시간대 수만큼 정확히 돌려준다.

    시간 라벨은 우리가 만든 창에서 나오므로 Adobe 응답 문자열 파싱이 없다.
    데이터가 없거나 호출이 실패한 시간대는 지표 0으로 채운다.
    (건너뛰면 '수집 안 됨'과 '트래픽 0'이 구분되지 않고 시계열에 구멍이 난다)
    """
    dataInitiator()
    windows = periodSlicer(startDate, endDate, "hourly", start_hour, end_hour, end_mode)
    nMetrics = len(tbColumn)

    # [방어] thirdLevelFrame과 같은 이유 (조용한 지표 잘림/None 반환 방지)
    _base = readJson(jsonFile)
    _rm = len(_base.get('metricContainer', {}).get('metrics', []))
    if _rm and _rm != nMetrics:
        raise PermanentError(
            f"tbColumn 개수({nMetrics})가 리포트의 metric 개수({_rm})와 다릅니다. "
            f"tbColumn={list(tbColumn)}")
    prefix_extra = ([extra] if extra != "" else []) + ([extra1] if extra1 != "" else [])

    cols = ["site_code"]
    if not if_site_code:
        cols.append("dimension")
    cols += ["period", "start_date", "end_date", "is_epp"]
    if extra != "":
        cols.append("extra")
    if extra1 != "":
        cols.append("extra1")
    cols += list(tbColumn)

    records, filled, failed = [], 0, 0
    for w in windows:
        payload = applyWindow(jsonFile, w)
        if rsList:
            payload = rsIDchange(payload, rsList[1])
            code = rsList[0]
        else:
            code = _siteCodeFromRsid(payload['rsid'])

        def head(dim):
            base = [code] + ([] if if_site_code else [dim])
            return base + ["hourly", w.start_label, w.end_label, epp] + prefix_extra

        try:
            df = dataretriever_data(payload)
        except EmptyDataError:
            if on_empty == "fill":
                records.append(head(dimension_fill) + _zeroRow(nMetrics, fill_value))
                filled += 1
            continue
        except Exception as e:
            failed += 1
            if on_error == "raise":
                raise
            logger.error("[%s] 수집 실패 (%s: %s) — 0으로 채웁니다",
                         w.start_label, type(e).__name__, e)
            records.append(head(dimension_fill) + _zeroRow(nMetrics, fill_value))
            continue

        if limit:
            df = df.head(limit)
        for row in df.values.tolist():
            if if_site_code:
                records.append(head(None) + list(row[:nMetrics]))
            else:
                records.append(head(row[0]) + list(row[1:1 + nMetrics]))

    out = pd.DataFrame(records, columns=cols)
    for c in tbColumn:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    covered = out['start_date'].nunique() if len(out) else 0
    logger.info("hourly: 요청 %d시간 / 수집 %d시간 / 데이터없음 %d / 실패 %d / %d행",
                len(windows), covered, filled, failed, len(out))
    if covered != len(windows):
        logger.warning("시간대 %d개가 결과에 없습니다.", len(windows) - covered)
    return out


# ---------------------------------------------------------------------------
# 병렬 워커
# ---------------------------------------------------------------------------

def _runTask(fn, label, max_retries, *args, **kwargs):
    start = time.time()
    try:
        result = callWithRetry(fn, *args, max_retries=max_retries, label=label, **kwargs)
        logger.info("[%s] 완료 %.2fs", label, time.time() - start)
        return result
    except PermanentError as e:
        logger.error("[%s] 설정/스키마 오류로 중단: %s", label, e)
    except Exception as e:
        logger.error("[%s] 실패, 다음 작업으로 이동: %s: %s", label, type(e).__name__, e)
    return None


def worker_refine_RS(task):
    (startDate, endDate, jsonLocation, rs, period, tbColumn, dbTableName,
     epp, limit, extra, extra1, start_hour, end_hour, max_retries) = task[:14]
    key_columns = task[14] if len(task) > 14 else None
    end_mode = task[15] if len(task) > 15 else "legacy"
    on_empty = task[16] if len(task) > 16 else "skip"
    return _runTask(refineRsIDChange, f"{rs[0]} {startDate}", max_retries,
                    startDate, endDate, jsonLocation, rs, period, tbColumn,
                    dbTableName, epp, limit, extra, extra1, start_hour, end_hour,
                    key_columns=key_columns, end_mode=end_mode, on_empty=on_empty)


def worker_refine_RB(task):
    (startDate, endDate, jsonLocation, rs, period, tbColumn, dbTableName,
     epp, limit, Biz_type, Device_type, Division, Category, site_code_ae,
     start_hour, end_hour, max_retries) = task[:17]
    key_columns = task[17] if len(task) > 17 else None
    end_mode = task[18] if len(task) > 18 else "legacy"
    on_empty = task[19] if len(task) > 19 else "skip"
    return _runTask(refineRsIDChangeRB, f"{rs[0]} {startDate}", max_retries,
                    startDate, endDate, jsonLocation, rs, period, tbColumn,
                    dbTableName, epp, limit, Biz_type, Device_type, Division,
                    Category, site_code_ae, start_hour, end_hour,
                    key_columns=key_columns)


def worker_refine_breakdown(task):
    """RS별 breakdown 병렬 워커. with_total=True 면 Total 을 먼저 적재한다.

    Total 과 breakdown 을 각각 따로 실행한다. 한 덩어리로 묶으면
    breakdown 실패가 Total 재적재로 이어진다.
    """
    (startDate, endDate, jsonFile, jsonFilebreakdown, rs, period, tbColumn,
     dbTableName, epp, limit1, limit2, extra, extra1, start_hour, end_hour,
     max_retries, key_columns, end_mode, on_empty, checkpoint_dir, resume,
     item_retries, retry_failed, progress_every, progress_mode,
     with_total) = task[:26]

    if with_total:
        _runTask(refineRsIDChangeTotal, f"{rs[0]} {startDate} total", max_retries,
                 startDate, endDate, jsonFile, rs, period, tbColumn, dbTableName,
                 epp, limit1, extra, extra1, start_hour, end_hour,
                 key_columns=key_columns, end_mode=end_mode)

    return _runTask(secondCaller, f"{rs[0]} {startDate} bd", max_retries,
                    startDate, endDate, jsonFile, jsonFilebreakdown, rs, period,
                    tbColumn, dbTableName, epp, limit1, limit2, extra, extra1,
                    start_hour, end_hour, key_columns=key_columns,
                    end_mode=end_mode, on_empty=on_empty,
                    checkpoint_dir=checkpoint_dir, resume=resume,
                    item_retries=item_retries, retry_failed=retry_failed,
                    progress_every=progress_every, progress_mode=progress_mode)


def worker_refine_common(task):
    """[수정] 인자 개수가 맞지 않아 호출 즉시 TypeError가 나던 함수.

    기존에는 refineRsIDChangeRB(16개 필수)에 14개만 넘겨
    extra가 Biz_type 자리에 들어가고 start_hour/end_hour가 누락됐다.
    상위 except Exception이 이를 5회 삼킨 뒤 "다음 작업으로 이동"만 출력하므로
    잘못된 코드가 정상 종료처럼 보였다.
    """
    (startDate, endDate, jsonLocation, rs, period, tbColumn, dbTableName,
     epp, limit, extra, extra1, start_hour, end_hour, max_retries, mode) = task[:15]
    key_columns = task[15] if len(task) > 15 else None

    if mode == "RB":
        return _runTask(refineRsIDChangeRB, f"{rs[0]} {startDate} RB", max_retries,
                        startDate, endDate, jsonLocation, rs, period, tbColumn,
                        dbTableName, epp, limit,
                        extra, extra1, "", "", "",          # Biz/Device/Division/Category/site_code_ae
                        start_hour, end_hour, key_columns=key_columns)
    return _runTask(refineRsIDChange, f"{rs[0]} {startDate} RS", max_retries,
                    startDate, endDate, jsonLocation, rs, period, tbColumn,
                    dbTableName, epp, limit, extra, extra1, start_hour, end_hour,
                    key_columns=key_columns)


# ---------------------------------------------------------------------------
# 진단 SQL
# ---------------------------------------------------------------------------

DUPLICATE_HOURS_SQL = """
-- 같은 site_code / 같은 시간대가 여러 실행에서 중복 적재된 건
SELECT site_code, {hour_col} AS hour_bucket, COUNT(*) AS row_cnt,
       COUNT(DISTINCT start_date) AS run_cnt,
       MIN(start_date) AS first_run, MAX(start_date) AS last_run
FROM `{table}`
WHERE start_date LIKE '% %'
GROUP BY site_code, {hour_col}
HAVING COUNT(*) > 1
ORDER BY row_cnt DESC LIMIT 200;
""".strip()

MISLABELED_ROWS_SQL = """
-- end_date 라벨이 하루 밀린 행 (시각 지정 수집분)
SELECT site_code, start_date, end_date, COUNT(*) AS row_cnt
FROM `{table}`
WHERE start_date LIKE '% %'
  AND DATE(SUBSTRING_INDEX(end_date, ' ', 1)) > DATE(SUBSTRING_INDEX(start_date, ' ', 1))
GROUP BY site_code, start_date, end_date
ORDER BY start_date DESC;
""".strip()
