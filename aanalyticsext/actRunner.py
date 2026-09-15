# 수집 단위 계산 — 기간/시각 경계, site_code 확장, 적재 컬럼 구성.
#
# 이 모듈은 "언제, 어느 리포트스위트를, 어떤 컬럼으로" 가져올지를 정한다.
# 실제 API 호출은 actModuler, 작업 실행은 actExecute가 맡는다.

from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time as dtime
import logging
import re

logger = logging.getLogger("act.runner")


# ---------------------------------------------------------------------------
# 조직 데이터 (프로파일로 주입된다)
# ---------------------------------------------------------------------------
# 이 파일의 매핑 컨테이너들은 비어 있는 채로 배포된다. 어떤 리포트스위트가
# 어느 국가인지, 어떤 법인이 어느 국가를 관할하는지는 회사마다 다르고 대개
# 대외비라서 엔진에 넣지 않는다.
#
#     from aanalyticsext import load_profile
#     load_profile("my_profile.json")
#
# 프로파일은 이 객체들의 "내용"을 제자리에서 채운다. `from ... import *` 로
# 이름을 가져다 쓴 곳이 있어서 리바인딩이 아니라 in-place 갱신이어야 한다.
# 형식은 profile.py 참고.

# site_code -> rsid 매핑. [[site_code, rsid], ...]

_DEFAULT_EPP = []

_DEFAULT_NONE = []

_DATE_FMT = "%Y-%m-%d"
_HOUR_FMT = "%H:%M"

# 종료 경계 정책
#   "legacy"    : end_hour + 1분   [09:00, 18:01)  -> 09~18시 (기존 동작, 기본값)
#   "exclusive" : end_hour 그대로  [09:00, 18:00)  -> 09~17시
#   "inclusive" : end_hour + 1시간 [09:00, 19:00)  -> 09~18시
END_MODES = ("legacy", "exclusive", "inclusive")


def dateConverter(date):
    return datetime.strptime(str(date), _DATE_FMT).date()


def lastDayofMonth(date_value):
    return date_value.replace(day=monthrange(date_value.year, date_value.month)[1])


def _parse_date(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v), _DATE_FMT).date()


def _parse_hour(v):
    return datetime.strptime(str(v), _HOUR_FMT).time()


# ---------------------------------------------------------------------------
# 수집 단위
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TimeWindow:
    """수집 구간 하나. 날짜와 시각을 함께 들고 다닌다.

    [수정] 기존에는 종료 경계를 날짜(+1일)와 시각(+1분) 두 곳에서 따로 보정했다.
    두 보정이 겹치면서 시각을 지정하면 조회 구간이 하루 넓어졌다.
        의도 : 2026-06-01 09:00 ~ 18:00 (10개 시간대)
        기존 : 2026-06-01T09:00 / 2026-06-02T18:01  -> 34개 시간대
    여기서는 종료 경계를 _end_exclusive 하나로만 계산하므로
    두 보정이 겹칠 수 있는 경로 자체가 없다.
    """
    start_date: date
    end_date: date
    start_hour: dtime
    end_hour: dtime
    granularity: str = "daily"      # daily|weekly|monthly|all|hourly
    end_mode: str = "legacy"
    whole_day: bool = True          # 시각 지정 없이 날짜 전체를 조회

    @property
    def _start_dt(self):
        return datetime.combine(self.start_date, self.start_hour)

    @property
    def _end_exclusive(self):
        if self.whole_day:
            return datetime.combine(self.end_date + timedelta(days=1), dtime(0, 0))

        base = datetime.combine(self.end_date, self.end_hour)
        if self.end_mode == "legacy":
            end = base + timedelta(minutes=1)
        elif self.end_mode == "inclusive":
            end = base + timedelta(hours=1)
        else:
            end = base

        # 자정을 넘기는 창(22:00~02:00)은 종료가 시작보다 앞서게 계산된다.
        # 처리하지 않으면 역방향 dateRange가 되어 구간이 통째로 빈다.
        if end <= self._start_dt:
            end += timedelta(days=1)
        return end

    @property
    def date_range(self):
        """globalFilters에 넣을 dateRange 문자열."""
        s, e = self._start_dt, self._end_exclusive
        return (f"{s.strftime(_DATE_FMT)}T{s.strftime(_HOUR_FMT)}:00.000/"
                f"{e.strftime(_DATE_FMT)}T{e.strftime(_HOUR_FMT)}:00.000")

    @property
    def start_label(self):
        """DB start_date 컬럼 값."""
        if self.whole_day:
            return str(self.start_date)
        return f"{self.start_date} {self.start_hour.strftime(_HOUR_FMT)}"

    @property
    def end_label(self):
        """DB end_date 컬럼 값.

        [수정] 기존에는 EndDateCalculation("0", endDate)[1] 즉 종료일+1을 넣어
        시각 지정 시 라벨이 하루 밀렸다. 요청한 종료 시점을 그대로 기록한다.
        hourly는 start==end가 되어버리므로 구간 종료 시각을 기록한다.
        """
        if self.whole_day:
            return str(self.end_date)
        if self.granularity == "hourly":
            e = self._end_exclusive
            return f"{e.strftime(_DATE_FMT)} {e.strftime(_HOUR_FMT)}"
        return f"{self.end_date} {self.end_hour.strftime(_HOUR_FMT)}"

    @property
    def duration_hours(self):
        return (self._end_exclusive - self._start_dt).total_seconds() / 3600.0

    def __str__(self):
        return f"[{self.granularity}] {self.start_label} ~ {self.end_label}"


def buildDateRange(startDate, endDate, start_hour="00:00", end_hour="00:00",
                   end_mode="legacy"):
    """단일 구간의 dateRange 문자열. jsonDateChange가 사용한다."""
    whole = (start_hour == "00:00" and end_hour == "00:00")
    return TimeWindow(_parse_date(startDate), _parse_date(endDate),
                      _parse_hour(start_hour), _parse_hour(end_hour),
                      "all", end_mode, whole).date_range


# ---------------------------------------------------------------------------
# 기간 분할
# ---------------------------------------------------------------------------

def periodSlicer(startDate, endDate, period, start_hour="00:00", end_hour="00:00",
                 end_mode="legacy", clamp=True):
    """기간을 TimeWindow 리스트로 분할한다.

    [수정] clamp=True(기본)일 때 마지막 구간이 요청 endDate를 넘지 않는다.
    기존 weekly/monthly는 요청 범위를 넘는 구간을 만들어
    진행 중인(미완결) 기간을 완결 데이터처럼 적재했다.
        weekly  2026-06-01~06-30 -> 마지막 구간이 2026-07-05 까지
        monthly 2026-01-15~04-10 -> 마지막 구간이 2026-04-30 까지
    clamp=False로 두면 완전한 주/월만 남기고 미완결 구간은 버린다.

    period="hourly"는 [start_hour, end_hour]를 1시간 단위로 쪼갠다.
    """
    if end_mode not in END_MODES:
        raise ValueError(f"end_mode는 {END_MODES} 중 하나여야 합니다.")

    s, e = _parse_date(startDate), _parse_date(endDate)
    if s > e:
        raise ValueError(f"startDate({s})가 endDate({e})보다 늦습니다.")

    sh, eh = _parse_hour(start_hour), _parse_hour(end_hour)
    whole = (start_hour == "00:00" and end_hour == "00:00")
    p = str(period).lower()
    out = []

    def W(d0, d1):
        return TimeWindow(d0, d1, sh, eh, p, end_mode, whole)

    if p == "hourly":
        # 종료 경계 규칙을 end_mode와 일치시킨다. 어긋나면 시간 dimension 방식과
        # 시간 슬라이싱 방식이 같은 요청에 서로 다른 개수를 내놓는다.
        start_h = sh.hour
        if whole:
            end_h = start_h + 24
        else:
            end_h = eh.hour if end_mode == "exclusive" else eh.hour + 1
            if end_h <= start_h:
                end_h += 24                     # 자정 넘김
        cur = s
        while cur <= e:
            for h in range(start_h, end_h):
                day = cur + timedelta(days=h // 24)
                out.append(TimeWindow(day, day, dtime(h % 24, 0), dtime(h % 24, 0),
                                      "hourly", "inclusive", False))
            cur += timedelta(days=1)
        seen, uniq = set(), []
        for w in sorted(out, key=lambda x: (x.start_date, x.start_hour)):
            k = (w.start_date, w.start_hour)
            if k not in seen:
                seen.add(k)
                uniq.append(w)
        out = uniq

    elif p == "daily":
        cur = s
        while cur <= e:
            out.append(W(cur, cur))
            cur += timedelta(days=1)

    elif p == "weekly":
        cur = s
        while cur <= e:
            bucket_end = cur + timedelta(days=6)
            if bucket_end > e:
                if not clamp:
                    break
                bucket_end = e
            out.append(W(cur, bucket_end))
            cur = bucket_end + timedelta(days=1)

    elif p == "monthly":
        cur = s
        while cur <= e:
            last = lastDayofMonth(cur)
            bucket_end = last
            if bucket_end > e:
                if not clamp:
                    break
                bucket_end = e
            out.append(W(cur, bucket_end))
            cur = last + timedelta(days=1)

    elif p == "all":
        out.append(W(s, e))

    else:
        raise ValueError("period는 daily, weekly, monthly, all, hourly 중 하나여야 합니다.")

    return out


def dateGenerator(startDate, endDate, period, clamp=True):
    """기존 시그니처 호환 — (시작일 리스트, 종료일 리스트)를 돌려준다."""
    if str(period).lower() == "hourly":
        raise ValueError(
            "hourly는 dateGenerator로 표현할 수 없습니다. "
            "periodSlicer(..., 'hourly', start_hour, end_hour) 또는 "
            "actExecute.retrieve_Hourly()를 사용하세요.")
    ws = periodSlicer(startDate, endDate, period, clamp=clamp)
    return [str(w.start_date) for w in ws], [str(w.end_date) for w in ws]


def findOverlaps(windows):
    """겹치는 구간 쌍. 겹치면 같은 데이터가 중복 적재된다."""
    ws = sorted(windows, key=lambda w: w._start_dt)
    return [(ws[i], ws[i + 1]) for i in range(len(ws) - 1)
            if ws[i]._end_exclusive > ws[i + 1]._start_dt]


# ---------------------------------------------------------------------------
# RS 매핑
# ---------------------------------------------------------------------------

# 사이트가 닫혔거나 다른 코드로 통합되어 더 이상 수집하지 않는 코드.
# {code: 사유}. returnRsList가 자동으로 제외하고 경고를 남긴다.
# 과거 데이터 재수집 등으로 일부러 포함해야 하면 include_deprecated=True.
DEPRECATED_SITE_CODES = {}

# 마이그레이션 이전 RS. 새 RS가 생기면서 남은 이전 코드. {code: 사유}.
# 신규 코드와 함께 수집하면 같은 트래픽이 두 번 잡히므로 그룹으로 펼칠 때는
# 기본 제외한다. 과거 구간 재수집이 필요하면 rsInput에 직접 적거나
# include_legacy=True.
LEGACY_SITE_CODES = {}


# ---------------------------------------------------------------------------
# 조직 그룹 — Region > Subsidiary > site_code
# ---------------------------------------------------------------------------
# rsInput에 site_code 대신 그룹 이름을 넣으면 소속 코드 전체로 펼쳐진다.
#
#   returnRsList(False, ["<법인명>"])           # 법인 단위
#   returnRsList(False, ["<지역명>"])           # 지역 단위
#   returnRsList(False, ["<지역명>", "jp"])     # 지역 + 개별 코드 혼용
#
# 주의: 지리적 직관과 법인 관할이 어긋나는 경우가 흔하고, 약어가 뜻하는 바도
# 조직마다 다르다. 틀린 확장은 조용히 잘못된 국가를 수집하므로 프로파일을
# 새로 쓰거나 고친 뒤에는 반드시 validateSiteGroups()로 점검할 것.
SUBSIDIARIES = {}

# 법인이 아직 정해지지 않은 site_code.
# 여기 있는 코드는 그룹으로 펼쳐지지 않으므로 rsInput에 직접 적어야 수집된다.
UNASSIGNED_SITE_CODES = []

REGIONS = {}

# 하위 통합 RS를 알아보는 접두사. 글로벌 통합본과 이중 계상되지 않도록
# 그룹 확장에서 빼두는 코드들이며, validateSiteGroups가 이것만 따로 알려준다.
SUB_TOTAL_PREFIX = ""


def loadSiteGroups(path):
    """Region/Subsidiary 매핑을 외부 JSON으로 대체한다.

    코드를 고치지 않고 조직 개편을 반영하기 위한 통로다.
        {"SUBSIDIARIES": {"<법인>": ["uk"]}, "REGIONS": {"<지역>": ["<법인>"]}}
    """
    import json as _json
    with open(path, "r", encoding="utf-8") as f:
        data = _json.load(f)
    if "SUBSIDIARIES" in data:
        SUBSIDIARIES.clear()
        SUBSIDIARIES.update(data["SUBSIDIARIES"])
    if "REGIONS" in data:
        REGIONS.clear()
        REGIONS.update(data["REGIONS"])
    logger.info("그룹 매핑 로드: 법인 %d개, 지역 %d개",
                len(SUBSIDIARIES), len(REGIONS))


def _normGroup(name):
    """그룹 이름 정규화. 대소문자·공백·하이픈·언더바 차이를 흡수한다.

    'SE ASIA' / 'se_asia' / 'seasia' / 'SE-ASIA' 를 모두 같은 그룹으로 본다.
    (하이픈이 든 법인명도 하이픈 없이 찾을 수 있다: 'AB-C' -> 'abc')
    """
    return re.sub(r"[\s_\-]+", "", str(name)).lower()


def _groupLookup():
    """그룹 이름 색인. 원래 이름과 정규화 이름을 모두 등록한다."""
    idx = {}
    for kind, table in (("subsidiary", SUBSIDIARIES), ("region", REGIONS)):
        for name, members in table.items():
            entry = (kind, name, list(members))
            idx[name.lower()] = entry
            idx.setdefault(_normGroup(name), entry)
    return idx


# 흔히 쓰이는 다른 표기 -> 공식 그룹 이름. {별칭(정규화형): 그룹명}
# 조직표 기준 이름이 아니어도 통하게 해준다. 뜻이 갈리는 표기
# (예: "Americas"가 북미인지 북미+중남미인지 불분명)는 넣지 않는 편이 낫다.
# 약어가 다른 법인 코드와 충돌하면 엉뚱한 국가가 조용히 수집되므로,
# 실제 그룹 이름이 항상 별칭보다 우선하도록 되어 있다(expandSiteCodes 참고).
GROUP_ALIASES = {}


def suggestGroup(token, cutoff=0.8):
    """입력과 가장 비슷한 그룹 이름을 돌려준다 (없으면 None).

    지역명이 조직 개편으로 바뀌거나('Europe' -> 'EU') 오타가 났을 때,
    그냥 '없는 코드'라고만 하면 왜 안 되는지 알기 어렵다.
    """
    import difflib
    names = list(SUBSIDIARIES) + list(REGIONS)
    norm = {_normGroup(n): n for n in names}
    key = _normGroup(token)
    if key in norm:
        return norm[key]
    if key in GROUP_ALIASES:
        return GROUP_ALIASES[key]
    # 기준을 낮추면 엉뚱한 제안이 나온다("Americas" -> "AFRICA").
    # 틀린 제안은 없느니만 못하므로 확실할 때만 돌려준다.
    hit = difflib.get_close_matches(key, list(norm), n=1, cutoff=cutoff)
    return norm[hit[0]] if hit else None


def expandSiteCodes(tokens, include_deprecated=False, strict=False,
                    include_legacy=False, with_origin=False):
    """site_code / 법인명 / 지역명이 섞인 목록을 site_code 목록으로 펼친다.

    - 그룹 이름은 대소문자를 구분하지 않는다 ("europe", "Europe", "EUROPE" 동일)
    - 지역은 법인을 거쳐 재귀적으로 펼친다
    - 순서를 유지하며 중복은 제거한다
    - 사용 중단 코드는 제외하고 경고한다
    """
    if isinstance(tokens, str):
        tokens = [tokens]
    idx = _groupLookup()
    out, seen, dropped, legacy = [], set(), [], []

    def add(code, origin):
        if code in seen:
            return
        if code in DEPRECATED_SITE_CODES and not include_deprecated:
            dropped.append((code, origin))
            return
        # 레거시 코드는 '그룹으로 펼쳐진 경우'에만 뺀다.
        # 사용자가 직접 적었다면 과거 구간 재수집 의도로 보고 존중한다.
        if (code in LEGACY_SITE_CODES and not include_legacy
                and origin != "직접 입력"):
            legacy.append((code, origin))
            return
        seen.add(code)
        out.append((code, origin))

    def walk(token, origin, chain=()):
        # 원래 표기로 먼저 찾고, 없으면 공백/하이픈/언더바를 무시하고 다시 찾는다
        # 실제 그룹 이름이 항상 우선한다. 별칭은 그 뒤에만 본다.
        # (예: "SEA"는 미국 법인이므로 동남아 별칭이 이를 덮으면 안 된다)
        hit = (idx.get(str(token).lower())
               or idx.get(_normGroup(token))
               or idx.get(_normGroup(GROUP_ALIASES.get(_normGroup(token), ""))))
        # 그룹 이름과 site_code가 대소문자만 다르게 겹칠 수 있다
        # (법인 "AB" vs 코드 "ab"). 이미 펼치는 중인 그룹을 다시 만나면
        # 그룹이 아니라 site_code로 본다. 그러지 않으면 자기 자신을
        # 무한 참조해 해당 코드가 조용히 누락된다.
        if hit is None or hit[1] in chain:
            add(token, origin)
            return
        kind, name, members = hit
        if len(chain) > 5:
            logger.warning("그룹 중첩이 너무 깊습니다: %s", name)
            return
        for m in members:
            walk(m, name, chain + (name,))

    for t in tokens:
        walk(t, "직접 입력")

    if dropped:
        msg = ", ".join(
            f"{c}({DEPRECATED_SITE_CODES[c]}, 출처: {o})" for c, o in dropped)
        if strict:
            raise ValueError(f"사용 중단된 site_code가 포함되어 있습니다: {msg}")
        logger.warning("사용 중단 site_code 제외: %s "
                       "(포함하려면 include_deprecated=True)", msg)
    if legacy:
        logger.info("레거시 site_code %d개 제외(신규 RS와 이중 계상 방지): %s "
                    "(포함하려면 include_legacy=True)",
                    len(legacy), [c for c, _ in legacy])
    return out if with_origin else [c for c, _ in out]


def listSiteGroups():
    """사용 가능한 지역/법인 이름과 소속 site_code 수를 돌려준다."""
    rows = []
    for name in REGIONS:
        rows.append(("region", name, len(expandSiteCodes([name]))))
    for name in SUBSIDIARIES:
        rows.append(("subsidiary", name, len(expandSiteCodes([name]))))
    return rows


def describeSiteGroup(name):
    """특정 지역/법인에 어떤 site_code가 들어가는지 확인한다."""
    hit = _groupLookup().get(str(name).lower())
    if hit is None:
        raise KeyError(f"'{name}'은(는) 등록된 지역/법인이 아닙니다. "
                       f"listSiteGroups()로 목록을 확인하세요.")
    kind, real, members = hit
    return {"kind": kind, "name": real, "members": members,
            "site_codes": expandSiteCodes([real])}


def validateSiteGroups(epp=False, verbose=True):
    """그룹 매핑이 실제 RS 목록과 맞는지 점검한다.

    매핑을 수정하거나 loadSiteGroups()로 갈아끼운 뒤 이 함수를 돌려
    오타·누락·유령 코드를 잡는다. 반환값이 비어 있으면 정상이다.
    """
    known = {c for c, _ in (_DEFAULT_EPP if epp else _DEFAULT_NONE)}
    grouped = set()
    for name in REGIONS:
        grouped |= set(expandSiteCodes([name], include_deprecated=True))

    phantom = sorted(grouped - known)                       # RS 목록에 없는 코드
    # 레거시(_old) 코드는 그룹에 없는 게 정상이다.
    # 신규 RS와 함께 펼쳐지면 전환 구간이 이중 계상되므로 일부러 넣지 않는다.
    ungrouped = sorted(known - grouped
                       - set(DEPRECATED_SITE_CODES) - set(LEGACY_SITE_CODES))
    pending = sorted(set(ungrouped) & set(UNASSIGNED_SITE_CODES))
    ungrouped = [c for c in ungrouped if c not in pending]
    idx = _groupLookup()
    orphan = sorted(                                        # 지역에 안 속한 법인
        n for n in SUBSIDIARIES
        if not any(n in REGIONS[r] for r in REGIONS))

    issues = {"phantom": phantom, "ungrouped": ungrouped, "orphan_subsidiary": orphan}
    if pending:
        logger.info("법인 미배정 site_code %d개 — 그룹으로는 안 펼쳐지므로 "
                    "rsInput에 직접 적어야 수집됩니다: %s", len(pending), pending)
    if verbose:
        if phantom:
            logger.warning("그룹에 있으나 RS 목록에 없는 코드 %d개: %s",
                           len(phantom), phantom)
        # 통합 RS의 하위 RS는 그룹에 넣지 않는 게 정상이다. 글로벌 통합본과
        # 함께 수집하면 같은 트래픽이 두 번 잡힌다. 어떤 접두사를 하위로 볼지는
        # 프로파일의 sub_total_prefix가 정한다(비어 있으면 이 구분을 하지 않음).
        sub_total = ([c for c in ungrouped if c.startswith(SUB_TOTAL_PREFIX)]
                     if SUB_TOTAL_PREFIX else [])
        rest = [c for c in ungrouped if c not in set(sub_total)]
        if sub_total:
            logger.info("하위 통합 RS %d개는 의도적으로 그룹에 넣지 않았습니다"
                        "(글로벌 통합본과 이중 계상 방지). 필요하면 직접 지정: %s",
                        len(sub_total), sub_total)
        if rest:
            logger.warning("어느 지역에도 속하지 않은 site_code %d개: %s",
                           len(rest), rest)
        if orphan:
            logger.warning("어느 지역에도 속하지 않은 법인 %d개: %s",
                           len(orphan), orphan)
        if not any(issues.values()):
            logger.info("그룹 매핑 정상: site_code %d개 전부 지역에 매핑됨",
                        len(known - set(DEPRECATED_SITE_CODES)))
    return {k: v for k, v in issues.items() if v}


def rsListGenerator(inputSiteCode, targetRSList, strict=False):
    """site_code -> [site_code, rsid] 매핑.

    [수정] 기존에는 매핑에 없는 코드를 아무 신호 없이 버렸다.
    오타 하나로 한 국가가 통째로 빠져도 알 방법이 없었다.
        ['uk','ukk','de'] -> [['uk',...], ['de',...]]   # ukk 흔적 없음
    strict=True면 예외, False면 경고를 남긴다.
    """
    lookup = {}
    for pair in targetRSList:
        lookup.setdefault(pair[0], pair[1])

    final, missing = [], []
    for code in inputSiteCode:
        if code in lookup:
            final.append([code, lookup[code]])
        else:
            missing.append(code)

    if missing:
        hints = []
        for code in missing:
            near = suggestGroup(code)
            if near:
                hints.append(f"{code} -> 혹시 '{near}'?")
        msg = (f"매핑되지 않은 site_code {len(missing)}개: {missing} "
               f"(오타이거나 해당 목록(EPP/일반)에 없는 코드입니다. 수집에서 제외됩니다)")
        if hints:
            msg += " | " + ", ".join(hints)
        if strict:
            raise ValueError(msg)
        logger.warning(msg)

    seen, dups = set(), []
    for code, rsid in final:
        if rsid in seen:
            dups.append((code, rsid))
        seen.add(rsid)
    if dups:
        logger.warning("동일 RSID가 중복 지정되었습니다(중복 적재 위험): %s", dups)

    return final


def returnRsList(epp, inputSiteCode, strict=False, include_deprecated=False,
                 expand=True, include_legacy=False):
    """site_code / 법인명 / 지역명 목록을 [site_code, rsid] 목록으로 바꾼다.

    [확장] inputSiteCode에 프로파일이 정의한 Region / Subsidiary 이름을 넣으면
    해당 site_code 전체로 펼쳐진다. 개별 코드와 섞어 써도 된다.

        returnRsList(False, ["<지역명>"])            # 지역 전체
        returnRsList(False, ["<법인명>"])            # 법인 단위
        returnRsList(False, ["<지역명>", "jp"])      # 혼용

    DEPRECATED_SITE_CODES에 등록된 코드는 자동 제외된다.
    expand=False면 기존처럼 site_code만 받는다.
    """
    target = _DEFAULT_EPP if epp is True else _DEFAULT_NONE
    if not expand:
        return rsListGenerator(inputSiteCode, target, strict=strict)

    pairs = expandSiteCodes(inputSiteCode, include_deprecated=include_deprecated,
                            strict=strict, include_legacy=include_legacy,
                            with_origin=True)

    # 그룹으로 펼쳐진 코드가 해당 목록에 없는 것은 오타가 아니다.
    # (EPP 목록은 보통 일반 목록보다 좁아서, 지역을 펼쳤을 때 EPP RS가 없는
    #  나라가 섞이는 게 정상이다)
    # 직접 적은 코드만 rsListGenerator로 넘겨 오타 경고를 받게 한다.
    known = {c for c, _ in target}
    codes, no_rs = [], []
    for code, origin in pairs:
        if origin != "직접 입력" and code not in known:
            no_rs.append(code)
            continue
        codes.append(code)
    if no_rs:
        logger.info("그룹 확장 중 %s 목록에 RS가 없어 제외된 코드 %d개: %s",
                    "EPP" if epp is True else "일반", len(no_rs), no_rs)

    return rsListGenerator(codes, target, strict=strict)


# ---------------------------------------------------------------------------
# 컬럼 생성
# ---------------------------------------------------------------------------

def tbColumnGenerator(tbColumn, if_site_code, breakdown, epp, site_code_rs):
    if site_code_rs is True:
        breakdown = False
        if_site_code = True

    defaultColumn = ["site_code", "period", "start_date", "end_date", "is_epp"]
    if epp is True:
        defaultColumn.insert(1, "breakdown")
        defaultColumn.insert(6, "is_epp_integ")
    else:
        if breakdown is False:
            if if_site_code is not True:
                defaultColumn.insert(1, "dimension")
        else:
            if if_site_code is True:
                defaultColumn.insert(1, "breakdown")
            else:
                defaultColumn[0] = "dimension"
                defaultColumn.insert(1, "breakdown")

    return defaultColumn + list(tbColumn)


def tbColumnGeneratorRB(tbColumn, if_site_code=False, breakdown=False, epp=True,
                        site_code_rs=False):
    defaultColumn = ["site_code", "RS ID", "Biz_type", "Division", "Category",
                     "Device_type", "Date", "Channel_Raw"]
    return defaultColumn + list(tbColumn)


def tbColumnGeneratorThird(tbColumn, extra="", extra1=""):
    """2단 breakdown용 컬럼. dimension > breakdown > breakdown2."""
    cols = ["site_code", "dimension", "breakdown", "breakdown2",
            "period", "start_date", "end_date", "is_epp"]
    if extra != "":
        cols.append("extra")
    if extra1 != "":
        cols.append("extra1")
    return cols + list(tbColumn)


def tbColumnGeneratorHourly(tbColumn, if_site_code=False, extra="", extra1=""):
    """hourly용 컬럼. 시간 구간은 start_date/end_date에 'YYYY-MM-DD HH:MM'으로 들어간다."""
    cols = ["site_code"]
    if not if_site_code:
        cols.append("dimension")
    cols += ["period", "start_date", "end_date", "is_epp"]
    if extra != "":
        cols.append("extra")
    if extra1 != "":
        cols.append("extra1")
    return cols + list(tbColumn)
