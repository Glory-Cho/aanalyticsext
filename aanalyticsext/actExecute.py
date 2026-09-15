# 수집 작업 실행 — 기간을 나눠 돌리고, 실패를 재시도하고, 진행을 보고한다.
#
# 공개 API(retrieve_* 함수들)가 모두 여기 있다.
# 요청/적재는 actModuler, 수집 단위 계산은 actRunner가 맡는다.
#
# limit function and para order / JsonToDB To refinedFrame1
# 20251212 retrieve_RB_parallel, retrieve_by_RS_parallel 추가
# 260727 아래 항목 수정 및 추가
#   - "Error occurred." 대신 실제 예외 타입/메시지 출력
#   - 영구 오류(스키마·설정)를 5회 반복하지 않고 즉시 중단
#   - 재시도에 지수 백오프 적용 (429 스로틀링 악화 방지)
#   - key_columns로 멱등 적재 선택 가능 (재실행 시 중복 방지)
#   - 커넥션 풀 크기를 max_workers에 맞춤
#   - retrieve_Hourly (신규)
#   - retrieve_ThirdLevel / retrieve_by_RS_ThirdLevel (신규, 2단 breakdown)
#
# 기존 함수 시그니처는 그대로이며 추가 인자는 모두 기본값이 있는 키워드 인자다.
# 기존 호출 코드는 수정 없이 동작한다.

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from . import actModuler as M
    from .actModuler import *
    from .actModuler import (_runTask, logger, _Progress, _runItems,
                             BreakdownCheckpoint, _jobKey)
    from .actRunner import *
except ImportError:
    import actModuler as M
    from actModuler import *
    from actModuler import (_runTask, logger, _Progress, _runItems,
                            BreakdownCheckpoint, _jobKey)
    from actRunner import *

# 프로파일이 주입하는 값은 모듈 속성으로 읽는다. `from ... import *` 로 복사해
# 두면 나중에 주입된 값을 못 본다.


def _forEachWindow(start_date, end_date, period, fn, label_fmt, max_retries,
                   clamp=True, progress_every=1, progress_mode="auto", label=""):
    """기간을 분할해 순차 실행. 한 구간이 실패해도 다음 구간으로 넘어간다."""
    startDate, endDate = dateGenerator(start_date, end_date, period, clamp=clamp)
    pr = _Progress(len(startDate), label or "구간", progress_every, unit="건",
                   mode=progress_mode)
    done = 0
    for sd, ed in zip(startDate, endDate):
        n = _runTask(fn, label_fmt.format(sd=sd, ed=ed), max_retries, sd, ed)
        if n is not None:
            done += 1
            pr.step(str(sd), "ok", rows=n if isinstance(n, int) else None)
        else:
            pr.step(str(sd), "fail")
    pr.finish()
    return done


# ---------------------------------------------------------------------------
# 1단
# ---------------------------------------------------------------------------

def retrieve_FirstLevel(start_date, end_date, period, jsonLocation, tbColumn,
                        dbTableName, epp, site_code_rs, limit=0, extra="", extra1="",
                        start_hour="00:00", end_hour="00:00", site_code="",
                        max_retries=5, key_columns=None, end_mode="legacy",
                        on_empty="skip", clamp=True, progress_every=1,
                        progress_mode="auto"):
    if_site_code = checkSiteCode(readJson(jsonLocation)["dimension"])
    cols = tbColumnGenerator(tbColumn, if_site_code, False, False, site_code_rs)

    def run(sd, ed):
        return refinedFrame1(sd, ed, period, jsonLocation, cols, dbTableName, epp,
                             if_site_code, site_code_rs, limit, extra, extra1,
                             start_hour, end_hour, site_code,
                             key_columns=key_columns, end_mode=end_mode,
                             on_empty=on_empty)

    return _forEachWindow(start_date, end_date, period, run,
                          "FirstLevel {sd}~{ed}", max_retries, clamp,
                          progress_every, progress_mode, "FirstLevel")


# ---------------------------------------------------------------------------
# 2단 (1st breakdown)
# ---------------------------------------------------------------------------

def retrieve_SecondLevel(start_date, end_date, period, jsonLocation,
                         jsonLocation_breakdown, tbColumn, dbTableName, epp,
                         limit1=0, limit2=0, extra="", extra1="",
                         start_hour="00:00", end_hour="00:00", site_code="",
                         max_retries=1, key_columns=None, end_mode="legacy",
                         on_empty="skip", clamp=True, checkpoint_dir=None,
                         resume=True, item_retries=3, retry_failed=False,
                         progress_every=1, progress_mode="auto"):
    """[수정] max_retries 기본값을 1로 낮췄다.

    항목 단위 재시도(item_retries)가 내부에서 이미 처리하므로,
    바깥에서 작업 전체를 여러 번 되감을 이유가 없다.
    기존 기본값 5를 그대로 쓰면 항목 재시도와 곱해져 호출이 폭증한다.
    """
    if_site_code = checkSiteCode(readJson(jsonLocation)["dimension"])
    # 리포트가 site_code를 차원으로 돌려주는 RS는 응답에서 뽑아 써야 한다.
    if returnRsID(jsonLocation) in M.SITE_CODE_DIMENSION_RSIDS:
        if_site_code = True
    cols = tbColumnGenerator(tbColumn, if_site_code, True, False, False)

    def run(sd, ed):
        return StackbreakValue(sd, ed, period, jsonLocation, jsonLocation_breakdown,
                               cols, dbTableName, epp, limit1, limit2, extra, extra1,
                               start_hour, end_hour, site_code,
                               key_columns=key_columns, end_mode=end_mode,
                               on_empty=on_empty, checkpoint_dir=checkpoint_dir,
                               resume=resume, item_retries=item_retries,
                               retry_failed=retry_failed,
                               progress_every=progress_every,
                               progress_mode=progress_mode)

    return _forEachWindow(start_date, end_date, period, run,
                          "SecondLevel {sd}~{ed}", max_retries, clamp,
                          progress_every, progress_mode, "SecondLevel")


def retrieve_SecondLevelTotal(start_date, end_date, period, jsonLocation,
                              jsonLocation_breakdown, tbColumn, dbTableName, epp,
                              limit1=0, limit2=0, extra="", extra1="",
                              start_hour="00:00", end_hour="00:00", site_code="",
                              max_retries=1, key_columns=None, end_mode="legacy",
                              on_empty="skip", clamp=True, checkpoint_dir=None,
                              resume=True, item_retries=3, retry_failed=False,
                              progress_every=1, progress_mode="auto"):
    """[수정] Total 적재와 breakdown 을 분리해 각각 재시도한다.
    (retrieve_by_RS_breakdownTotal 과 같은 중복 적재 문제)"""
    if_site_code = checkSiteCode(readJson(jsonLocation)["dimension"])
    # 리포트가 site_code를 차원으로 돌려주는 RS는 응답에서 뽑아 써야 한다.
    if returnRsID(jsonLocation) in M.SITE_CODE_DIMENSION_RSIDS:
        if_site_code = True
    cols = tbColumnGenerator(tbColumn, if_site_code, True, False, False)

    sds, eds = dateGenerator(start_date, end_date, period, clamp=clamp)
    done = 0
    for sd, ed in zip(sds, eds):
        _runTask(refinedFrameTotal, f"SecondLevelTotal {sd} total", max_retries,
                 sd, ed, period, jsonLocation, cols, dbTableName, epp,
                 if_site_code, False, limit1, extra, extra1,
                 start_hour, end_hour, site_code,
                 key_columns=key_columns, end_mode=end_mode)
        if _runTask(StackbreakValue, f"SecondLevelTotal {sd} bd", max_retries,
                    sd, ed, period, jsonLocation, jsonLocation_breakdown,
                    cols, dbTableName, epp, limit1, limit2, extra, extra1,
                    start_hour, end_hour, site_code,
                    key_columns=key_columns, end_mode=end_mode, on_empty=on_empty,
                    checkpoint_dir=checkpoint_dir, resume=resume,
                    item_retries=item_retries, retry_failed=retry_failed,
                    progress_every=progress_every,
                    progress_mode=progress_mode) is not None:
            done += 1
    logger.info("%d/%d 구간 완료", done, len(sds))
    return done


# ---------------------------------------------------------------------------
# RS 단위
# ---------------------------------------------------------------------------

def retrieve_by_RS(start_date, end_date, period, jsonLocation, rsInput, tbColumn,
                   dbTableName, epp, limit=0, extra="", extra1="",
                   start_hour="00:00", end_hour="00:00", max_retries=5,
                   key_columns=None, end_mode="legacy", strict_rs=False, clamp=True,
                   on_empty="skip", progress_every=1, progress_mode="auto"):
    cols = tbColumnGenerator(tbColumn, False, False, True, False)
    sds, eds = dateGenerator(start_date, end_date, period, clamp=clamp)
    rsList = returnRsList(epp, rsInput, strict=strict_rs)

    tasks = [(rs, sd, ed) for rs in rsList for sd, ed in zip(sds, eds)]
    pr = _Progress(len(tasks), "by_RS", progress_every, unit="행",
                   mode=progress_mode)
    for rs, sd, ed in tasks:
        n = _runTask(refineRsIDChange, f"{rs[0]} {sd}", max_retries,
                     sd, ed, jsonLocation, rs, period, cols, dbTableName, epp,
                     limit, extra, extra1, start_hour, end_hour,
                     key_columns=key_columns, end_mode=end_mode, on_empty=on_empty)
        pr.step(f"{rs[0]} {sd}", "ok" if n is not None else "fail",
                rows=n if isinstance(n, int) else None)
    pr.finish()
    return pr.ok


def retrieve_by_RS_breakdown(startDate, endDate, period, jsonFile, jsonFilebreakdown,
                             rsInput, tbColumn, dbTableName, epp, limit1=0, limit2=0,
                             extra="", extra1="", start_hour="00:00", end_hour="00:00",
                             max_retries=1, key_columns=None, end_mode="legacy",
                             on_empty="skip", strict_rs=False, clamp=True,
                             checkpoint_dir=None, resume=True, item_retries=3,
                             retry_failed=False, progress_every=1,
                             progress_mode="auto"):
    """[수정] 항목 단위 재시도 + 체크포인트 지원.

    checkpoint_dir을 주면 중단 지점부터 이어서 실행한다. 재실행 시 이미
    완료된 항목은 API를 다시 호출하지 않는다.
    """
    defaultColumn = ["site_code", "dimension", "breakdown", "period",
                     "start_date", "end_date", "is_epp"]
    if extra != "":
        defaultColumn.append("extra")
    if extra1 != "":
        defaultColumn.append("extra1")
    cols = defaultColumn + list(tbColumn)

    sds, eds = dateGenerator(startDate, endDate, period, clamp=clamp)
    rsList = returnRsList(epp, rsInput, strict=strict_rs)

    for rs in rsList:
        for sd, ed in zip(sds, eds):
            _runTask(secondCaller, f"{rs[0]} {sd} bd", max_retries,
                     sd, ed, jsonFile, jsonFilebreakdown, rs, period, cols,
                     dbTableName, epp, limit1, limit2, extra, extra1,
                     start_hour, end_hour,
                     key_columns=key_columns, end_mode=end_mode, on_empty=on_empty,
                     checkpoint_dir=checkpoint_dir, resume=resume,
                     item_retries=item_retries, retry_failed=retry_failed,
                     progress_every=progress_every, progress_mode=progress_mode)


def retrieve_by_RS_breakdownTotal(startDate, endDate, period, jsonFile,
                                  jsonFilebreakdown, rsInput, tbColumn, dbTableName,
                                  epp, limit1=0, limit2=0, extra="", extra1="",
                                  start_hour="00:00", end_hour="00:00", max_retries=1,
                                  key_columns=None, end_mode="legacy",
                                  on_empty="skip", strict_rs=False, clamp=True,
                                  checkpoint_dir=None, resume=True, item_retries=3,
                                  retry_failed=False, progress_every=1,
                                  progress_mode="auto"):
    """[수정] Total 적재와 breakdown 을 분리해 각각 재시도한다.

    기존에는 둘을 한 클로저로 묶어 _runTask 로 감쌌다. breakdown 이 실패하면
    이미 끝난 Total 까지 함께 되감겨 같은 구간의 Total 행이 재시도 횟수만큼
    중복 적재됐다 (max_retries=5 -> Total 6행).
    항목 단위 재시도·체크포인트도 secondCaller 로 전달되지 않고 있었다.
    """
    defaultColumn = ["site_code", "dimension", "breakdown", "period",
                     "start_date", "end_date", "is_epp"]
    if extra != "":
        defaultColumn.append("extra")
    if extra1 != "":
        defaultColumn.append("extra1")
    cols = defaultColumn + list(tbColumn)

    sds, eds = dateGenerator(startDate, endDate, period, clamp=clamp)
    rsList = returnRsList(epp, rsInput, strict=strict_rs)

    for rs in rsList:
        for sd, ed in zip(sds, eds):
            # Total 과 breakdown 을 각각 재시도한다.
            # 한쪽 실패가 다른 쪽 재적재로 이어지지 않게 하기 위함이다.
            _runTask(refineRsIDChangeTotal, f"{rs[0]} {sd} total", max_retries,
                     sd, ed, jsonFile, rs, period, cols, dbTableName,
                     epp, limit1, extra, extra1, start_hour, end_hour,
                     key_columns=key_columns, end_mode=end_mode)
            _runTask(secondCaller, f"{rs[0]} {sd} bd", max_retries,
                     sd, ed, jsonFile, jsonFilebreakdown, rs, period, cols,
                     dbTableName, epp, limit1, limit2, extra, extra1,
                     start_hour, end_hour, key_columns=key_columns,
                     end_mode=end_mode, on_empty=on_empty,
                     checkpoint_dir=checkpoint_dir, resume=resume,
                     item_retries=item_retries, retry_failed=retry_failed,
                     progress_every=progress_every, progress_mode=progress_mode)


# ---------------------------------------------------------------------------
# RB
# ---------------------------------------------------------------------------

def retrieve_RB(start_date, end_date, jsonLocation, rsInput, tbColumn, dbTableName,
                epp, limit=0, Biz_type="", Device_type="", Division="", Category="",
                max_retries=10, key_columns=None, site_code_ae="", strict_rs=False,
                clamp=True, progress_every=1, progress_mode="auto",
                on_empty="skip", end_mode="legacy"):
    period = "daily"
    cols = tbColumnGeneratorRB(tbColumn)
    sds, eds = dateGenerator(start_date, end_date, period, clamp=clamp)
    rsList = returnRsList(epp, rsInput, strict=strict_rs)

    tasks = [(rs, sd, ed) for rs in rsList for sd, ed in zip(sds, eds)]
    pr = _Progress(len(tasks), "RB", progress_every, unit="행", mode=progress_mode)
    for rs, sd, ed in tasks:
        n = _runTask(refineRsIDChangeRB, f"{rs[0]} {sd} RB", max_retries,
                     sd, ed, jsonLocation, rs, period, cols, dbTableName, epp, limit,
                     Biz_type, Device_type, Division, Category, site_code_ae,
                     "00:00", "00:00", key_columns=key_columns,
                     end_mode=end_mode, on_empty=on_empty)
        pr.step(f"{rs[0]} {sd}", "ok" if n is not None else "fail",
                rows=n if isinstance(n, int) else None)
    pr.finish()
    return pr.ok


def retrieve_RB_AE(start_date, end_date, jsonLocation, rsInput, tbColumn, dbTableName,
                   epp, limit=0, Biz_type="", Device_type="", Division="", Category="",
                   site_code_ae="", max_retries=10, key_columns=None, strict_rs=False,
                   clamp=True, progress_every=1, progress_mode="auto",
                   on_empty="skip", end_mode="legacy"):
    """retrieve_RB 와 동일하며 site_code 를 site_code_ae 로 덮어쓴다."""
    return retrieve_RB(start_date, end_date, jsonLocation, rsInput, tbColumn,
                       dbTableName, epp, limit=limit, Biz_type=Biz_type,
                       Device_type=Device_type, Division=Division,
                       Category=Category, max_retries=max_retries,
                       key_columns=key_columns, site_code_ae=site_code_ae,
                       strict_rs=strict_rs, clamp=clamp,
                       progress_every=progress_every, progress_mode=progress_mode,
                       on_empty=on_empty, end_mode=end_mode)


# ---------------------------------------------------------------------------
# 병렬
# ---------------------------------------------------------------------------

def _runParallel(worker, tasks, max_workers, progress_every=1, label="병렬"):
    """작업을 병렬 실행한다. 완료 순서대로 진행 상황을 집계한다."""
    logger.info("총 작업 수: %d개 (워커 %d)", len(tasks), max_workers)
    # 워커가 여러 개면 같은 줄 덮어쓰기가 뒤엉키므로 로그 방식으로 집계한다
    pr = _Progress(len(tasks), label, progress_every, unit="건", mode="log")
    ok = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(worker, t) for t in tasks]
        for f in as_completed(futures):
            try:
                f.result()
                ok += 1
                pr.step("작업", "ok")
            except Exception as e:
                pr.log(logging.ERROR, "처리되지 않은 오류: %s: %s",
                       type(e).__name__, e)
                pr.step("작업", "fail")
    pr.finish()
    return ok


def retrieve_by_RS_parallel(start_date, end_date, period, jsonLocation, rsInput,
                            tbColumn, dbTableName, epp, limit=0, extra="", extra1="",
                            start_hour="00:00", end_hour="00:00", max_retries=5,
                            max_workers=10, key_columns=None, strict_rs=False,
                            clamp=True, end_mode="legacy", on_empty="skip",
                            progress_every=1):
    """[수정] 직렬 버전(retrieve_by_RS)에만 있던 end_mode / on_empty 를 맞췄다.

    같은 작업인데 병렬로 바꾸면 시각 처리와 빈 데이터 처리가 달라지는 것은
    발견하기 어려운 함정이다.
    """
    cols = tbColumnGenerator(tbColumn, False, False, True, False)
    sds, eds = dateGenerator(start_date, end_date, period, clamp=clamp)
    rsList = returnRsList(epp, rsInput, strict=strict_rs)

    # 커넥션 풀을 워커 수에 맞춘다
    # (기존: 풀 상한 7 vs 워커 10 -> 3개는 항상 대기하다 QueuePool 타임아웃)
    get_db_engine(pool_size=max(max_workers, 5))

    tasks = [(sd, ed, jsonLocation, rs, period, cols, dbTableName, epp, limit,
              extra, extra1, start_hour, end_hour, max_retries, key_columns,
              end_mode, on_empty)
             for rs in rsList for sd, ed in zip(sds, eds)]
    return _runParallel(worker_refine_RS, tasks, max_workers, progress_every,
                        label="by_RS 병렬")


def retrieve_RB_parallel(start_date, end_date, jsonLocation, rsInput, tbColumn,
                         dbTableName, epp, limit=0, Biz_type="", Device_type="",
                         Division="", Category="", max_retries=5, max_workers=10,
                         key_columns=None, site_code_ae="", strict_rs=False,
                         clamp=True, progress_every=1, on_empty="skip",
                         end_mode="legacy"):
    period = "daily"
    cols = tbColumnGeneratorRB(tbColumn)
    sds, eds = dateGenerator(start_date, end_date, period, clamp=clamp)
    rsList = returnRsList(epp, rsInput, strict=strict_rs)
    get_db_engine(pool_size=max(max_workers, 5))

    tasks = [(sd, ed, jsonLocation, rs, period, cols, dbTableName, epp, limit,
              Biz_type, Device_type, Division, Category, site_code_ae,
              "00:00", "00:00", max_retries, key_columns, end_mode, on_empty)
             for rs in rsList for sd, ed in zip(sds, eds)]
    return _runParallel(worker_refine_RB, tasks, max_workers, progress_every,
                        label="RB 병렬")


def _breakdownParallel(startDate, endDate, period, jsonFile, jsonFilebreakdown,
                       rsInput, tbColumn, dbTableName, epp, with_total,
                       limit1, limit2, extra, extra1, start_hour, end_hour,
                       max_retries, max_workers, key_columns, end_mode, on_empty,
                       strict_rs, clamp, checkpoint_dir, resume, item_retries,
                       retry_failed, progress_every, progress_mode):
    defaultColumn = ["site_code", "dimension", "breakdown", "period",
                     "start_date", "end_date", "is_epp"]
    if extra != "":
        defaultColumn.append("extra")
    if extra1 != "":
        defaultColumn.append("extra1")
    cols = defaultColumn + list(tbColumn)

    sds, eds = dateGenerator(startDate, endDate, period, clamp=clamp)
    rsList = returnRsList(epp, rsInput, strict=strict_rs)
    get_db_engine(pool_size=max(max_workers, 5))

    if progress_mode == "auto" and max_workers > 1:
        # 워커 여러 개가 같은 줄을 덮어쓰면 진행 표시가 뒤엉킨다
        progress_mode = "log"

    tasks = [(sd, ed, jsonFile, jsonFilebreakdown, rs, period, cols, dbTableName,
              epp, limit1, limit2, extra, extra1, start_hour, end_hour,
              max_retries, key_columns, end_mode, on_empty, checkpoint_dir,
              resume, item_retries, retry_failed, progress_every, progress_mode,
              with_total)
             for rs in rsList for sd, ed in zip(sds, eds)]
    return _runParallel(worker_refine_breakdown, tasks, max_workers)


def retrieve_by_RS_breakdown_parallel(startDate, endDate, period, jsonFile,
                                      jsonFilebreakdown, rsInput, tbColumn,
                                      dbTableName, epp, limit1=0, limit2=0,
                                      extra="", extra1="", start_hour="00:00",
                                      end_hour="00:00", max_retries=1,
                                      max_workers=10, key_columns=None,
                                      end_mode="legacy", on_empty="skip",
                                      strict_rs=False, clamp=True,
                                      checkpoint_dir=None, resume=True,
                                      item_retries=3, retry_failed=False,
                                      progress_every=1, progress_mode="auto"):
    """retrieve_by_RS_breakdown 의 병렬 버전. RS x 기간 단위로 분배한다."""
    return _breakdownParallel(startDate, endDate, period, jsonFile,
                              jsonFilebreakdown, rsInput, tbColumn, dbTableName,
                              epp, False, limit1, limit2, extra, extra1,
                              start_hour, end_hour, max_retries, max_workers,
                              key_columns, end_mode, on_empty, strict_rs, clamp,
                              checkpoint_dir, resume, item_retries, retry_failed,
                              progress_every, progress_mode)


def retrieve_by_RS_breakdownTotal_parallel(startDate, endDate, period, jsonFile,
                                           jsonFilebreakdown, rsInput, tbColumn,
                                           dbTableName, epp, limit1=0, limit2=0,
                                           extra="", extra1="", start_hour="00:00",
                                           end_hour="00:00", max_retries=1,
                                           max_workers=10, key_columns=None,
                                           end_mode="legacy", on_empty="skip",
                                           strict_rs=False, clamp=True,
                                           checkpoint_dir=None, resume=True,
                                           item_retries=3, retry_failed=False,
                                           progress_every=1, progress_mode="auto"):
    """retrieve_by_RS_breakdownTotal 의 병렬 버전.

    Total 과 breakdown 은 워커 안에서 각각 실행되므로,
    breakdown 실패가 Total 재적재로 이어지지 않는다.
    """
    return _breakdownParallel(startDate, endDate, period, jsonFile,
                              jsonFilebreakdown, rsInput, tbColumn, dbTableName,
                              epp, True, limit1, limit2, extra, extra1,
                              start_hour, end_hour, max_retries, max_workers,
                              key_columns, end_mode, on_empty, strict_rs, clamp,
                              checkpoint_dir, resume, item_retries, retry_failed,
                              progress_every, progress_mode)


def retrieve_RB_AE_parallel(start_date, end_date, jsonLocation, rsInput, tbColumn,
                            dbTableName, epp, limit=0, Biz_type="", Device_type="",
                            Division="", Category="", site_code_ae="",
                            max_retries=5, max_workers=10, key_columns=None,
                            strict_rs=False, clamp=True, progress_every=1,
                            on_empty="skip", end_mode="legacy"):
    return retrieve_RB_parallel(start_date, end_date, jsonLocation, rsInput, tbColumn,
                                dbTableName, epp, limit=limit, Biz_type=Biz_type,
                                Device_type=Device_type, Division=Division,
                                Category=Category, max_retries=max_retries,
                                max_workers=max_workers, key_columns=key_columns,
                                site_code_ae=site_code_ae, strict_rs=strict_rs,
                                clamp=clamp, progress_every=progress_every,
                                on_empty=on_empty, end_mode=end_mode)


# ---------------------------------------------------------------------------
# 신규: hourly
# ---------------------------------------------------------------------------

def retrieve_Hourly(start_date, end_date, jsonLocation, tbColumn, dbTableName, epp,
                    start_hour="00:00", end_hour="00:00", rsInput=None, limit=0,
                    extra="", extra1="", max_retries=5, key_columns=None,
                    end_mode="legacy", on_empty="fill", fill_value=0,
                    dimension_fill="(none)", strict_rs=False, dry_run=False,
                    progress_every=1, progress_mode="auto"):
    """시간 단위 수집.

    요청한 시간대 수만큼 정확히 수집한다. 데이터가 없거나 호출이 실패한
    시간대는 지표 0으로 채워 시계열에 구멍이 생기지 않게 한다.

        start_hour="09:00", end_hour="18:00"  ->  09시~18시  10개 구간
        start_hour="00:00", end_hour="00:00"  ->  00시~23시  24개 구간
        start_hour="22:00", end_hour="02:00"  ->  자정을 넘겨 5개 구간

    period 컬럼에 "hourly"가 들어가고, start_date/end_date에는
    'YYYY-MM-DD HH:MM' 형식의 구간 경계가 기록된다.
    기존 일별 테이블과 섞지 말고 별도 테이블 사용을 권한다
    (기존 쿼리가 시간별 행까지 끌어가 이중 계상될 수 있다).
    """
    assertMetricCount(jsonLocation, tbColumn)     # 설정 오류는 첫 호출 전에 차단
    if_site_code = checkSiteCode(readJson(jsonLocation)["dimension"])
    rsList = returnRsList(epp, rsInput, strict=strict_rs) if rsInput else [None]

    frames = []
    for rs in rsList:
        label = f"hourly {rs[0] if rs else returnRsID(jsonLocation)} {start_date}"
        df = _runTask(hourlyFrame, label, max_retries,
                      start_date, end_date, jsonLocation, rs, tbColumn, epp,
                      start_hour=start_hour, end_hour=end_hour, limit=limit,
                      extra=extra, extra1=extra1, end_mode=end_mode,
                      if_site_code=if_site_code, on_empty=on_empty,
                      fill_value=fill_value, dimension_fill=dimension_fill)
        if df is None or df.empty:
            continue
        frames.append(df)
        if not dry_run:
            stackTodb(df, dbTableName, key_columns)

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# 신규: 3단 (2nd breakdown)
# ---------------------------------------------------------------------------

def retrieve_ThirdLevel(start_date, end_date, period, jsonLocation,
                        breakdown1, breakdown2, tbColumn, dbTableName, epp,
                        limit0=0, limit1=0, limit2=0, extra="", extra1="",
                        start_hour="00:00", end_hour="00:00", max_retries=1,
                        key_columns=None, end_mode="legacy", on_empty="fill",
                        fill_value=0, dimension_fill="(none)", clamp=True,
                        dry_run=False, progress_every=1, progress_mode="auto",
                        checkpoint_dir=None, resume=True, item_retries=3,
                        retry_failed=False):
    """2단 breakdown: dimension > breakdown1 > breakdown2.

    breakdown1/2에 dimension 이름("variables/evar3")을 주면 breakdown JSON을
    자동 생성한다. ".json" 경로를 주면 기존처럼 템플릿 파일을 사용한다.

    호출 수가 상위 레벨 행 수의 곱으로 늘어나므로 limit0/limit1 지정을 권한다.
    """
    return _thirdLevel(start_date, end_date, period, jsonLocation, breakdown1,
                       breakdown2, None, tbColumn, dbTableName, epp,
                       limit0, limit1, limit2, extra, extra1, start_hour, end_hour,
                       max_retries, key_columns, end_mode, on_empty, fill_value,
                       dimension_fill, clamp, dry_run, progress_every, progress_mode,
                       checkpoint_dir, resume, item_retries, retry_failed)


def retrieve_by_RS_ThirdLevel(start_date, end_date, period, jsonLocation,
                              breakdown1, breakdown2, rsInput, tbColumn, dbTableName,
                              epp, limit0=0, limit1=0, limit2=0, extra="", extra1="",
                              start_hour="00:00", end_hour="00:00", max_retries=1,
                              key_columns=None, end_mode="legacy", on_empty="fill",
                              fill_value=0, dimension_fill="(none)",
                              strict_rs=False, clamp=True, dry_run=False,
                              progress_every=1, progress_mode="auto",
                              checkpoint_dir=None, resume=True, item_retries=3,
                              retry_failed=False):
    rsList = returnRsList(epp, rsInput, strict=strict_rs)
    return _thirdLevel(start_date, end_date, period, jsonLocation, breakdown1,
                       breakdown2, rsList, tbColumn, dbTableName, epp,
                       limit0, limit1, limit2, extra, extra1, start_hour, end_hour,
                       max_retries, key_columns, end_mode, on_empty, fill_value,
                       dimension_fill, clamp, dry_run, progress_every)


def _thirdLevel(start_date, end_date, period, jsonLocation, breakdown1, breakdown2,
                rsList, tbColumn, dbTableName, epp, limit0, limit1, limit2,
                extra, extra1, start_hour, end_hour, max_retries, key_columns,
                end_mode, on_empty, fill_value, dimension_fill, clamp, dry_run,
                progress_every=1, progress_mode="auto", checkpoint_dir=None,
                resume=True, item_retries=3, retry_failed=False):
    """[수정] (RS, 기간) 단위 체크포인트를 붙였다.

    3단 추출은 호출 수가 L0 x L1 로 곱해져 가장 비싸다. 기존에는 체크포인트가
    없어 중간에 끊기면 처음부터 다시 돌아야 했고, max_retries=5 라
    실패 시 전체를 다섯 번 되감았다.
    """
    assertMetricCount(jsonLocation, tbColumn)     # 설정 오류는 첫 호출 전에 차단
    sds, eds = dateGenerator(start_date, end_date, period, clamp=clamp)
    targets = rsList if rsList else [None]

    # (RS, 기간) 조합을 '항목'으로 보고 _runItems 에 태운다.
    # 이미 끝난 조합은 다시 호출하지 않는다.
    units = [(rs, sd, ed) for rs in targets for sd, ed in zip(sds, eds)]
    items = [(f"{rs[0] if rs else 'default'} {sd}", f"{rs[1] if rs else 'default'}|{sd}")
             for rs, sd, ed in units]
    lookup = {k: u for (_, k), u in zip(items, units)}

    window0 = makeWindow(start_date, end_date, period, start_hour, end_hour, end_mode)
    cp = BreakdownCheckpoint(
        _jobKey(returnRsID(jsonLocation), str(breakdown2), window0, dbTableName, "3rd"),
        checkpoint_dir)

    frames = []

    def work(label, key):
        rs, sd, ed = lookup[key]
        df = thirdLevelFrame(sd, ed, period, jsonLocation, breakdown1, breakdown2,
                             rs, tbColumn, epp, limit0=limit0, limit1=limit1,
                             limit2=limit2, extra=extra, extra1=extra1,
                             start_hour=start_hour, end_hour=end_hour,
                             end_mode=end_mode, on_empty=on_empty,
                             fill_value=fill_value, dimension_fill=dimension_fill,
                             item_retries=item_retries,
                             progress_every=progress_every,
                             progress_mode=progress_mode)
        if df is None or df.empty:
            return 0
        frames.append(df)
        return 0 if dry_run else stackTodb(df, dbTableName, key_columns)

    # 라벨을 구분한다. 안쪽 thirdLevelFrame 도 자체 진행 표시를 내므로
    # 같은 이름이면 어느 층의 로그인지 알 수 없다.
    _runItems(items, work, cp, item_retries, resume, retry_failed,
              f"3rd 전체({len(items)}건)", progress_every=progress_every,
              progress_mode=progress_mode)

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)
