# 완결성 검증 / 누락 진단 / 호출 한도 제어 테스트.
#
# 합성 task와 SQLite로 돈다. 실제 조직 데이터도, Adobe 자격증명도 필요 없다.
#   PYTHONPATH="tests:." python tests/test_validator.py

import sys, os, time, threading
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from sqlalchemy import create_engine, text

from aanalyticsext import (SQLValidator, build_expected, find_missing,
                           plan_refill, missing_report, group_contiguous,
                           daterange, site_key, GapChecker,
                           TokenBucket, suggest_parallel)

DB = "sqlite://"          # in-memory


def make_tasks():
    """실제 실행 계획과 같은 모양의 합성 task."""
    return [
        {"table": "tb_x", "rs": ["rs_a", "rs_b"], "division": "D1",
         "category": "C1", "start": "2026-07-01", "end": "2026-07-03"},
        {"table": "tb_x", "rs": ["rs_c"], "division": "D2",
         "category": "C2", "start": "2026-07-01", "end": "2026-07-02",
         "extra2": "site_c"},          # 통합 RS → 실제 사이트 코드를 따로 들고 다님
        {"table": "tb_y", "rs": ["rs_a"], "division": "D1",
         "category": "C1", "start": "2026-07-02", "end": "2026-07-02"},
    ]


def seed(engine, rows):
    with engine.begin() as c:
        for t in ("tb_x", "tb_y"):
            c.execute(text(f"CREATE TABLE {t} (site_code TEXT, division TEXT, "
                           f"category TEXT, date TEXT, v INT)"))
        for tbl, site, div, cat, d in rows:
            c.execute(text(f"INSERT INTO {tbl} VALUES (:s,:dv,:c,:d,1)"),
                      {"s": site, "dv": div, "c": cat, "d": d})


# --- 1. 날짜 유틸 -----------------------------------------------------
assert daterange("2026-07-01", "2026-07-03") == \
       ["2026-07-01", "2026-07-02", "2026-07-03"]
assert group_contiguous([]) == []
assert group_contiguous(["2026-07-01", "2026-07-02", "2026-07-05"]) == \
       [("2026-07-01", "2026-07-02"), ("2026-07-05", "2026-07-05")]
# 순서가 뒤섞이고 중복이 있어도 같은 결과
assert group_contiguous(["2026-07-05", "2026-07-02", "2026-07-01", "2026-07-02"]) == \
       [("2026-07-01", "2026-07-02"), ("2026-07-05", "2026-07-05")]
print("=== [1] 날짜 유틸: 연속 구간 압축 / 중복·역순 흡수 ===")

# --- 2. 기대 조합은 DB 없이 계획만으로 ---------------------------------
tasks = make_tasks()
exp = build_expected(tasks)
assert set(exp) == {"tb_x", "tb_y"}
# rs 2개 × 3일 + rs 1개 × 2일 = 8
assert len(exp["tb_x"]) == 8, len(exp["tb_x"])
assert len(exp["tb_y"]) == 1
# extra2가 있으면 rs 대신 그것이 사이트 키가 된다
assert ("site_c", "D2", "C2", "2026-07-01") in exp["tb_x"]
assert ("rs_c", "D2", "C2", "2026-07-01") not in exp["tb_x"]
assert site_key({"extra2": "s"}, "rs") == "s" and site_key({}, "rs") == "rs"
print("=== [2] 기대 조합 계산 (DB 접근 0회) ===")
print(f"  task {len(tasks)}개 -> 기대 조합 {sum(len(v) for v in exp.values())}개")

# --- 3. 차집합으로 누락 찾기 -------------------------------------------
eng = create_engine(DB)
rows = [("tb_x", s, "D1", "C1", d) for s in ("rs_a", "rs_b")
        for d in ("2026-07-01", "2026-07-02", "2026-07-03")]
rows += [("tb_x", "site_c", "D2", "C2", "2026-07-01")]   # 07-02 누락
# tb_y 는 통째로 비어 있음
rows.remove(("tb_x", "rs_b", "D1", "C1", "2026-07-02"))  # 구멍 하나 더
seed(eng, rows)

v = SQLValidator(eng)
missing = find_missing(tasks, v, "2026-07-01", "2026-07-03")
flat = {(t, k) for t, m in missing.items() for k in m}
assert ("tb_x", ("rs_b", "D1", "C1", "2026-07-02")) in flat
assert ("tb_x", ("site_c", "D2", "C2", "2026-07-02")) in flat
assert ("tb_y", ("rs_a", "D1", "C1", "2026-07-02")) in flat
assert len(flat) == 3, flat
print("=== [3] 누락 검출 ===")
print(f"  기대 9 / 실제 6 / 누락 {len(flat)} (테이블 {len(missing)}개)")

# --- 4. 누락 -> 최소 개수의 재추출 task --------------------------------
refill = plan_refill(missing)
assert len(refill) == 3, refill
for r in refill:
    assert len(r["rs"]) == 1          # rs 단위로 쪼개짐
    assert r["start"] == r["end"]     # 흩어진 하루짜리 구멍
# 연속 누락은 한 구간으로 합쳐진다
tasks2 = [{"table": "tb_z", "rs": ["rs_a"], "division": "D1", "category": "C1",
           "start": "2026-07-01", "end": "2026-07-05"}]
miss2 = {"tb_z": {("rs_a", "D1", "C1", d): (tasks2[0], "rs_a")
                  for d in ("2026-07-02", "2026-07-03", "2026-07-04")}}
r2 = plan_refill(miss2)
assert len(r2) == 1 and r2[0]["start"] == "2026-07-02" and r2[0]["end"] == "2026-07-04"
print("=== [4] 재추출 계획 ===")
print(f"  흩어진 누락 3건 -> task 3개 / 연속 누락 3일 -> task 1개")

# --- 5. 차원을 바꿔도 동작 ---------------------------------------------
tasks3 = [{"table": "tb_d", "rs": ["rs_a"], "brand": "B1",
           "start": "2026-07-01", "end": "2026-07-01"}]
exp3 = build_expected(tasks3, dimensions=("brand",))
assert list(exp3["tb_d"]) == [("rs_a", "B1", "2026-07-01")], exp3
v3 = SQLValidator(eng, colmap={"brand": "brand"}, dimensions=("brand",))
assert v3.dimensions == ("brand",)
print("=== [5] 키 차원 교체 (division/category 고정 아님) ===")

# --- 6. GapChecker: planner는 덕 타이핑 --------------------------------
class FakePlanner:
    cfg = {"rs_vrs": ["rs_b"]}
    def __init__(self, tasks): self._t = tasks
    def iter_tasks(self, start, end, prefix, **f): return iter(self._t)
    def dry_run(self, *a, **k): return "DRY"
    def run(self, *a, **k): return "RAN"

gc = GapChecker(FakePlanner(tasks), eng)
df = gc.scan("2026-07-01", "2026-07-03", "tb_")
assert len(df) == 3, df
assert set(gc.sites()) == {"rs_b", "site_c", "rs_a"}
assert gc.sites(vrs_only=True) == ["rs_b"]        # cfg의 rs_vrs가 반영됨
assert not gc.by_site().empty and not gc.by_date().empty and not gc.by_vrs().empty
assert gc.backfill(dry_run=True) == "DRY"
assert gc.backfill() == "RAN"
# cfg가 없는 planner여도 뜬다
class Bare(FakePlanner):
    cfg = {}
assert GapChecker(Bare(tasks), eng).vrs == set()
print("=== [6] GapChecker (planner 덕 타이핑, 설정 파일 안 읽음) ===")
print(f"  누락 {len(df)}건 / VRS {int(df['vrs'].sum())}건")

# --- 7. 리포트 저장 -----------------------------------------------------
import tempfile, pathlib, json
with tempfile.TemporaryDirectory() as td:
    p = missing_report(missing, os.path.join(td, "miss"))
    assert p and p.exists()
    data = json.loads(p.read_text(encoding="utf-8"))
    assert set(data) == {"tb_x", "tb_y"}
assert missing_report({}) is None
print("=== [7] 끝까지 비는 조합 리포트 ===")

# --- 8. 토큰버킷 --------------------------------------------------------
tb = TokenBucket(per_minute=600, window=1.0)   # 1초에 600건
t0 = time.time()
for _ in range(10):
    tb.acquire()
assert time.time() - t0 < 0.5
st = tb.stats()
assert st["누적_허용"] == 10, st

tb2 = TokenBucket(per_minute=2, window=0.4)    # 0.4초에 2건
t0 = time.time()
for _ in range(4):
    tb2.acquire()
waited = time.time() - t0
assert waited >= 0.35, waited                  # 넘치는 만큼 로컬에서 대기
print("=== [8] 토큰버킷 ===")
print(f"  한도 내 10건 즉시 / 한도 초과 4건 {waited:.2f}s 대기")

# 스레드 안전: 동시에 쳐도 한도를 넘기지 않는다
tb3 = TokenBucket(per_minute=20, window=1.0)
def burn():
    for _ in range(5): tb3.acquire()
ths = [threading.Thread(target=burn) for _ in range(4)]
t0 = time.time()
[t.start() for t in ths]; [t.join() for t in ths]
assert tb3.stats()["누적_허용"] == 20, tb3.stats()
print(f"  4스레드 × 5건 = 20건, 한도 위반 없음 ({time.time()-t0:.2f}s)")

assert suggest_parallel(4) >= 1
assert suggest_parallel(60) > suggest_parallel(4)   # 느릴수록 병렬을 높여야 포화
print(f"  suggest_parallel: 응답 4s -> {suggest_parallel(4)}, 60s -> {suggest_parallel(60)}")

print("\n✅ 완결성 검증 / 누락 진단 / 한도 제어 통과")
