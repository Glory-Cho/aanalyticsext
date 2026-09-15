"""
추출 완결성 검증 + 누락 조합 재추출 계획
=========================================
"다 받았는가"를 확인하려고 받은 것을 다시 내려받을 필요는 없다.

  1. 기대 조합은 실행 계획(task 목록)에서 계산한다 — DB 접근 0회
  2. 실제 조합은 테이블당 SELECT DISTINCT "한 번"으로 키만 받는다 (수 KB)
  3. 집합 차집합 = 누락. 누락 날짜를 연속 구간으로 묶어 최소 개수의 task로 만든다

조합이 수백만 개여도 내려받는 건 키 목록뿐이라, 비용이 조합 수가 아니라
테이블 수에 비례한다. 조합 폭발은 DB 집계 엔진이 감당한다.

주의: 누락이 곧 오류는 아니다. 해당 국가에서 취급하지 않는 품목처럼
      정당하게 0 row인 조합이 있다. 그래서 재추출은 횟수를 제한하고,
      끝까지 비는 조합은 고치려 들지 말고 리포트로만 남긴다.

키 차원은 고정이 아니다. 기본값은 (site, division, category, date)이며,
다른 모델을 쓴다면 dimensions 인자로 바꾼다.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

log = logging.getLogger("act.validator")


# ------------------------------------------------------------------
# 날짜 유틸
# ------------------------------------------------------------------
def daterange(start: str, end: str) -> list[str]:
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    return [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]


def group_contiguous(dates: list[str]) -> list[tuple[str, str]]:
    """정렬된 날짜 목록을 연속 구간 (start, end) 리스트로 압축.
    >>> group_contiguous(["2026-07-01","2026-07-02","2026-07-05"])
    [('2026-07-01', '2026-07-02'), ('2026-07-05', '2026-07-05')]
    """
    if not dates:
        return []
    ds = sorted(date.fromisoformat(d) for d in set(dates))
    ranges, run_start, prev = [], ds[0], ds[0]
    for d in ds[1:]:
        if d != prev + timedelta(days=1):
            ranges.append((run_start.isoformat(), prev.isoformat()))
            run_start = d
        prev = d
    ranges.append((run_start.isoformat(), prev.isoformat()))
    return ranges


# ------------------------------------------------------------------
# 기대 조합 계산
# ------------------------------------------------------------------
# 기대 조합의 키를 이루는 차원. 사이트와 날짜는 항상 들어가고,
# 그 사이에 이 필드들이 task에서 읽혀 들어간다.
DEFAULT_DIMENSIONS = ("division", "category")


def site_key(task: dict, rs: str) -> str:
    """DB의 사이트 식별자. 기본: extra2가 있으면 그것, 없으면 rs.

    한 리포트스위트가 여러 사이트를 담는 경우(지역 통합 RS 등) rs만으로는
    행을 구분할 수 없어서, task가 실제 사이트 코드를 extra2로 들고 다닌다.
    스키마가 다르면 이 함수만 교체하면 된다.
    """
    return task.get("extra2") or rs


def build_expected(tasks: list[dict], dimensions=DEFAULT_DIMENSIONS) -> dict[str, dict]:
    """task 목록 → 테이블별 {키: 원본 task 참조}.

    키 = (site, *dimensions, date). DB 접근 없이 실행 계획만으로 만든다.
    """
    expected: dict[str, dict] = {}
    for t in tasks:
        tbl = expected.setdefault(t["table"], {})
        for rs in t["rs"]:
            site = site_key(t, rs)
            dims = tuple(t[d] for d in dimensions)
            for d in daterange(t["start"], t["end"]):
                tbl[(site, *dims, d)] = (t, rs)
    return expected


# ------------------------------------------------------------------
# 실제 조합 조회 (DB)
# ------------------------------------------------------------------
class SQLValidator:
    """SQLAlchemy engine 기반 기본 구현. 컬럼명이 다르면 colmap만 수정.
    다른 DB 클라이언트를 쓰면 fetch_keys 시그니처만 맞춘 객체를 주입하면 됨."""

    DEFAULT_COLMAP = {"site": "site_code", "division": "division",
                      "category": "category", "date": "date"}

    def __init__(self, engine, colmap: dict | None = None,
                 dimensions=DEFAULT_DIMENSIONS):
        self.engine = engine
        self.c = {**self.DEFAULT_COLMAP, **(colmap or {})}
        self.dimensions = tuple(dimensions)

    def fetch_keys(self, table: str, start: str, end: str) -> set[tuple]:
        """테이블당 쿼리 1회. 존재하는 키 조합만 받는다 (row 전체가 아니다)."""
        from sqlalchemy import text
        c = self.c
        cols = [c["site"]] + [c.get(d, d) for d in self.dimensions] + [c["date"]]
        sql = text(
            f"SELECT DISTINCT {', '.join(cols)} "
            f"FROM {table} WHERE {c['date']} BETWEEN :s AND :e")
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"s": start, "e": end}).fetchall()
        # 날짜는 DB 드라이버에 따라 date/datetime/str 로 오므로 앞 10자로 맞춘다
        return {tuple(str(v) for v in r[:-1]) + (str(r[-1])[:10],) for r in rows}


# ------------------------------------------------------------------
# 검증 → Refill 계획
# ------------------------------------------------------------------
def find_missing(tasks: list[dict], validator, start: str, end: str,
                 dimensions=None) -> dict:
    """반환: {table: {missing_key: (task, rs)}}"""
    if dimensions is None:
        dimensions = getattr(validator, "dimensions", DEFAULT_DIMENSIONS)
    expected = build_expected(tasks, dimensions)
    missing: dict[str, dict] = {}
    for table, exp in expected.items():
        actual = validator.fetch_keys(table, start, end)
        miss = {k: v for k, v in exp.items() if k not in actual}
        if miss:
            missing[table] = miss
        log.info(f"검증 [{table}] 기대 {len(exp):,} / 실제 {len(actual):,} "
                 f"/ 누락 {len(miss):,}")
    return missing


def plan_refill(missing: dict) -> list[dict]:
    """누락 조합 → 최소 개수의 재추출 task.
    (원본 task, rs)별로 누락 날짜를 모아 연속 구간으로 압축."""
    buckets: dict[tuple, list[str]] = {}
    task_ref: dict[tuple, tuple] = {}
    for table, miss in missing.items():
        for (site, div, cat, d), (task, rs) in miss.items():
            key = (id(task), rs)
            buckets.setdefault(key, []).append(d)
            task_ref[key] = (task, rs)

    refill = []
    for key, dates in buckets.items():
        task, rs = task_ref[key]
        for s, e in group_contiguous(dates):
            refill.append({**task, "rs": [rs], "start": s, "end": e})
    return refill


def missing_report(missing: dict, path_prefix: str = "missing_combos") -> Path | None:
    """끝까지 비는 조합을 사람이 볼 수 있게 저장 (정당한 0-row 후보 판단용)."""
    if not missing:
        return None
    out = {tbl: sorted(["|".join(k) for k in m]) for tbl, m in missing.items()}
    p = Path(f"{path_prefix}_{datetime.now():%Y%m%d_%H%M%S}.json")
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return p
