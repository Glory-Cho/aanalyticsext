"""
누락 진단 — 노트북에서 표로 들여다보기 위한 얇은 층
=====================================================
validator가 계산한 차집합을 DataFrame으로 바꿔, 어디가 왜 비었는지
사람이 눈으로 훑을 수 있게 한다. 읽기 전용(SELECT)이라 추출이 돌고 있는 중에
실행해도 안전하다. 다만 진행 중에는 아직 안 채워진 조합도 누락으로 잡히니
진단은 완료 후가 낫다.

    from aanalyticsext import GapChecker
    from sqlalchemy import create_engine

    gc = GapChecker(planner, create_engine(DSN))
    df = gc.scan("2026-08-01", "2026-08-18", "tb_prefix_")

    gc.by_site()      # 사이트별 누락 일수와 구간
    gc.by_date()      # 날짜별 누락 사이트 수 — 특정 시각에 몰리면 장애/토큰만료
    gc.by_vrs()       # VRS vs 일반 RS 누락률 — VRS 쪽이 높으면 처리 로직 점검
    gc.backfill()     # 누락분만 재추출

planner 는 iter_tasks / dry_run / run 을 가진 객체면 무엇이든 된다.
이 모듈은 planner의 설정 파일을 읽지 않는다 — 계획을 받아 대조할 뿐이다.
"""
from __future__ import annotations

from collections import defaultdict

import pandas as pd

from .validator import SQLValidator, build_expected, group_contiguous


class GapChecker:
    def __init__(self, extractor, engine, colmap: dict | None = None,
                 vrs_sites: set[str] | None = None):
        """
        extractor : 실행 계획을 만드는 객체. iter_tasks(start, end, prefix, **filters)가
                    필수이고, backfill까지 쓰려면 dry_run / run 도 있어야 한다.
        engine    : SQLAlchemy engine
        vrs_sites : VRS(Virtual Report Suite) 기반 사이트 집합. 응답이 느려
                    누락이 몰리는 경향이 있어 따로 집계한다. 미지정 시
                    extractor.cfg["rs_vrs"]를 보고, 그것도 없으면 빈 집합.
        """
        self.ex = extractor
        self.v = SQLValidator(engine, colmap)
        cfg_vrs = set(getattr(extractor, "cfg", {}).get("rs_vrs") or [])
        self.vrs = set(vrs_sites) if vrs_sites is not None else cfg_vrs
        self._df = None
        self._ctx = None

    # ---------------- 스캔 ----------------
    def scan(self, start: str, end: str, table_prefix: str, **filters) -> pd.DataFrame:
        """기대 조합 vs 실제 조합 대조. 누락 DataFrame 반환 (읽기 전용)."""
        tasks = list(self.ex.iter_tasks(start, end, table_prefix, **filters))
        expected = build_expected(tasks)

        rows, summary = [], []
        for table, exp in expected.items():
            try:
                actual = self.v.fetch_keys(table, start, end)
            except Exception as e:
                summary.append({"table": table, "기대": len(exp), "실제": None,
                                "누락": None, "비고": f"조회 실패: {e}"})
                continue
            miss = [k for k in exp if k not in actual]
            summary.append({"table": table, "기대": len(exp), "실제": len(actual),
                            "누락": len(miss),
                            "누락률": f"{len(miss)/max(len(exp),1)*100:.1f}%"})
            for site, div, cat, d in miss:
                rows.append({"table": table, "site": site, "division": div,
                             "category": cat, "date": d,
                             "vrs": site in self.vrs})

        self.summary = pd.DataFrame(summary)
        self._df = pd.DataFrame(rows)
        self._ctx = dict(start=start, end=end, table_prefix=table_prefix, filters=filters)
        print(self.summary.to_string(index=False))
        if self._df.empty:
            print("\n[OK] 누락 없음")
        else:
            n_vrs = int(self._df["vrs"].sum())
            print(f"\n누락 {len(self._df):,}건 | VRS 사이트 {n_vrs:,}건 "
                  f"({n_vrs/len(self._df)*100:.0f}%) | 일반 RS {len(self._df)-n_vrs:,}건")
        return self._df

    def _check(self):
        if self._df is None:
            raise RuntimeError("먼저 scan()을 실행하세요.")

    # ---------------- 집계 뷰 ----------------
    def by_site(self, top: int = 50) -> pd.DataFrame:
        """국가별 누락 일수 + 누락 구간 (연속 날짜 압축)."""
        self._check()
        if self._df.empty:
            return self._df
        out = []
        for (tbl, site, vrs), g in self._df.groupby(["table", "site", "vrs"]):
            dates = sorted(g["date"].unique())
            rng = group_contiguous(dates)
            out.append({"table": tbl, "site": site, "VRS": "✔" if vrs else "",
                        "누락일수": len(dates),
                        "구간": ", ".join(f"{s}~{e}" if s != e else s for s, e in rng[:3])
                                + (f" 외 {len(rng)-3}구간" if len(rng) > 3 else "")})
        return (pd.DataFrame(out).sort_values("누락일수", ascending=False)
                .head(top).reset_index(drop=True))

    def by_date(self, top: int = 30) -> pd.DataFrame:
        """날짜별 누락 국가 수. 특정 날짜에 몰리면 그 시각 장애/토큰만료 의심."""
        self._check()
        if self._df.empty:
            return self._df
        g = (self._df.groupby("date")
             .agg(누락국가수=("site", "nunique"), 누락조합=("site", "size"),
                  VRS비중=("vrs", lambda s: f"{s.mean()*100:.0f}%"))
             .sort_values("누락국가수", ascending=False))
        return g.head(top).reset_index()

    def by_vrs(self) -> pd.DataFrame:
        """VRS vs 일반 RS 누락 비교 — VRS 쪽이 높으면 VRS 처리 로직 점검 필요."""
        self._check()
        if self._df.empty:
            return self._df
        return (self._df.groupby("vrs")
                .agg(누락조합=("site", "size"), 국가수=("site", "nunique"))
                .rename(index={True: "VRS", False: "일반 RS"})
                .reset_index().rename(columns={"vrs": "구분"}))

    def sites(self, vrs_only: bool = False) -> list[str]:
        """누락된 사이트 목록 (backfill의 countries 인자로 바로 사용 가능)."""
        self._check()
        if self._df.empty:
            return []
        df = self._df[self._df["vrs"]] if vrs_only else self._df
        return sorted(df["site"].unique())

    # ---------------- 재추출 ----------------
    def backfill(self, refill_rounds: int = -1, refill_stall_limit: int = 2,
                 parallel_tasks: int = 2, countries: list[str] | None = None,
                 dry_run: bool = False):
        """누락된 국가만 재추출. scan()의 기간·필터를 그대로 재사용.

        countries 생략 시 누락된 사이트 전체를 대상으로 함.
        dry_run=True 면 실행 계획만 출력.
        """
        self._check()
        if self._df.empty:
            print("누락이 없어 재추출할 것이 없습니다.")
            return None
        c = dict(self._ctx)
        filters = dict(c["filters"])
        filters["countries"] = countries or self.sites()
        print(f"재추출 대상 {len(filters['countries'])}개 사이트: "
              f"{', '.join(filters['countries'][:15])}"
              f"{' …' if len(filters['countries']) > 15 else ''}")
        if dry_run:
            return self.ex.dry_run(c["start"], c["end"], c["table_prefix"], **filters)
        return self.ex.run(c["start"], c["end"], c["table_prefix"],
                           parallel_tasks=parallel_tasks, validator=self.v,
                           refill_rounds=refill_rounds,
                           refill_stall_limit=refill_stall_limit, **filters)
