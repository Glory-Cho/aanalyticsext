"""
rate_limiter.py — 클라이언트측 전역 토큰버킷
=============================================
왜 필요한가
-----------
Adobe 게이트웨이 한도(사용자당 분당 ~120건, 상향 불가)를 넘기면 429가 나고,
재시도가 다시 한도를 먹어 '죽음의 나선'에 빠진다(요청은 계속 날아가는데
전부 거절되어 진행이 멈춤). 반대로 한도를 넘지 않으려고 동시성을 낮추면,
VRS처럼 응답이 느린 RS에서는 워커가 놀아 한도에 도달조차 못 한다.

해결: 발사 직전에 클라이언트가 스스로 한도를 지킨다.
그러면 429가 나지 않으므로 **동시성을 얼마든지 높여도 손해가 없다.**
초과분은 서버가 거절하는 대신 로컬에서 잠깐 대기할 뿐이다.

필요 동시성 계산
----------------
한도를 포화시키려면  동시성 ≈ (초당 허용 건수) × (평균 응답 지연)
  · 응답 4초  → 2 × 4  =  8   → 워커 10개로 이미 포화 (parallel_tasks=1)
  · 응답 30초 → 2 × 30 = 60   → parallel_tasks≈6
  · 응답 60초 → 2 × 60 = 120  → parallel_tasks≈12
VRS가 섞여 응답이 느릴수록 병렬을 높이는 것이 이득이다.

사용
----
    from aanalyticsext import limiter, rate_limited

    # 1) 실제 API 호출 지점을 감싼다
    @rate_limited
    def _call_api(...):
        return aa.getReport(...)

    # 2) 또는 직접
    limiter.acquire()          # 토큰이 생길 때까지 대기
    res = aa.getReport(...)

    # 3) 한도 조정 (기본 분당 110 — 마진 10)
    limiter.configure(per_minute=110)
    print(limiter.stats())
"""

from __future__ import annotations

import functools
import threading
import time
from collections import deque


class TokenBucket:
    """스레드 안전 슬라이딩 윈도 레이트 리미터.

    프로세스 전역 인스턴스 하나를 공유해야 의미가 있다
    (여러 task가 동시에 API를 쳐도 합산 한도를 지키기 위함).
    """

    def __init__(self, per_minute: int = 110, window: float = 60.0):
        self.per_minute = per_minute
        self.window = window
        self._times: deque[float] = deque()
        self._lock = threading.Lock()
        self._granted = 0
        self._waited = 0.0
        self._max_wait = 0.0

    def configure(self, per_minute: int | None = None, window: float | None = None):
        with self._lock:
            if per_minute is not None:
                self.per_minute = per_minute
            if window is not None:
                self.window = window

    def acquire(self, timeout: float | None = None) -> bool:
        """토큰 1개를 소비. 한도에 걸리면 여유가 생길 때까지 대기.
        timeout 초과 시 False 반환(그래도 호출은 진행할지 호출부가 결정)."""
        start = time.monotonic()
        while True:
            with self._lock:
                now = time.monotonic()
                while self._times and self._times[0] <= now - self.window:
                    self._times.popleft()
                if len(self._times) < self.per_minute:
                    self._times.append(now)
                    self._granted += 1
                    w = now - start
                    self._waited += w
                    self._max_wait = max(self._max_wait, w)
                    return True
                sleep_for = self._times[0] + self.window - now
            if timeout is not None and (time.monotonic() - start) + sleep_for > timeout:
                time.sleep(max(0.0, timeout - (time.monotonic() - start)))
                return False
            time.sleep(min(max(sleep_for, 0.01), 1.0))

    def stats(self) -> dict:
        with self._lock:
            now = time.monotonic()
            recent = sum(1 for t in self._times if t > now - self.window)
            return {
                "per_minute_limit": self.per_minute,
                "현재_윈도_사용": recent,
                "누적_허용": self._granted,
                "누적_대기초": round(self._waited, 1),
                "평균_대기초": round(self._waited / self._granted, 2) if self._granted else 0,
                "최대_대기초": round(self._max_wait, 1),
            }

    def reset_stats(self):
        with self._lock:
            self._granted = 0
            self._waited = 0.0
            self._max_wait = 0.0


# 프로세스 전역 인스턴스 (기본 분당 110 — 실제 한도 120에서 마진 10)
limiter = TokenBucket(per_minute=110)


def rate_limited(fn):
    """API 호출 함수를 감싸 한도를 지키게 하는 데코레이터."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        limiter.acquire()
        return fn(*args, **kwargs)
    return wrapper


def suggest_parallel(avg_latency_sec: float, per_minute: int | None = None,
                     workers_per_task: int = 10) -> int:
    """평균 응답 지연으로부터 권장 parallel_tasks 계산.

    >>> suggest_parallel(4)    # 빠른 RS
    1
    >>> suggest_parallel(30)   # VRS 섞임
    6
    >>> suggest_parallel(60)   # 느린 VRS 위주
    11
    """
    pm = per_minute or limiter.per_minute
    need = (pm / 60.0) * avg_latency_sec        # 포화에 필요한 동시성
    return max(1, round(need / workers_per_task))
