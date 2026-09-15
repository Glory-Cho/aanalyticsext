"""Adobe Analytics 2.0 API 대량 추출 엔진.

기간을 나눠 돌리고, 호출 한도와 재시도를 감당하고, 결과를 DB에 적재한다.
조직별 데이터(리포트스위트 매핑, 법인/지역 그룹)는 코드가 아니라
프로파일로 주입한다 — `load_profile()` 참고.

    from aanalyticsext import load_profile, retrieve_by_RS
    load_profile("my_profile.json")
"""

from .__version__ import __version__

from .profile import Profile, SiteCodeRule, load_profile, current_profile, clear_profile

from .actExecute import *
from .actModuler import *
from .actRunner import *

# 완결성 검증 / 누락 진단 / 호출 한도 제어.
# pandas·sqlalchemy 위에서만 도는 부수 기능이라 엔진 본체와 독립적이다.
from .validator import (SQLValidator, build_expected, find_missing, plan_refill,
                        missing_report, group_contiguous, daterange, site_key,
                        DEFAULT_DIMENSIONS)
from .gaps import GapChecker
from .rate_limiter import limiter, rate_limited, TokenBucket, suggest_parallel
