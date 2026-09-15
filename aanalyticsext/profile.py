# 조직별 데이터를 엔진 바깥에서 주입한다.
#
# 이 패키지는 Adobe Analytics 2.0 API를 다루는 엔진만 담는다.
# 어떤 리포트스위트가 어느 국가인지, 어떤 법인이 어느 국가를 관할하는지는
# 회사마다 다르고 대개 대외비라서, 코드가 아니라 프로파일로 주입받는다.
#
#     from aanalyticsext import load_profile
#     load_profile("my_profile.json")
#
# 프로파일을 넣지 않으면 그룹 확장과 site_code 추론이 비활성 상태로 동작한다.
# (rsInput에 site_code 대신 rsid를 직접 넘기는 방식은 프로파일 없이도 된다.)

from dataclasses import dataclass, field
import json
import logging

logger = logging.getLogger("act.profile")

__all__ = ["Profile", "SiteCodeRule", "load_profile", "current_profile",
           "clear_profile"]


@dataclass
class SiteCodeRule:
    """rsid에서 site_code를 뽑아내는 규칙.

    리포트스위트 이름에 국가 코드가 박혀 있는 경우가 많다.
    예를 들어 구분자가 "4"이고 접미사 "epp"를 떼는 규칙이라면
        "<prefix>4ukepp" -> "uk"
    JSON으로 선언할 수 있게 일부러 데이터로만 표현했다.
    더 복잡한 규칙이 필요하면 Profile.site_code_resolver에 함수를 직접 넣는다.
    """

    separator: str = ""          # 이 문자로 자른 뒤
    take: str = "last"           # "last" | "first" 조각을 쓰고
    strip: list = field(default_factory=list)   # 이 접미사들을 떼어낸다

    def __call__(self, rsid):
        out = str(rsid)
        if self.separator:
            parts = out.split(self.separator)
            out = parts[-1] if self.take == "last" else parts[0]
        for suffix in self.strip:
            if suffix and suffix in out:
                out = out.replace(suffix, "")
        return out


def _identity_rule(rsid):
    """프로파일이 없을 때의 기본값. rsid를 그대로 site_code로 쓴다."""
    return str(rsid)


@dataclass
class Profile:
    """한 조직의 리포트스위트/조직 매핑 묶음."""

    # site_code -> rsid 매핑. epp 여부에 따라 다른 목록을 쓴다.
    rs_map: dict = field(default_factory=lambda: {"default": [], "epp": []})

    # 조직 그룹. rsInput에 법인명·지역명을 넣으면 site_code로 펼쳐진다.
    subsidiaries: dict = field(default_factory=dict)
    regions: dict = field(default_factory=dict)
    group_aliases: dict = field(default_factory=dict)

    # 수집 대상에서 자동으로 빼는 코드들
    deprecated_site_codes: dict = field(default_factory=dict)
    legacy_site_codes: dict = field(default_factory=dict)
    unassigned_site_codes: list = field(default_factory=list)

    # 전체 통합 리포트스위트 (여러 국가가 한 RS에 들어오는 경우)
    total_rsid: str = ""
    total_site_code: str = ""

    # EPP 트래픽이 일반 RS에 통합돼 들어오는 리포트스위트 (is_epp_integ = Y)
    epp_integrated_rsids: list = field(default_factory=list)

    # 리포트가 site_code를 차원으로 돌려주는 리포트스위트.
    # 이 RS들은 site_code를 rsid에서 추론하지 않고 응답에서 뽑는다.
    site_code_dimension_rsids: list = field(default_factory=list)

    # 하위 통합 RS를 알아보는 접두사 (validateSiteGroups 진단용)
    sub_total_prefix: str = ""

    # 통합(MST) 리포트 필터용 site_code 목록과 보정값
    mst_base_site_codes: list = field(default_factory=list)
    mst_app_missing: list = field(default_factory=list)
    mst_extra_site_codes: list = field(default_factory=list)
    mst_epp_suffix: str = "_epp"
    mst_app_suffix: str = "-app"

    # Adobe 조직(company) ID
    company_id: str = ""

    # Adobe 인증 설정 파일(JSON) 경로
    auth_config_path: str = ""

    # DB 접속 URL. 환경변수 ACT_DB_URL이 있으면 그쪽이 우선한다.
    # 공개 저장소에 두는 프로파일이라면 여기 실제 접속 정보를 넣지 말 것.
    db_url: str = ""

    # rsid -> site_code 예외. 규칙으로 안 풀리는 것만 직접 적는다.
    site_code_overrides: dict = field(default_factory=dict)

    # rsid -> site_code 규칙. SiteCodeRule 또는 임의의 callable.
    site_code_resolver: object = None

    @classmethod
    def from_mapping(cls, data):
        data = dict(data)
        rule = data.pop("site_code_rule", None)
        resolver = data.pop("site_code_resolver", None)
        if resolver is None and isinstance(rule, dict):
            resolver = SiteCodeRule(**rule)
        known = {f for f in cls.__dataclass_fields__}
        unknown = set(data) - known
        if unknown:
            logger.warning("프로파일에 모르는 항목이 있어 무시합니다: %s",
                           sorted(unknown))
        return cls(site_code_resolver=resolver,
                   **{k: v for k, v in data.items() if k in known})

    def site_code_of(self, rsid):
        """rsid에서 site_code를 구한다. 예외 > 통합 RS > 규칙 순으로 본다."""
        if rsid in self.site_code_overrides:
            return self.site_code_overrides[rsid]
        if self.total_rsid and rsid == self.total_rsid:
            return self.total_site_code or self.total_rsid
        rule = self.site_code_resolver or _identity_rule
        return rule(rsid)

    def rs_list(self, epp):
        return self.rs_map.get("epp" if epp is True else "default", [])


_EMPTY = Profile()
_current = _EMPTY


def current_profile():
    """현재 적용된 프로파일. 주입 전이면 빈 프로파일이다."""
    return _current


def load_profile(source):
    """프로파일을 읽어 엔진 전역에 적용한다.

    source 는 JSON 파일 경로, dict, 또는 Profile 인스턴스.
    이미 임포트된 모듈의 전역값을 제자리에서 갈아끼우므로,
    호출 시점 이후의 모든 함수가 새 매핑을 본다.
    """
    if isinstance(source, Profile):
        prof = source
    elif isinstance(source, dict):
        prof = Profile.from_mapping(source)
    else:
        with open(source, "r", encoding="utf-8") as f:
            prof = Profile.from_mapping(json.load(f))
    _apply(prof)
    return prof


def clear_profile():
    """프로파일을 지운다. 주로 테스트에서 쓴다."""
    _apply(Profile())


def _apply(prof):
    """모듈 전역을 제자리에서 교체한다.

    `from .actRunner import *` 로 이름을 가져다 쓴 곳이 있으므로
    리바인딩이 아니라 기존 객체의 내용을 비우고 채우는 방식이어야 한다.
    """
    global _current
    from . import actRunner as R
    from . import actModuler as M

    R._DEFAULT_NONE[:] = prof.rs_list(False)
    R._DEFAULT_EPP[:] = prof.rs_list(True)

    for target, src in ((R.SUBSIDIARIES, prof.subsidiaries),
                        (R.REGIONS, prof.regions),
                        (R.GROUP_ALIASES, prof.group_aliases),
                        (R.DEPRECATED_SITE_CODES, prof.deprecated_site_codes),
                        (R.LEGACY_SITE_CODES, prof.legacy_site_codes)):
        target.clear()
        target.update(src)
    R.UNASSIGNED_SITE_CODES[:] = prof.unassigned_site_codes
    R.SUB_TOTAL_PREFIX = prof.sub_total_prefix

    M.TOTAL_RSID = prof.total_rsid
    M.TOTAL_SITE_CODE = prof.total_site_code
    M.EPP_INTEGRATED_RSIDS = tuple(prof.epp_integrated_rsids)
    M.SITE_CODE_DIMENSION_RSIDS = tuple(prof.site_code_dimension_rsids)
    M._site_code_of = prof.site_code_of

    M._DEFAULT_SITE_CODES[:] = prof.mst_base_site_codes
    M.MST_APP_MISSING[:] = prof.mst_app_missing
    M.MST_EXTRA_SITE_CODES[:] = prof.mst_extra_site_codes
    M.MST_EPP_SUFFIX = prof.mst_epp_suffix
    M.MST_APP_SUFFIX = prof.mst_app_suffix
    if prof.company_id:
        M.COMPANY_ID = prof.company_id
    if prof.auth_config_path:
        M.AUTH_CONFIG_PATH = prof.auth_config_path
    if prof.db_url:
        M.DB_URL = prof.db_url

    # 필터 목록은 임포트 시점에 한 번 계산돼 있으므로 다시 만든다.
    # DEPRECATED_SITE_CODES를 참조하므로 그룹 주입 뒤에 와야 한다.
    M.MST_SITE_CODES[:] = M.buildMstFilterCodes()

    _current = prof
    logger.info("프로파일 적용: RS %d개(EPP %d개), 법인 %d개, 지역 %d개",
                len(R._DEFAULT_NONE), len(R._DEFAULT_EPP),
                len(R.SUBSIDIARIES), len(R.REGIONS))
