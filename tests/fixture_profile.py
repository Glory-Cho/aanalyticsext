# 테스트용 합성 프로파일.
#
# 실제 조직 데이터는 이 저장소에 없다. 엔진이 프로파일에 맞춰 동작하는지만
# 확인하면 되므로, 실제 매핑이 가진 "모양"만 흉내 낸 가짜 데이터를 쓴다.
#
#   - 법인 이름에 공백/하이픈이 섞여 있다 (표기 변형 흡수 확인용)
#   - 그룹 이름과 site_code가 대소문자만 다르게 겹친다 (자기 참조 방지 확인용)
#   - 종료된 코드, 마이그레이션 이전 코드가 섞여 있다
#   - 글로벌 통합 RS와 그 하위 통합 RS가 함께 있다 (이중 계상 방지 확인용)

FIXTURE = {
    "rs_map": {
        "default": [
            ["a1", "rs4a1"], ["a2", "rs4a2"],
            ["b1", "rs4b1"], ["closed1", "rs4closed1"],
            ["c1", "rs4c1"], ["c2", "rs4c2"],
            ["d1", "rs4d1"], ["closed2", "rs4closed2"],
            ["old1", "rs4old1"],
            ["total", "rs4total"], ["total_sub", "rs4totalsub"],
        ],
        "epp": [
            ["a1", "rs4a1epp"], ["b1", "rs4b1epp"],
        ],
    },

    # 이름 표기가 제각각인 것도 일부러다
    "subsidiaries": {
        "SUB A":  ["a1", "a2"],
        "SUB-B":  ["b1", "closed1"],
        "SUBC":   ["c1", "c2"],
        "SUB D":  ["d1", "closed2"],
        "C1":     ["c1"],            # 그룹명 'C1' vs 코드 'c1' 충돌
    },
    "regions": {
        "REG ONE": ["SUB A", "SUB-B"],
        "REG TWO": ["SUBC", "SUB D", "C1"],
        "TOTAL":   ["total"],        # 하위 통합 RS(total_sub)는 넣지 않는다
    },
    "group_aliases": {
        "firstregion": "REG ONE",
    },

    "deprecated_site_codes": {
        "closed1": "사이트 종료",
        "closed2": "사이트 종료",
    },
    "legacy_site_codes": {
        "old1": "마이그레이션 이전",
    },
    "unassigned_site_codes": [],

    "total_rsid": "rs4total",
    "total_site_code": "TOTAL",
    "sub_total_prefix": "total_",

    "epp_integrated_rsids": ["rs4a1epp"],
    "site_code_dimension_rsids": ["rs4dim"],

    # rsid 에서 site_code 뽑기: "4"로 자른 뒤 "epp" 제거
    "site_code_rule": {"separator": "4", "take": "last", "strip": ["epp"]},

    # 통합 리포트 필터용 기반 목록
    "mst_base_site_codes": [
        "a1", "a2", "b1", "c1", "c2", "d1", "closed1", "closed2",
        "a1-app", "a2-app", "b1-app", "c1-app", "c2-app", "d1-app",
        "closed1-app", "closed2-app",
        "e1",                       # -app 짝이 없는 코드
    ],
    "mst_app_missing": ["e1"],

    "company_id": "demo0",

    # 테스트에서는 aanalytics2 자체가 모의 객체라 실제로 읽지 않는다.
    "auth_config_path": "/tmp/demo_auth.json",
}
