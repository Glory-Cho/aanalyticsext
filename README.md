# aanalyticsext

Adobe Analytics 2.0 API로 데이터를 **대량으로, 오래** 긁어야 할 때 쓰는 엔진.

Workspace에서 손으로 내려받는 건 국가 하나 기간 한 달이면 되지만, 100개국 ×
2년 × 일별 × 2단 breakdown이 되면 얘기가 다르다. 분당 호출 한도에 걸리고,
중간에 끊기고, 재시도하다 같은 행을 두 번 적재하고, 어디까지 받았는지 아무도
모르게 된다. 이 패키지는 그 지점들을 하나씩 막아둔 것이다.

`aanalytics2`(pitchmuc) 위에 얹는 레이어다. 인증과 API 호출 자체는 그쪽이 하고,
여기서는 "무엇을 언제 어떻게 나눠서 받아 어디에 넣을지"를 맡는다.

## 뭘 해주나

- **기간 분할** — daily / weekly / monthly / all. 요청 종료일을 넘지 않게 자른다.
- **시각(hour) 단위 수집** — 경계 계산을 한 군데(`TimeWindow`)로 모아서, 구간이
  하루씩 밀리거나 겹쳐서 중복 적재되는 걸 막는다.
- **N단 breakdown** — dimension > breakdown > breakdown2. 항목 단위로 재시도하고,
  실패한 항목만 건너뛴 채 나머지를 계속 진행한다.
- **체크포인트/재개** — 중간에 죽어도 다시 돌리면 끝난 항목은 다시 안 부른다.
- **멱등 적재** — `key_columns`를 주면 같은 트랜잭션에서 DELETE 후 INSERT한다.
  재실행해도 행이 불어나지 않는다.
- **호출 한도 대응** — 토큰버킷 + 지수 백오프. 429를 맞고 더 세게 때리는
  악순환을 만들지 않는다.
- **진행 표시** — 한 줄 덮어쓰기, ETA, 비-TTY 환경 자동 전환.

## 조직 데이터는 안 들어 있다

어떤 리포트스위트가 어느 국가인지, 어떤 법인이 어느 국가를 관할하는지는
회사마다 다르고 대개 대외비다. 그래서 **엔진에 넣지 않고 프로파일로 주입한다.**

```python
from aanalyticsext import load_profile, retrieve_by_RS

load_profile("my_profile.json")
```

프로파일이 없어도 엔진은 뜬다. 그룹 확장과 site_code 추론만 비활성 상태가 된다.

```json
{
  "rs_map": {
    "default": [["kr", "myorg_kr"], ["us", "myorg_us"]],
    "epp":     [["kr", "myorg_kr_epp"]]
  },
  "subsidiaries": { "SUB A": ["kr"], "SUB B": ["us"] },
  "regions":      { "APAC": ["SUB A"], "AMER": ["SUB B"] },
  "deprecated_site_codes": { "old_kr": "사이트 종료" },
  "site_code_rule": { "separator": "_", "take": "last", "strip": ["epp"] },
  "company_id": "myorg0"
}
```

전체 항목은 [`aanalyticsext/profile.py`](aanalyticsext/profile.py)의 `Profile`
데이터클래스에 주석과 함께 정리돼 있다. 규칙으로 안 풀리는 rsid는
`site_code_overrides`에 직접 적고, 더 복잡한 규칙이 필요하면
`Profile(site_code_resolver=함수)`로 파이썬 함수를 그대로 넣으면 된다.

프로파일을 새로 쓰거나 고친 뒤에는 반드시 점검할 것:

```python
from aanalyticsext import validateSiteGroups
validateSiteGroups(verbose=True)    # 오타 / 유령 코드 / 고아 법인
```

지리적 직관과 법인 관할은 자주 어긋나고, 약어는 조직마다 다른 걸 가리킨다.
그룹을 잘못 매핑하면 **오류 없이** 엉뚱한 국가가 수집된다.

## 설치

```bash
pip install aanalyticsext
pip install aanalyticsext[mysql]     # MySQL에 적재한다면
```

`aanalytics2`의 인증 설정 파일 경로와 DB 접속 정보는 환경변수로 준다.
기본값은 일부러 두지 않았다 — 패키지에 접속 정보를 넣으면 배포가 곧
자격증명 배포가 된다.

```bash
export AA_AUTH_CONFIG=/path/to/adobe_auth.json
export ACT_DB_URL='mysql+pymysql://user:pass@host:3306/db?charset=utf8mb4'
export ACT_COMPANY_ID=myorg0
```

## 구성

```
aanalyticsext/
├── profile.py      # 조직 데이터 주입 (Profile, load_profile)
├── actRunner.py    # 언제/어디를 — 기간·시각 경계, site_code 확장, 적재 컬럼
├── actModuler.py   # 어떻게 — 요청 조립, 한도/재시도, 응답 정규화, DB 적재
└── actExecute.py   # 공개 API — retrieve_* 함수들
```

## 테스트

```bash
PYTHONPATH="tests:." python tests/test_engine.py
```

모의 Adobe API + SQLite로 돌아간다. 실제 자격증명이 필요 없고,
`tests/fixture_profile.py`의 합성 프로파일을 쓴다 — 실제 조직 데이터는
이 저장소 어디에도 없다.

## 한계

- Adobe 2.0 API의 사용자당 분당 ~120 requests 한도는 우회할 수 없다. 이 패키지는
  그 한도를 **효율적으로 채우는** 것이지 넘기는 게 아니다.
- 적재 대상은 SQLAlchemy가 물리는 DB. 실사용·검증은 MySQL 기준이다.
- `aanalytics2`의 내부 구조에 일부 의존한다(안전 페이지네이션 경로). 구조가
  바뀌면 기존 경로로 자동 대체하고 경고를 남긴다.
