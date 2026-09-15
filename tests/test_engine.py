# 통합 회귀 테스트. 모의 Adobe API + SQLite로 실행한다: python test_act.py
# 실제 자격증명 없이 동작하며, 배포 전 이 파일을 돌려 회귀를 확인할 것.
import os
for _p in ("/tmp/act.db",):
    os.path.exists(_p) and os.remove(_p)
import _stub, json, logging
logging.getLogger("act").setLevel(logging.WARNING)
import pandas as pd
import aanalytics2 as api2
from fixture_profile import FIXTURE
import aanalyticsext
aanalyticsext.load_profile(FIXTURE)
from aanalyticsext import actModuler as M, actExecute as E, actRunner as R
from sqlalchemy import create_engine, text

eng = create_engine("sqlite:////tmp/act.db")
M._db_engine = eng
M.get_db_engine = lambda *a, **k: eng
E.get_db_engine = lambda *a, **k: eng

LEVEL0=[("KR",100,5),("DE",200,9)]; IDS={"KR":"id_kr","DE":"id_de"}
L1={"id_kr":[("Mobile","id_mo"),("PC","id_pc")],"id_de":[]}
L2={("id_kr","id_mo"):[["/home",50,2],["/shop",30,1]],("id_kr","id_pc"):[]}
HOURS_WITH_DATA={9,10,11,13,14,17,18}
SEEN=[]

def dr_of(p):
    for gf in p["globalFilters"]:
        if gf.get("type")=="dateRange": return gf["dateRange"]
    raise AssertionError("dateRange 필터 없음")

def handler(payload, item_id):
    for gf in payload["globalFilters"]:
        if gf.get("type")=="segment":
            assert "dateRange" not in gf, "세그먼트 필터 오염!"
    dr = dr_of(payload); SEEN.append(dr)
    mf=[f for f in payload.get("metricContainer",{}).get("metricFilters",[]) if f.get("type")=="breakdown"]
    chain=tuple(f["itemId"] for f in mf)
    if len(chain)==2:
        rows=L2.get(chain,[])
        return {"data": pd.DataFrame(rows,columns=["v","visits","orders"]) if rows else pd.DataFrame()}
    if len(chain)==1:
        rows=L1.get(chain[0],[])
        return {"data": pd.DataFrame([[v,1,1,i] for v,i in rows],columns=["v","a","b","item_id"]) if rows else pd.DataFrame()}
    s,e = dr.split("/")
    hourly = (s[:10]==e[:10])
    if hourly and int(s[11:13]) not in HOURS_WITH_DATA:
        return {"data": pd.DataFrame()}
    if item_id:
        return {"data": pd.DataFrame([[v,a,b,IDS[v]] for v,a,b in LEVEL0],columns=["v","a","b","item_id"])}
    return {"data": pd.DataFrame([[v,a,b] for v,a,b in LEVEL0],columns=["v","visits","orders"])}
api2.Analytics.HANDLER = handler

base={"rsid":"rs4a1",
      "globalFilters":[{"type":"segment","segmentId":"s1"},{"type":"dateRange","dateRange":"x"}],
      "metricContainer":{"metrics":[{"columnId":"0","id":"metrics/visits"},
                                    {"columnId":"1","id":"metrics/orders"}]},
      "dimension":"variables/evar2"}     # capacityMetadata 없음
json.dump(base, open("/tmp/l0.json","w"))

print("=== [1] 기존 호출 방식 그대로 (1단 일별) ===")
E.retrieve_FirstLevel("2026-06-01","2026-06-02","daily","/tmp/l0.json",
                      ["visits","orders"],"tb_first",False,False)
with eng.connect() as c:
    rows=c.execute(text("select * from tb_first")).fetchall()
for r in rows: print("  ",r)
assert len(rows)==4

print("\n=== [2] 시각 지정: 34시간 -> 10시간 ===")
SEEN.clear()
E.retrieve_FirstLevel("2026-06-01","2026-06-01","daily","/tmp/l0.json",
                      ["visits","orders"],"tb_hr",False,False,
                      start_hour="09:00",end_hour="18:00")
print("  요청 dateRange:", SEEN[0])
assert SEEN[0]=="2026-06-01T09:00:00.000/2026-06-01T18:01:00.000"
with eng.connect() as c:
    print("  DB 라벨:", c.execute(text("select start_date,end_date from tb_hr limit 1")).fetchall())

print("\n=== [3] hourly 09~18시 (10구간, 결손 0채움) ===")
df = E.retrieve_Hourly("2026-06-01","2026-06-01","/tmp/l0.json",["visits","orders"],
                       "tb_hourly","N",start_hour="09:00",end_hour="18:00")
print(df[["site_code","dimension","start_date","end_date","visits"]].to_string(index=False))
assert df.start_date.nunique()==10
assert set(df[df.visits==0].start_date)=={f"2026-06-01 {h}:00" for h in ("12","15","16")}

print("\n=== [4] 2단 breakdown (dimension 이름만, 템플릿 불필요) ===")
df3 = E.retrieve_ThirdLevel("2026-06-01","2026-06-01","daily","/tmp/l0.json",
        "variables/evar3","variables/evar4",["visits","orders"],"tb_third","N",dry_run=True)
print(df3.to_string(index=False))
assert len(df3)==4
assert set(df3[df3.dimension=="DE"].breakdown)=={"(none)"}
assert set(df3[(df3.dimension=="KR")&(df3.breakdown=="PC")].breakdown2)=={"(none)"}

print("\n=== [5] 멱등 적재 ===")
for i in range(3):
    E.retrieve_FirstLevel("2026-06-01","2026-06-01","daily","/tmp/l0.json",
        ["visits","orders"],"tb_idem",False,False,
        key_columns=["site_code","start_date","end_date"])
    with eng.connect() as c:
        n=c.execute(text("select count(*) from tb_idem")).scalar()
    print(f"  {i+1}회 실행 후 행수: {n}")
assert n==2

print("\n=== [6] site_code 오타 검출 / weekly 범위 / worker_refine_common ===")
import io, contextlib
R.returnRsList(False, ["a1","a1x","b1"])
s,e = R.dateGenerator("2026-06-01","2026-06-30","weekly")
print(f"  weekly 마지막 구간: {s[-1]}~{e[-1]} (요청 종료 2026-06-30)")
assert e[-1]=="2026-06-30"
task=("2026-06-01","2026-06-01","/tmp/l0.json",["a1","rs4a1"],"daily",
      ["site_code","RS ID","Biz_type","Division","Category","Device_type","Date","Channel_Raw","visits","orders"],
      "tb_rb","N",0,"","","00:00","00:00",2,"RB")
M.worker_refine_common(task)
with eng.connect() as c:
    print("  worker_refine_common 적재 행수:", c.execute(text("select count(*) from tb_rb")).scalar())

print("\n✅ 전체 통합 검증 통과")

# ---------------------------------------------------------------------------
# 체크포인트 / 항목 단위 재시도 회귀
# ---------------------------------------------------------------------------
import shutil, os as _os
shutil.rmtree("/tmp/ckpt_t", ignore_errors=True)
M._bucket = M._TokenBucket(60000)

ITEMS2=[("KR","id_kr"),("DE","id_de"),("FR","id_fr"),("IT","id_it")]
C2={"level0":0,"item":{}}; FAIL2={"id_fr"}
def h2(payload,item_id):
    mf=[f for f in payload.get("metricContainer",{}).get("metricFilters",[]) if f.get("type")=="breakdown"]
    if not mf:
        C2["level0"]+=1
        rows=[[v,1,1,i] for v,i in ITEMS2] if item_id else [[v,1,1] for v,_ in ITEMS2]
        cols=["v","a","b","item_id"] if item_id else ["v","a","b"]
        return {"data":pd.DataFrame(rows,columns=cols)}
    iid=mf[0]["itemId"]; C2["item"][iid]=C2["item"].get(iid,0)+1
    if iid in FAIL2: raise ConnectionError("Connection aborted")
    return {"data":pd.DataFrame([["/home",10,1]],columns=["v","visits","orders"])}
api2.Analytics.HANDLER=h2

bdj={"rsid":"rs4a1","globalFilters":[{"type":"dateRange","dateRange":"x"}],
  "metricContainer":{"metrics":[{"columnId":"0","id":"metrics/visits","filters":["bf0"]},
                                {"columnId":"1","id":"metrics/orders","filters":["bf0"]}],
    "metricFilters":[{"id":"bf0","type":"breakdown","dimension":"variables/evar2","itemId":"__X__"}]},
  "dimension":"variables/evar3"}
json.dump(bdj,open("/tmp/b1.json","w"))
KEY=["site_code","dimension","start_date","end_date"]

print("\n=== [7] 실패 항목 건너뛰고 계속 진행 ===")
E.retrieve_by_RS_breakdown("2026-06-01","2026-06-01","daily","/tmp/l0.json","/tmp/b1.json",
    ["a1"],["visits","orders"],"tb_ck",False,checkpoint_dir="/tmp/ckpt_t",
    item_retries=1,key_columns=KEY)
print(f"  level-0 {C2['level0']}회, IT 호출 {C2['item'].get('id_it',0)}회")
assert C2["level0"]==1 and C2["item"].get("id_it")==1

print("\n=== [8] 재실행 시 완료 항목 재호출 안 함 ===")
C2["level0"]=0; C2["item"].clear()
E.retrieve_by_RS_breakdown("2026-06-01","2026-06-01","daily","/tmp/l0.json","/tmp/b1.json",
    ["a1"],["visits","orders"],"tb_ck",False,checkpoint_dir="/tmp/ckpt_t",
    item_retries=1,key_columns=KEY)
assert C2["level0"]==0 and not C2["item"]
print("  재호출 없음 (체크포인트 재사용)")

print("\n=== [9] retry_failed=True로 실패분만 재시도 ===")
FAIL2.clear(); C2["item"].clear()
E.retrieve_by_RS_breakdown("2026-06-01","2026-06-01","daily","/tmp/l0.json","/tmp/b1.json",
    ["a1"],["visits","orders"],"tb_ck",False,checkpoint_dir="/tmp/ckpt_t",
    item_retries=1,retry_failed=True,key_columns=KEY)
assert list(C2["item"])==["id_fr"]
with eng.connect() as c:
    n=c.execute(text("select count(*) from tb_ck")).scalar()
print(f"  FR만 재호출, DB 총 {n}행 (중복 없음)")
assert n==4

print("\n=== [10] 페이지네이션 중단 감지 ===")
def hp(payload,item_id):
    print("Warning : No data returned & lastPage is False.")
    print("Exit the loop - no save file & empty dataframe.")
    return {"data":pd.DataFrame()}
api2.Analytics.HANDLER=hp
try:
    M.dataretriever_data({"rsid":"r","globalFilters":[{"type":"dateRange","dateRange":"x"}],
                          "metricContainer":{"metrics":[]},"dimension":"variables/evar2"})
    raise AssertionError("예외 없음")
except M.PartialDataError:
    print("  PartialDataError로 구분됨 (EmptyDataError 아님)")

print("\n✅ 체크포인트 회귀 통과")
# ---------------------------------------------------------------------------
# 그룹 확장 · deprecated · 통합 리포트 필터 (합성 프로파일 기준)
# ---------------------------------------------------------------------------
# 실제 조직 데이터는 이 저장소에 없다. fixture_profile.FIXTURE 가 실제 매핑의
# "모양"만 흉내 내고, 여기서는 엔진이 그 모양대로 동작하는지를 본다.

print("\n=== [11] 종료된 site_code 자동 제외 ===")
assert [c for c, _ in R.returnRsList(False, ["a1", "closed1", "closed2", "b1"])] \
       == ["a1", "b1"]
assert R.returnRsList(False, ["closed1"], include_deprecated=True)
print("  기본 제외 / include_deprecated=True 시 포함 확인")

print("\n=== [12] 통합 리포트 필터의 EPP 변형 ===")
dfm = pd.DataFrame({"site_code": ["a1", "a1_epp", "b1_epp", "c1", "zzz"],
                    "v": [1, 2, 3, 4, 5]})
got = M.filterSiteCode(dfm, "").site_code.tolist()
assert got == ["a1", "a1_epp", "b1_epp", "c1"], got
print(f"  {len(M._DEFAULT_SITE_CODES)}개 -> {len(M.MST_SITE_CODES)}개, 필터 결과 {got}")

print("\n=== [13] Region / Subsidiary 확장 ===")
assert R.expandSiteCodes(["SUB A"]) == ["a1", "a2"]
assert R.expandSiteCodes(["REG TWO"]) == ["c1", "c2", "d1"]
# 종료 코드는 소속은 유지하되 확장에서 빠진다
assert "closed1" in R.SUBSIDIARIES["SUB-B"] and R.expandSiteCodes(["SUB-B"]) == ["b1"]
# 이름 표기 차이 흡수 (공백/하이픈/언더바/대소문자)
_b = R.expandSiteCodes(["SUB A"])
for _n in ["sub a", "suba", "SUB-A", "Sub_A"]:
    assert R.expandSiteCodes([_n]) == _b, _n
# 개수를 하드코딩하면 매핑이 바뀔 때마다 깨진다. 정의와 대조한다.
_expect = []
for _sub in R.REGIONS["REG ONE"]:
    _expect += R.SUBSIDIARIES[_sub]
assert R.expandSiteCodes(["REG ONE"]) == [
    c for c in _expect if c not in R.DEPRECATED_SITE_CODES]
# 별칭은 실제 그룹명을 절대 덮지 않는다
_idx = R._groupLookup()
assert not [a for a in R.GROUP_ALIASES if a in _idx], "별칭이 실제 그룹명과 충돌"
for _a, _t in R.GROUP_ALIASES.items():
    assert R.expandSiteCodes([_a]) == R.expandSiteCodes([_t]), _a
# 틀린 제안은 하지 않는다
assert R.suggestGroup("firstregion") == "REG ONE"
assert R.suggestGroup("전혀없는이름") is None
# 그룹명과 site_code가 대소문자만 다르게 겹치면 코드로 본다 (자기 참조 방지)
assert R.expandSiteCodes(["C1"]) == ["c1"]
# 글로벌 통합 RS에 하위 통합 RS를 넣지 않는다 (이중 계상 방지)
assert R.expandSiteCodes(["TOTAL"]) == ["total"]
assert not [c for c in R.REGIONS["TOTAL"] if c.startswith(R.SUB_TOTAL_PREFIX)]
assert R.expandSiteCodes(["total_sub"]) == ["total_sub"]   # 직접 지정은 가능
print(f"  지역 {len(R.REGIONS)}개 / 법인 {len(R.SUBSIDIARIES)}개, "
      f"REG ONE {len(R.expandSiteCodes(['REG ONE']))}개")

print("\n=== [14] 그룹 매핑 검증 ===")
issues = R.validateSiteGroups(verbose=False)
assert not issues.get("phantom"), issues          # 존재하지 않는 코드 = 오타
# 하위 통합 RS는 의도적 미포함이므로 ungrouped에 남는 것이 정상
assert all(c.startswith(R.SUB_TOTAL_PREFIX)
           for c in issues.get("ungrouped", [])), issues
assert not issues.get("orphan_subsidiary"), issues
print("  오타 0 / 하위 통합 RS만 ungrouped로 분리 보고")

print("\n=== [15] 통합 필터 보정 (종료사이트/앱짝/중복부착) ===")
_f = set(M.MST_SITE_CODES)
for _c in ["closed1", "closed2"]:
    assert _c not in _f and _c + "-app" not in _f and _c + "_epp" not in _f
assert all(c + "-app" in _f for c in M.MST_APP_MISSING)
assert "a1_epp_epp" not in _f and "a1_epp" in _f
assert {"e1", "e1-app", "e1_epp", "e1_epp-app"} <= _f
print(f"  필터 {len(M.MST_SITE_CODES)}개: 종료사이트 제거 / -app 짝 보완 / "
      f"EPP 중복부착 없음")

print("\n=== [16] 레거시 이중계상 방지 ===")
assert "old1" not in R.expandSiteCodes(["REG ONE"])
assert "a1" in R.expandSiteCodes(["REG ONE"])
assert "old1" in R.expandSiteCodes(["old1"])          # 직접 지정은 존중
print("  그룹 확장 시 신규 코드만 포함, 레거시 제외")

print("\n=== [16b] 프로파일 없이도 엔진이 뜬다 ===")
aanalyticsext.clear_profile()
assert R._DEFAULT_NONE == [] and R.SUBSIDIARIES == {}
assert M._siteCodeFromRsid("rs4a1epp") == "rs4a1epp"   # 규칙 없으면 그대로
assert M._isTotalRsid("") is False                     # 빈 TOTAL_RSID 오매칭 금지
aanalyticsext.load_profile(FIXTURE)
assert M._siteCodeFromRsid("rs4a1epp") == "a1"
print("  빈 프로파일 <-> 주입 왕복 정상")

print("\n✅ 그룹 기능 회귀 통과")

print("\n=== [17] tbColumn 개수 불일치 조기 차단 ===")
for _n, _cols in [(1, ["visits"]), (3, ["visits", "orders", "revenue"])]:
    try:
        E.retrieve_ThirdLevel("2026-06-01", "2026-06-01", "daily", "/tmp/l0.json",
                              "variables/evar3", "variables/evar4", _cols,
                              "tb_x", "N", dry_run=True)
        raise AssertionError(f"tbColumn {_n}개인데 예외 없음")
    except M.PermanentError:
        pass
print("  적게/많게 준 경우 모두 PermanentError로 즉시 중단 (재시도 안 함)")

print("\n=== [18] monthly 월말 강제 고정 해제 ===")
for _sd, _ed in [("2026-06-05","2026-06-20"), ("2026-08-01","2026-08-14"),
                 ("2026-12-10","2027-02-05")]:
    _s, _e = R.dateGenerator(_sd, _ed, "monthly")
    assert _e[-1] <= _ed, (_sd, _ed, _e[-1])
assert R.dateGenerator("2026-01-15","2026-04-10","monthly")[1][-1] == "2026-04-10"
print("  요청 종료일을 넘는 구간 없음 (clamp=False면 기존 동작)")

print("\n✅ 전체 회귀 통과")

print("\n=== [19] 진행 표시 ===")
_p = M._Progress(4, "테스트", every=1)
_p.step("A", "ok", rows=3); _p.step("B", "fail"); _p.step("C", "skip", quiet=True)
_p.step("D", "ok", rows=2)
assert (_p.ok, _p.failed, _p.skipped, _p.rows) == (2, 1, 1, 3+2)
assert M._Progress._hms(75) == "01:15" and M._Progress._hms(3725) == "01:02:05"
assert M.logger.propagate is False          # 로그 중복 출력 방지
print("  진행/ETA 집계 정상, 로그 중복 없음")

print("\n=== [20] 한 줄 덮어쓰기 ===")
import io as _io
class _Tty(_io.StringIO):
    def isatty(self): return True
_b = _Tty()
_saved = M.logger.handlers
M.logger.handlers = [logging.StreamHandler(_b)]
M.logger.setLevel(logging.INFO)
_pr = M._Progress(3, "job", 1, mode="inline", stream=_b)
_pr.step("KR", "ok", 2)
_pr.log(logging.ERROR, "[job] %s 건너뜀", "FR")     # 진행 줄 지우고 로그
_pr.step("독일DE", "ok", 1)
_pr.step("SE", "ok", 1)
_pr.finish()
_out = _b.getvalue()
assert "\r" in _out
# 한글 폭 계산 (잔상 방지)
assert M._displayWidth("독일DE") == 6 and M._displayWidth("KR") == 2
# 비-TTY에서는 \r 를 쓰지 않는다 (파일 리다이렉트 / ECS 로그)
_plain = _io.StringIO()
M.logger.handlers = [logging.StreamHandler(_plain)]
_pr2 = M._Progress(2, "job", 1, mode="auto", stream=_plain)
assert _pr2.inline is False
_pr2.step("A", "ok", 1); _pr2.finish()
assert "\r" not in _plain.getvalue()
# off 모드
_off = _Tty(); M.logger.handlers = [logging.StreamHandler(_off)]
_pr3 = M._Progress(2, "job", 1, mode="off", stream=_off)
_pr3.step("A", "ok", 1)
assert "\r" not in _off.getvalue()
M.logger.handlers = _saved
print("  inline 덮어쓰기 / 한글 폭 / 비-TTY 자동전환 / off 모드 정상")

print("\n✅ 전체 회귀 통과")
