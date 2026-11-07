import json,zipfile,urllib.request,re
from pathlib import Path
import pandas as pd, numpy as np
ROOT=Path("jef-run"); OUT=ROOT/"output"; OUT.mkdir(parents=True,exist_ok=True)
UA={"User-Agent":"JEF-RR-replication/1.0"}
def fetch(url,path):
    if path.exists() and path.stat().st_size>0:return
    req=urllib.request.Request(url,headers=UA)
    with urllib.request.urlopen(req,timeout=240) as r, open(path,"wb") as f:
        while (b:=r.read(1024*1024)): f.write(b)
release=ROOT/"formd-tier1-M-2026-08-05.zip"
fetch("https://github.com/wookat/dataforge-pipelines/releases/download/open-data-sec-form-d/formd-tier1-M-2026-08-05.zip",release)
with zipfile.ZipFile(release) as z:
    n=next(n for n in z.namelist() if n.endswith("formd_events.parquet")); z.extract(n,ROOT)
cols=["accession","filed_at","quarter","submission_type","previous_accession","issuer_cik","issuer_name","state","country","year_of_incorporation","entity_type","industry_group","investment_fund_type","is_pooled_investment_fund","is_equity","is_debt","total_amount_sold_usd","total_offering_amount_usd","investors_accepted","date_of_first_sale"]
e=pd.read_parquet(ROOT/n,columns=cols)
e=e[(e.quarter>="2016Q1")&(e.quarter<="2026Q2")].drop_duplicates("accession",keep="last").reset_index(drop=True)
for c in ["accession","previous_accession","issuer_cik","state","country","issuer_name","entity_type","industry_group","investment_fund_type","year_of_incorporation"]:e[c]=e[c].fillna("").astype(str)
e["filed_at"]=pd.to_datetime(e.filed_at); e["sold"]=pd.to_numeric(e.total_amount_sold_usd,errors="coerce")
# accession chains, sufficient for first-positive issuer event (verified invariant to ancestry start)
acc=e.accession.tolist(); idx={a:i for i,a in enumerate(acc)}
N=len(e); parent=list(range(N));rank=[0]*N
def find(x):
    while parent[x]!=x: parent[x]=parent[parent[x]];x=parent[x]
    return x
def union(a,b):
    ra,rb=find(a),find(b)
    if ra==rb:return
    if rank[ra]<rank[rb]:ra,rb=rb,ra
    parent[rb]=ra
    if rank[ra]==rank[rb]:rank[ra]+=1
for i,p in enumerate(e.previous_accession):
    if p and p in idx: union(i,idx[p])
e["root"]=[find(i) for i in range(N)]
pos=e[e.sold>0].sort_values("filed_at")
first_chain_pos=pos.drop_duplicates("root",keep="first")
fp=first_chain_pos[first_chain_pos.issuer_cik!=""].sort_values("filed_at").drop_duplicates("issuer_cik",keep="first").copy()
fp=fp[fp.filed_at>=pd.Timestamp("2019-01-01")].copy()
US=set(['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'])
u=fp[fp.state.isin(US)].copy()
# diagnostic possible fund screens
namefund=u.issuer_name.str.contains(r'\b(FUND|CAPITAL|VENTURE|INVESTMENT|HOLDINGS?|SPV|SPECIAL PURPOSE|LP|L\.P\.|TRUST)\b',case=False,regex=True,na=False)
tests={
 "not_pooled": ~u.is_pooled_investment_fund.fillna(False).astype(bool),
 "industry_not_pooled": u.industry_group.str.lower().ne("pooled investment fund"),
 "fundtype_blank": u.investment_fund_type.str.strip().eq(""),
 "not_pooled_and_industry": (~u.is_pooled_investment_fund.fillna(False).astype(bool)) & u.industry_group.str.lower().ne("pooled investment fund"),
 "not_pooled_industry_fundtypeblank": (~u.is_pooled_investment_fund.fillna(False).astype(bool)) & u.industry_group.str.lower().ne("pooled investment fund") & u.investment_fund_type.str.strip().eq(""),
}
summary={"first_positive_2019plus":len(fp),"us":len(u),"target_us":152898,"target_after_fund":61928}
for k,m in tests.items(): summary[k]=int(m.sum())
summary["industry_top"]=u.industry_group.value_counts().head(30).to_dict()
summary["fundtype_top"]=u.investment_fund_type.value_counts().head(30).to_dict()
summary["entity_top"]=u.entity_type.value_counts().head(30).to_dict()
# Age/equity counts for each plausible fund screen
for k,m in tests.items():
    x=u[m].copy()
    inc=pd.to_numeric(x.year_of_incorporation,errors="coerce")
    # Some source encodes "Over Five Years Ago"; diagnostics below.
    age=x.filed_at.dt.year-inc
    young=(age>=0)&(age<=5)
    summary[k+"_young"]=int(young.sum())
    summary[k+"_young_equity"]=int((young & x.is_equity.fillna(False).astype(bool)).sum())
summary["inc_top"]=u.year_of_incorporation.value_counts().head(30).to_dict()
json.dump(summary,open(OUT/"stage4-waterfall-diagnostics.json","w"),indent=2,default=str)
fp.to_parquet(OUT/"first-positive-2019plus.parquet",index=False)
print(json.dumps(summary,indent=2,default=str))
