import json,zipfile,urllib.request,re
from pathlib import Path
import pandas as pd, numpy as np
ROOT=Path("jef-run");OUT=ROOT/"output";OUT.mkdir(parents=True,exist_ok=True)
UA={"User-Agent":"JEF-RR-replication/1.0"}
def fetch(url,p):
    if p.exists() and p.stat().st_size>0:return
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=240) as r,open(p,"wb") as f:
        while (b:=r.read(1048576)):f.write(b)
zpath=ROOT/"f.zip";fetch("https://github.com/wookat/dataforge-pipelines/releases/download/open-data-sec-form-d/formd-tier1-M-2026-08-05.zip",zpath)
with zipfile.ZipFile(zpath) as z:
    n=next(n for n in z.namelist() if n.endswith("formd_events.parquet"));z.extract(n,ROOT)
cols=["accession","filed_at","quarter","previous_accession","issuer_cik","issuer_name","state","industry_group","investment_fund_type","is_pooled_investment_fund","entity_type","total_amount_sold_usd"]
e=pd.read_parquet(ROOT/n,columns=cols)
e=e[(e.quarter>="2016Q1")&(e.quarter<="2026Q2")].drop_duplicates("accession").reset_index(drop=True)
for c in ["accession","previous_accession","issuer_cik","issuer_name","state","industry_group","investment_fund_type","entity_type"]:e[c]=e[c].fillna("").astype(str)
e["filed_at"]=pd.to_datetime(e.filed_at);e["sold"]=pd.to_numeric(e.total_amount_sold_usd,errors="coerce")
idx={a:i for i,a in enumerate(e.accession)};N=len(e);par=list(range(N))
def find(x):
    while par[x]!=x:par[x]=par[par[x]];x=par[x]
    return x
def union(a,b):
    a,b=find(a),find(b)
    if a!=b:par[b]=a
for i,p in enumerate(e.previous_accession):
    j=idx.get(p)
    if p and j is not None:union(i,j)
e["root"]=[find(i) for i in range(N)]
fp=e[e.sold>0].sort_values("filed_at").drop_duplicates("root").sort_values("filed_at").drop_duplicates("issuer_cik")
fp=fp[(fp.issuer_cik!="")&(fp.filed_at>="2019-01-01")]
US=set('AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC'.split())
u=fp[fp.state.isin(US)].copy()
pooled=u.is_pooled_investment_fund.fillna(False).astype(bool)
base=u[(~pooled)&u.industry_group.str.lower().ne("pooled investment fund")].copy()
patterns={
 "fund":r"\bfunds?\b",
 "investment":r"\binvest(ment|ments|ing)?\b",
 "capital":r"\bcapital\b",
 "ventures":r"\bventures?\b",
 "holdings":r"\bholdings?\b",
 "partners":r"\bpartners?\b",
 "asset":r"\bassets?\b",
 "portfolio":r"\bportfolio\b",
 "spv":r"\bspv\b|special purpose",
 "roman_lp":r"\b[IVX]{1,5}\b.*\b(LP|L\.P\.)\b"
}
out={"base":len(base),"target":61928,"remove_needed":len(base)-61928,
     "industry":base.industry_group.value_counts().to_dict(),
     "entity":base.entity_type.value_counts().to_dict()}
for k,p in patterns.items():
    out["name_"+k]=int(base.issuer_name.str.contains(p,case=False,regex=True,na=False).sum())
# cumulative plausible vehicle token sets
sets={
 "fund_investment": ["fund","investment"],
 "fund_investment_capital":["fund","investment","capital"],
 "fund_investment_capital_holdings":["fund","investment","capital","holdings"],
 "all_vehicle":["fund","investment","capital","ventures","holdings","partners","asset","portfolio","spv","roman_lp"]
}
for k,ks in sets.items():
    m=pd.Series(False,index=base.index)
    for x in ks:m|=base.issuer_name.str.contains(patterns[x],case=False,regex=True,na=False)
    out[k]=int(m.sum());out[k+"_remain"]=int((~m).sum())
# category combinations from obvious financial vehicle classes
cats=["Investing","REITS and Finance","Commercial Banking","Other Banking and Financial Services","Insurance"]
for c in cats:out["cat_"+c]=int((base.industry_group==c).sum())
# all subsets of those categories
from itertools import combinations
for r in range(1,len(cats)+1):
  for cs in combinations(cats,r):
    n=int(base.industry_group.isin(cs).sum())
    if abs((len(base)-n)-61928)<=800:
      out["subset_"+"+".join(cs)]={"remove":n,"remain":len(base)-n}
# cross-tabs for Investing and REITS/Finance
for c in ["Investing","REITS and Finance","Commercial Banking"]:
    sub=base[base.industry_group==c]
    out[c+"_entities"]=sub.entity_type.value_counts().to_dict()
    out[c+"_sample_names"]=sub.issuer_name.head(100).tolist()
json.dump(out,open(OUT/"stage4b-fund-screen.json","w"),indent=2)
print(json.dumps({k:v for k,v in out.items() if not k.endswith("sample_names")},indent=2))
