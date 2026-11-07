import json, zipfile, urllib.request, re
from pathlib import Path
from itertools import combinations
import pandas as pd, numpy as np

ROOT=Path("jef-run"); OUT=ROOT/"output"; OUT.mkdir(parents=True,exist_ok=True)
UA={"User-Agent":"JEF-RR-replication/1.0"}
def fetch(url,p):
    if p.exists() and p.stat().st_size>0:return
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=240) as r,open(p,"wb") as f:
        while True:
            b=r.read(1048576)
            if not b: break
            f.write(b)

zpath=ROOT/"f.zip"
fetch("https://github.com/wookat/dataforge-pipelines/releases/download/open-data-sec-form-d/formd-tier1-M-2026-08-05.zip",zpath)
with zipfile.ZipFile(zpath) as z:
    n=next(n for n in z.namelist() if n.endswith("formd_events.parquet"))
    z.extract(n,ROOT)

cols=["accession","filed_at","quarter","previous_accession","issuer_cik","issuer_name","state",
      "industry_group","investment_fund_type","is_pooled_investment_fund","entity_type",
      "total_amount_sold_usd","year_of_incorporation","is_equity"]
e=pd.read_parquet(ROOT/n,columns=cols)
e=e[(e.quarter>="2016Q1")&(e.quarter<="2026Q2")].drop_duplicates("accession").reset_index(drop=True)
for c in ["accession","previous_accession","issuer_cik","issuer_name","state","industry_group","investment_fund_type","entity_type","year_of_incorporation"]:
    e[c]=e[c].fillna("").astype(str)
e["filed_at"]=pd.to_datetime(e.filed_at)
e["sold"]=pd.to_numeric(e.total_amount_sold_usd,errors="coerce")

idx={a:i for i,a in enumerate(e.accession)}
N=len(e); par=list(range(N)); rank=[0]*N
def find(x):
    while par[x]!=x:
        par[x]=par[par[x]]; x=par[x]
    return x
def union(a,b):
    a,b=find(a),find(b)
    if a==b:return
    if rank[a]<rank[b]:a,b=b,a
    par[b]=a
    if rank[a]==rank[b]:rank[a]+=1
for i,p in enumerate(e.previous_accession):
    if p:
        j=idx.get(p)
        if j is not None: union(i,j)
e["root"]=[find(i) for i in range(N)]

fp=e[e.sold>0].sort_values("filed_at").drop_duplicates("root")
fp=fp[fp.issuer_cik!=""].sort_values("filed_at").drop_duplicates("issuer_cik")
fp=fp[fp.filed_at>=pd.Timestamp("2019-01-01")].copy()
US=set("AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC".split())
u=fp[fp.state.isin(US)].copy()
pooled=u.is_pooled_investment_fund.fillna(False).astype(bool)
base=u[(~pooled)&u.industry_group.str.lower().ne("pooled investment fund")].copy()

inc=pd.to_numeric(base.year_of_incorporation,errors="coerce")
young=(base.filed_at.dt.year-inc).between(0,5)
eq=base.is_equity.fillna(False).astype(bool)

atoms={
 "industry Investing":base.industry_group.eq("Investing"),
 "industry Investment Banking":base.industry_group.eq("Investment Banking"),
 "industry REITS and Finance":base.industry_group.eq("REITS and Finance"),
 "industry Commercial Banking":base.industry_group.eq("Commercial Banking"),
 "name SPV":base.issuer_name.str.contains(r"\bSPV\b|SPECIAL PURPOSE",case=False,regex=True,na=False),
 "name sidecar":base.issuer_name.str.contains(r"\bSIDECAR\b",case=False,regex=True,na=False),
 "name feeder":base.issuer_name.str.contains(r"\bFEEDER\b",case=False,regex=True,na=False),
 "name co-invest":base.issuer_name.str.contains(r"CO[- ]?INVEST",case=False,regex=True,na=False),
 "name fund":base.issuer_name.str.contains(r"\bFUNDS?\b",case=False,regex=True,na=False),
 "name investment":base.issuer_name.str.contains(r"\bINVEST(MENT|MENTS|ING)?\b",case=False,regex=True,na=False),
 "name capital":base.issuer_name.str.contains(r"\bCAPITAL\b",case=False,regex=True,na=False),
 "name holdings":base.issuer_name.str.contains(r"\bHOLDINGS?\b",case=False,regex=True,na=False),
 "name partners":base.issuer_name.str.contains(r"\bPARTNERS?\b",case=False,regex=True,na=False),
 "entity Business Trust":base.entity_type.eq("Business Trust"),
}
T=(61928,55874,46875)
ranked=[]
keys=list(atoms)
for rsz in range(1,5):
    for ks in combinations(keys,rsz):
        m=pd.Series(False,index=base.index)
        for k in ks:m |= atoms[k]
        keep=~m
        vals=(int(keep.sum()),int((keep&young).sum()),int((keep&young&eq).sum()))
        score=sum(abs(vals[i]-T[i]) for i in range(3))
        ranked.append((score,ks,vals,int(m.sum())))
ranked.sort(key=lambda x:x[0])

out={
 "base_counts":[int(len(base)),int(young.sum()),int((young&eq).sum())],
 "target":T,
 "best":[{"score":x[0],"flags":list(x[1]),"counts":list(x[2]),"removed":x[3]} for x in ranked[:60]]
}
json.dump(out,open(OUT/"stage4c-joint-screen-search.json","w"),indent=2)
print(json.dumps(out,indent=2))
