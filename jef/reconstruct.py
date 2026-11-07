import json,zipfile,urllib.request
from pathlib import Path
from bisect import bisect_left,bisect_right
import pandas as pd
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
e=pd.read_parquet(ROOT/n,columns=["accession","filed_at","quarter","submission_type","previous_accession","issuer_cik"])
e=e[(e.quarter>="2016Q1")&(e.quarter<="2026Q2")].drop_duplicates("accession",keep="last").reset_index(drop=True)
for c in ["accession","previous_accession","issuer_cik"]:e[c]=e[c].fillna("").astype(str)
e["filed_at"]=pd.to_datetime(e.filed_at)
acc=e.accession.tolist(); prev=e.previous_accession.tolist()
idx={a:i for i,a in enumerate(acc)}
N=len(e); parent=list(range(N));rank=[0]*N
def find(x):
    while parent[x]!=x:
        parent[x]=parent[parent[x]];x=parent[x]
    return x
def union(a,b):
    ra,rb=find(a),find(b)
    if ra==rb:return
    if rank[ra]<rank[rb]:ra,rb=rb,ra
    parent[rb]=ra
    if rank[ra]==rank[rb]:rank[ra]+=1
for i,p in enumerate(prev):
    if p:
        j=idx.get(p)
        if j is not None:union(i,j)
e["comp"]=[find(i) for i in range(N)]
e["isD"]=(e.submission_type=="D").astype(int)
# Fast component metadata
g=e.groupby("comp",sort=False).agg(n=("accession","size"),nD=("isD","sum"),first_date=("filed_at","min"),last_date=("filed_at","max")).reset_index()
first_cik=e[e.issuer_cik!=""].drop_duplicates("comp")[["comp","issuer_cik"]].rename(columns={"issuer_cik":"cik"})
g=g.merge(first_cik,on="comp",how="left");g["cik"]=g.cik.fillna("")
orph=g[g.nD==0].copy()
bycik={}
for r in g[g.cik!=""].itertuples(index=False):
    bycik.setdefault(r.cik,[]).append((pd.Timestamp(r.last_date),int(r.comp)))
for cik in bycik: bycik[cik].sort()
rows=[]
for r in orph.itertuples(index=False):
    items=bycik.get(r.cik,[]); dates=[x[0] for x in items]
    lo=pd.Timestamp(r.first_date)-pd.Timedelta(days=365); hi=pd.Timestamp(r.first_date)-pd.Timedelta(nanoseconds=1)
    a=bisect_left(dates,lo); b=bisect_right(dates,hi)
    cs=[x for x in items[a:b] if x[1]!=int(r.comp)]
    rows.append((int(r.comp),r.cik,str(pd.Timestamp(r.first_date).date()),len(cs),
                 ";".join(str(x[1]) for x in cs),
                 ";".join(str((pd.Timestamp(r.first_date)-x[0]).days) for x in cs)))
R=pd.DataFrame(rows,columns=["comp","cik","first_date","candidate_count_365","candidate_comps","candidate_gaps"])
summary={
 "accession_components":int(len(g)),"orphan_components":int(len(orph)),
 "orphans_with_at_least_one_365_candidate":int((R.candidate_count_365>=1).sum()),
 "orphans_with_exactly_one_365_candidate":int((R.candidate_count_365==1).sum()),
 "orphans_with_multiple_365_candidates":int((R.candidate_count_365>1).sum()),
 "components_after_unique_365_links":int(len(g)-(R.candidate_count_365==1).sum()),
 "target_chains":349285,"target_ambiguous_chains":10
}
json.dump(summary,open(OUT/"stage2-resolution-rule.json","w"),indent=2)
R[R.candidate_count_365>0].to_csv(OUT/"resolved-or-ambiguous-orphans.csv",index=False)
print(json.dumps(summary,indent=2))
