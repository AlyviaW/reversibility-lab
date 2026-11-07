import json,zipfile,urllib.request
from pathlib import Path
import pandas as pd
ROOT=Path("jef-run"); OUT=ROOT/"output"; OUT.mkdir(parents=True,exist_ok=True)
UA={"User-Agent":"JEF-RR-replication/1.0"}
def fetch(url,path):
    if path.exists() and path.stat().st_size>0:return
    req=urllib.request.Request(url,headers=UA)
    with urllib.request.urlopen(req,timeout=240) as r, open(path,"wb") as f:
        while True:
            b=r.read(1024*1024)
            if not b:break
            f.write(b)
release=ROOT/"formd-tier1-M-2026-08-05.zip"
fetch("https://github.com/wookat/dataforge-pipelines/releases/download/open-data-sec-form-d/formd-tier1-M-2026-08-05.zip",release)
with zipfile.ZipFile(release) as z:
    n=next(n for n in z.namelist() if n.endswith("formd_events.parquet"))
    z.extract(n,ROOT)
    e=pd.read_parquet(ROOT/n,columns=["accession","filed_at","quarter","submission_type","previous_accession","issuer_cik"])
e=e[(e.quarter>="2016Q1")&(e.quarter<="2026Q2")].copy().drop_duplicates("accession",keep="last").reset_index(drop=True)
for c in ["accession","previous_accession","issuer_cik"]:e[c]=e[c].fillna("").astype(str)
e["filed_at"]=pd.to_datetime(e.filed_at)
idx={a:i for i,a in enumerate(e.accession)}
parent=list(range(len(e)));rank=[0]*len(e)
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
for i,r in e.iterrows():
    j=idx.get(r.previous_accession.strip())
    if j is not None:union(i,j)
e["comp"]=[find(i) for i in range(len(e))]
g=e.groupby("comp").agg(
    n=("accession","size"),
    nD=("submission_type",lambda x:int((x=="D").sum())),
    cik=("issuer_cik",lambda x:next((v for v in x if v), "")),
    cik_n=("issuer_cik",lambda x:len(set(v for v in x if v))),
    first_date=("filed_at","min"),last_date=("filed_at","max")
).reset_index()
orph=g[g.nD==0].copy()
bycik={c:sub.copy() for c,sub in g[g.cik!=""].groupby("cik")}
rows=[]
for _,r in orph.iterrows():
    cand=bycik.get(r.cik)
    if cand is None:
        cs=pd.DataFrame()
    else:
        cs=cand[(cand["comp"]!=r["comp"]) & (cand["last_date"]<r["first_date"])].copy()
        cs["gap"]=(r["first_date"]-cs["last_date"]).dt.days
        cs=cs[(cs.gap>=0)&(cs.gap<=365)]
    rows.append({
      "comp":int(r.comp),"cik":r.cik,"first_date":str(r.first_date.date()),
      "candidate_count_365":int(len(cs)),
      "candidate_comps":";".join(map(str,cs.comp.tolist())) if len(cs) else "",
      "candidate_gaps":";".join(map(str,cs.gap.tolist())) if len(cs) else ""
    })
R=pd.DataFrame(rows)
summary={
 "accession_components":len(g),"orphan_components":len(orph),
 "orphans_with_at_least_one_365_candidate":int((R.candidate_count_365>=1).sum()),
 "orphans_with_exactly_one_365_candidate":int((R.candidate_count_365==1).sum()),
 "orphans_with_multiple_365_candidates":int((R.candidate_count_365>1).sum()),
 "components_after_unique_365_links":int(len(g)-(R.candidate_count_365==1).sum()),
 "target_chains":349285,"target_ambiguous_chains":10
}
json.dump(summary,open(OUT/"stage2-resolution-rule.json","w"),indent=2)
R[R.candidate_count_365>0].to_csv(OUT/"resolved-or-ambiguous-orphans.csv",index=False)
print(json.dumps(summary,indent=2))
