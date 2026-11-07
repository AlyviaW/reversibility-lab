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
    nDA=("submission_type",lambda x:int((x=="D/A").sum())),
    cik=("issuer_cik",lambda x:next((v for v in x if v), "")),
    cik_n=("issuer_cik",lambda x:len(set(v for v in x if v))),
    first_date=("filed_at","min"),
    last_date=("filed_at","max")
).reset_index()
orph=g[g.nD==0].copy()
normal=g[g.nD>0].copy()
# same-CIK existence in another component
cik_groups=g[g.cik!=""].groupby("cik").comp.apply(list).to_dict()
orph["same_cik_other"]=orph.apply(lambda r: len(cik_groups.get(r.cik,[]))>1,axis=1)
# specifically any earlier component same CIK
meta=g.set_index("comp")
def earlier_candidate(r):
    comps=[c for c in cik_groups.get(r.cik,[]) if c!=r.comp]
    return [c for c in comps if meta.loc[c,"last_date"] < r.first_date]
orph["has_earlier_same_cik"]=orph.apply(lambda r:len(earlier_candidate(r))>0,axis=1)
# nearest earlier component gap
def gapdays(r):
    cs=earlier_candidate(r)
    if not cs:return None
    last=max(meta.loc[c,"last_date"] for c in cs)
    return (r.first_date-last).days
orph["gap_days"]=orph.apply(gapdays,axis=1)
summary={
 "components_accession_only":len(g),
 "orphan_components_no_D":len(orph),
 "components_with_D":len(normal),
 "orphan_with_same_cik_other_component":int(orph.same_cik_other.sum()),
 "orphan_with_earlier_same_cik_component":int(orph.has_earlier_same_cik.sum()),
 "target_merges_needed":len(g)-349285,
 "target_orphan_components_remaining":349285-len(normal),
 "orphans_by_gap_le_30":int((orph.gap_days<=30).fillna(False).sum()),
 "orphans_by_gap_le_90":int((orph.gap_days<=90).fillna(False).sum()),
 "orphans_by_gap_le_365":int((orph.gap_days<=365).fillna(False).sum()),
 "orphans_by_gap_any":int(orph.gap_days.notna().sum()),
 "multi_cik_accession_components":int((g.cik_n>1).sum())
}
json.dump(summary,open(OUT/"stage2-cik-resolution.json","w"),indent=2)
orph.to_csv(OUT/"orphan-components.csv",index=False)
print(json.dumps(summary,indent=2))
