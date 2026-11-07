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
e=e[e.quarter<="2026Q2"].copy().drop_duplicates("accession",keep="last").reset_index(drop=True)
for c in ["accession","previous_accession"]:e[c]=e[c].fillna("").astype(str)
e["issuer_cik"]=e["issuer_cik"].fillna("").astype(str)
idx={a:i for i,a in enumerate(e.accession)}
parent=list(range(len(e)));rank=[0]*len(e)
def find(x):
    while parent[x]!=x:
        parent[x]=parent[parent[x]];x=parent[x]
    return x
def union(a,b):
    ra,rb=find(a),find(b)
    if ra==rb:return False
    if rank[ra]<rank[rb]:ra,rb=rb,ra
    parent[rb]=ra
    if rank[ra]==rank[rb]:rank[ra]+=1
    return True
orphans=[];cycles=0
for i,r in e.iterrows():
    p=r.previous_accession.strip()
    if not p:continue
    j=idx.get(p)
    if j is None:orphans.append((r.accession,p,r.quarter));continue
    if find(i)==find(j):cycles+=1
    else:union(i,j)
roots=[find(i) for i in range(len(e))]
e["root"]=[roots[i] for i in range(len(e))]
win=e[(e.quarter>="2016Q1")&(e.quarter<="2026Q2")].copy()
# root components represented in analysis window
win_roots=set(win.root)
# CIK consistency evaluated across each represented full-history chain
full_rep=e[e.root.isin(win_roots)].copy()
cik_n=full_rep[full_rep.issuer_cik!=""].groupby("root").issuer_cik.nunique()
bad_roots=set(cik_n[cik_n>1].index)
summary={
 "full_history_rows_to_2026Q2":len(e),
 "window_rows":len(win),"window_D":int((win.submission_type=="D").sum()),"window_DA":int((win.submission_type=="D/A").sum()),
 "window_components_with_full_ancestry":len(win_roots),
 "full_history_orphan_links":len(orphans),"full_history_cycles":cycles,
 "cik_inconsistent_chains":len(bad_roots),
 "window_components_after_dropping_cik_inconsistent":len(win_roots-bad_roots),
 "target_chains":349285,"target_cik_inconsistent":10
}
json.dump(summary,open(OUT/"stage2-chain-check.json","w"),indent=2)
pd.DataFrame(orphans,columns=["accession","previous_accession","quarter"]).to_csv(OUT/"full-history-orphans.csv",index=False)
pd.DataFrame({"root":list(bad_roots)}).to_csv(OUT/"cik-inconsistent-roots.csv",index=False)
print(json.dumps(summary,indent=2))
