import json,zipfile,urllib.request,collections
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
    e=pd.read_parquet(ROOT/n,columns=["accession","quarter","previous_accession"])
e=e[e.quarter<="2026Q2"].copy().drop_duplicates("accession",keep="last").reset_index(drop=True)
e["accession"]=e.accession.fillna("").astype(str); e["previous_accession"]=e.previous_accession.fillna("").astype(str)
idx={a:i for i,a in enumerate(e.accession)}
children=collections.defaultdict(list)
for i,p in enumerate(e.previous_accession):
    if p in idx: children[idx[p]].append(i)

N=len(e); parent=list(range(N)); rank=[0]*N; active=[False]*N; haswin=[False]*N
wincomp=0
def find(x):
    while parent[x]!=x:
        parent[x]=parent[parent[x]];x=parent[x]
    return x
def union(a,b):
    global wincomp
    ra,rb=find(a),find(b)
    if ra==rb:return
    wa,wb=haswin[ra],haswin[rb]
    if rank[ra]<rank[rb]:ra,rb=rb,ra;wa,wb=wb,wa
    # merging two components that both contain window observations reduces represented components by 1
    if wa and wb: wincomp-=1
    parent[rb]=ra; haswin[ra]=wa or wb
    if rank[ra]==rank[rb]:rank[ra]+=1

# activate by descending quarter. Record count after all nodes >= start are active.
quarters=sorted(e.quarter.unique(),reverse=True)
res={}
for q in quarters:
    ids=e.index[e.quarter==q].tolist()
    # activate nodes first
    for i in ids:
        active[i]=True
        if "2016Q1"<=e.at[i,"quarter"]<="2026Q2":
            haswin[i]=True;wincomp+=1
    # union each added node to active parent and active children
    for i in ids:
        p=e.at[i,"previous_accession"]; j=idx.get(p)
        if j is not None and active[j]:union(i,j)
        for ch in children.get(i,[]):
            if active[ch]:union(i,ch)
    if q<="2016Q1":
        res[q]=wincomp

closest=sorted(res.items(),key=lambda kv:abs(kv[1]-349285))[:30]
summary={"target":349285,"closest":closest,"counts_by_history_start":res}
json.dump(summary,open(OUT/"stage2-cutoff-sweep.json","w"),indent=2)
print(json.dumps({"target":349285,"closest":closest},indent=2))
