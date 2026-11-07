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
    e=pd.read_parquet(ROOT/n,columns=["accession","quarter","submission_type","previous_accession","issuer_cik"])
e=e[e.quarter<="2026Q2"].copy().drop_duplicates("accession",keep="last")
for c in ["accession","previous_accession"]:e[c]=e[c].fillna("").astype(str)

def calc(start):
    h=e[e.quarter>=start].copy().reset_index(drop=True)
    idx={a:i for i,a in enumerate(h.accession)}
    parent=list(range(len(h)));rank=[0]*len(h)
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
    for i,r in h.iterrows():
        p=r.previous_accession.strip()
        if p in idx:union(i,idx[p])
    roots=[find(i) for i in range(len(h))]
    h["root"]=roots
    w=h[(h.quarter>="2016Q1")&(h.quarter<="2026Q2")]
    return len(set(w.root))

starts=[]
for y in range(2008,2017):
    for q in range(1,5):
        s=f"{y}Q{q}"
        if s>"2016Q1":continue
        starts.append(s)
res={s:calc(s) for s in starts}
closest=sorted(res.items(),key=lambda kv:abs(kv[1]-349285))[:20]
summary={"target":349285,"closest":closest,"all":res}
json.dump(summary,open(OUT/"stage2-cutoff-sweep.json","w"),indent=2)
print(json.dumps({"target":349285,"closest":closest},indent=2))
