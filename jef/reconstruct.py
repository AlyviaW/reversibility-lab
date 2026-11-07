import json,zipfile,urllib.request
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
e=pd.read_parquet(ROOT/n,columns=["accession","filed_at","quarter","submission_type","previous_accession","issuer_cik","state","country","total_amount_sold_usd"])
e=e[e.quarter<="2026Q2"].drop_duplicates("accession",keep="last").reset_index(drop=True)
for c in ["accession","previous_accession","issuer_cik","state","country"]:e[c]=e[c].fillna("").astype(str)
e["filed_at"]=pd.to_datetime(e.filed_at); e["sold"]=pd.to_numeric(e.total_amount_sold_usd,errors="coerce")
US=set(['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'])

def chain_ids(start):
    h=e[(e.quarter>=start)&(e.quarter<="2026Q2")].copy().reset_index()
    acc=h.accession.tolist(); prev=h.previous_accession.tolist(); idx={a:i for i,a in enumerate(acc)}
    N=len(h); parent=list(range(N)); rank=[0]*N
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
            if j is not None: union(i,j)
    h["root"]=[find(i) for i in range(N)]
    return h

def downstream(start):
    h=chain_ids(start)
    # Only analysis-window filings can define first-positive events, but earlier nodes can connect chains.
    w=h[h.quarter>="2016Q1"].copy()
    pos=w[w.sold>0].copy()
    # first positive filing within each connected chain
    cp=pos.sort_values("filed_at").drop_duplicates("root",keep="first")[["root","filed_at","issuer_cik","state","country"]]
    # chain belongs to issuer CIK at first positive; choose issuer's earliest positive chain
    fp=cp[cp.issuer_cik!=""].sort_values("filed_at").drop_duplicates("issuer_cik",keep="first")
    fp19=fp[fp.filed_at>=pd.Timestamp("2019-01-01")]
    us=fp19[fp19.state.isin(US)]
    return {
      "components_window":int(w.root.nunique()),
      "all_first_positive_issuers":int(len(fp)),
      "first_positive_2019plus":int(len(fp19)),
      "us50dc":int(len(us))
    }
starts=["2016Q1","2015Q1","2014Q1","2013Q1","2012Q1","2011Q2","2011Q1","2010Q1","2009Q1","2008Q1"]
res={s:downstream(s) for s in starts}
json.dump(res,open(OUT/"stage3-downstream-scheme-check.json","w"),indent=2)
print(json.dumps(res,indent=2))
