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
e=e[(e.quarter>="2016Q1")&(e.quarter<="2026Q2")].copy()
e=e.drop_duplicates("accession",keep="last").reset_index(drop=True)
for c in ["accession","previous_accession"]:e[c]=e[c].fillna("").astype(str)
e["issuer_cik"]=e["issuer_cik"].fillna("").astype(str)
idx={a:i for i,a in enumerate(e.accession)}
cik=dict(zip(e.accession,e.issuer_cik))

def components(skip_mismatch=False):
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
    orphan=0;mismatch=[];cycles=0;linked=0
    for i,r in e.iterrows():
        p=r.previous_accession.strip()
        if not p:continue
        j=idx.get(p)
        if j is None:
            orphan+=1;continue
        bad=(r.issuer_cik and cik.get(p,"") and r.issuer_cik!=cik[p])
        if bad:
            mismatch.append((r.accession,p,r.issuer_cik,cik[p]))
            if skip_mismatch:continue
        if find(i)==find(j):cycles+=1
        else:union(i,j);linked+=1
    roots=[find(i) for i in range(len(e))]
    return {"components":len(set(roots)),"linked":linked,"orphan":orphan,"cycles":cycles,"mismatch_edges":mismatch,"roots":roots}

a=components(False); b=components(True)
# identify full all-link components containing any mismatch edge
bad_roots=set()
for cur,prev,_,_ in a["mismatch_edges"]:
    bad_roots.add(a["roots"][idx[cur]]);bad_roots.add(a["roots"][idx[prev]])
n_bad_rows=sum(1 for r in a["roots"] if r in bad_roots)
components_after_drop=a["components"]-len(bad_roots)
summary={
 "rows":len(e),"D":int((e.submission_type=="D").sum()),"DA":int((e.submission_type=="D/A").sum()),
 "components_all_links":a["components"],"linked_all":a["linked"],"orphans":a["orphan"],"cycles_all":a["cycles"],
 "cik_mismatch_edges":len(a["mismatch_edges"]),"cik_mismatch_components":len(bad_roots),"rows_in_mismatch_components":n_bad_rows,
 "components_same_cik_only":b["components"],"linked_same_cik":b["linked"],"cycles_same_cik":b["cycles"],
 "components_after_dropping_mismatch_components":components_after_drop,
 "target_chains":349285
}
json.dump(summary,open(OUT/"stage2-chain-check.json","w"),indent=2)
pd.DataFrame(a["mismatch_edges"],columns=["accession","previous_accession","cik","previous_cik"]).to_csv(OUT/"cik-mismatch-edges.csv",index=False)
print(json.dumps(summary,indent=2))
