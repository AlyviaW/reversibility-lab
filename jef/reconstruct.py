import io, os, re, json, zipfile, urllib.request
from pathlib import Path
import pandas as pd

ROOT=Path("jef-run"); RAW=ROOT/"formd"; OUT=ROOT/"output"
RAW.mkdir(parents=True,exist_ok=True); OUT.mkdir(parents=True,exist_ok=True)
UA={"User-Agent":"JEF-RR-replication/1.0"}

def fetch(url,path):
    if path.exists() and path.stat().st_size>0:return
    req=urllib.request.Request(url,headers=UA)
    with urllib.request.urlopen(req,timeout=180) as r, open(path,"wb") as f:
        while True:
            b=r.read(1024*1024)
            if not b:break
            f.write(b)

req=urllib.request.Request("https://zenodo.org/api/records/24042058",headers=UA)
with urllib.request.urlopen(req,timeout=60) as r: rec=json.load(r)
files={f["key"]:f["links"]["self"] for f in rec.get("files",[])}

quarters=[(y,q) for y in range(2016,2026) for q in range(1,5)]+[(2026,1),(2026,2)]
frames=[]
missing=[]
for y,q in quarters:
    canonical=f"{y}q{q}_d.zip"
    matches=[k for k in files if re.fullmatch(fr"{y}q{q}_d(?: \(1\))?\.zip",k)]
    if not matches:
        missing.append(f"{y}Q{q}"); continue
    p=RAW/canonical; fetch(files[matches[0]],p)
    with zipfile.ZipFile(p) as z:
        n=next(n for n in z.namelist() if n.upper().split("/")[-1]=="FORMDSUBMISSION.TSV")
        with z.open(n) as f:s=pd.read_csv(f,sep="\t",dtype=str,keep_default_na=False,low_memory=False)
    cols={c.upper():c for c in s.columns}
    x=s[[cols["ACCESSIONNUMBER"],cols["SUBMISSIONTYPE"],cols["PREVIOUSACCESSIONNUMBER"]]].copy()
    x.columns=["accession","submission_type","previous_accession"]
    x["quarter"]=f"{y}Q{q}"
    frames.append(x)

# Missing official quarters from independently reconstructed full SEC stream.
release=ROOT/"formd-tier1-M-2026-08-05.zip"
fetch("https://github.com/wookat/dataforge-pipelines/releases/download/open-data-sec-form-d/formd-tier1-M-2026-08-05.zip",release)
with zipfile.ZipFile(release) as z:
    n=next(n for n in z.namelist() if n.endswith("formd_events.parquet"))
    z.extract(n,ROOT)
    e=pd.read_parquet(ROOT/n,columns=["accession","submission_type","previous_accession","quarter","issuer_cik"])
patch=e[e["quarter"].isin(missing)].copy()
frames.append(patch[["accession","submission_type","previous_accession","quarter"]])

S=pd.concat(frames,ignore_index=True)
for c in ["accession","previous_accession","submission_type"]: S[c]=S[c].fillna("").astype(str)
S=S.drop_duplicates("accession",keep="last").reset_index(drop=True)
idx={a:i for i,a in enumerate(S.accession)}
parent=list(range(len(S))); rank=[0]*len(S)
def find(x):
    while parent[x]!=x:
        parent[x]=parent[parent[x]]; x=parent[x]
    return x
def union(a,b):
    ra,rb=find(a),find(b)
    if ra==rb:return False
    if rank[ra]<rank[rb]:ra,rb=rb,ra
    parent[rb]=ra
    if rank[ra]==rank[rb]:rank[ra]+=1
    return True
linked=0; orphan=0; selflinks=0; cycles=0
for i,r in S.iterrows():
    p=r.previous_accession.strip()
    if not p:continue
    j=idx.get(p)
    if j is None:
        orphan+=1;continue
    if j==i:selflinks+=1;continue
    if find(i)==find(j):
        cycles+=1
    else:
        union(i,j); linked+=1
roots=[find(i) for i in range(len(S))]
components=len(set(roots))
# amendment-specific orphan count
am=S[S.submission_type=="D/A"]
orph_am=sum(1 for p in am.previous_accession if p.strip() and p.strip() not in idx)
blank_prev_am=sum(1 for p in am.previous_accession if not p.strip())

summary={
 "rows":len(S),"D":int((S.submission_type=="D").sum()),"DA":int((S.submission_type=="D/A").sum()),
 "linked_edges":linked,"components_all":components,"target_reported_chains":349285,
 "difference_components_vs_target":components-349285,
 "orphan_previous_all":orphan,"orphan_amendments":orph_am,"blank_previous_amendments":blank_prev_am,
 "self_links":selflinks,"cycle_edges":cycles
}
json.dump(summary,open(OUT/"stage2-chain-check.json","w"),indent=2)
print(json.dumps(summary,indent=2))
# save accession graph compactly for next stage
S.to_csv(OUT/"submission-linkage.csv.gz",index=False,compression="gzip")
