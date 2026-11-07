import io, os, re, json, zipfile, urllib.request, time, pathlib
from pathlib import Path
import pandas as pd

ROOT=Path("jef-run"); RAW=ROOT/"formd"; OUT=ROOT/"output"
RAW.mkdir(parents=True,exist_ok=True); OUT.mkdir(parents=True,exist_ok=True)
UA={"User-Agent":"JEF-RR-replication/1.0"}

def fetch(url,path):
    if path.exists() and path.stat().st_size>0: return
    req=urllib.request.Request(url,headers=UA)
    with urllib.request.urlopen(req,timeout=120) as r, open(path,"wb") as f:
        while True:
            b=r.read(1024*1024)
            if not b: break
            f.write(b)

# Zenodo record inventory
req=urllib.request.Request("https://zenodo.org/api/records/24042058",headers=UA)
with urllib.request.urlopen(req,timeout=60) as r:
    rec=json.load(r)
files={f["key"]:f["links"]["self"] for f in rec.get("files",[])}

quarters=[(y,q) for y in range(2016,2026) for q in range(1,5)] + [(2026,1),(2026,2)]
missing=[]
present=[]
for y,q in quarters:
    canonical=f"{y}q{q}_d.zip"
    matches=[k for k in files if re.fullmatch(fr"{y}q{q}_d(?: \(1\))?\.zip",k)]
    if not matches:
        missing.append(canonical); continue
    key=matches[0]
    path=RAW/canonical
    fetch(files[key],path)
    present.append((canonical,key,path.stat().st_size))
    print("ZENODO",canonical,"<-",key,path.stat().st_size)

# Count raw submission rows and submission type in available quarters
summary={"zenodo_present":len(present),"missing":missing}
all_sub=[]
for canonical,key,_ in present:
    p=RAW/canonical
    with zipfile.ZipFile(p) as z:
        names=z.namelist()
        member=next(n for n in names if n.upper().split("/")[-1]=="FORMDSUBMISSION.TSV")
        with z.open(member) as f:
            s=pd.read_csv(f,sep="\t",dtype=str,keep_default_na=False,low_memory=False)
        s["SOURCE_Q"]=canonical[:6].upper()
        all_sub.append(s)
S=pd.concat(all_sub,ignore_index=True)
stype=next(c for c in S.columns if c.upper()=="SUBMISSIONTYPE")
summary["zenodo_rows"]=int(len(S))
summary["zenodo_types"]={str(k):int(v) for k,v in S[stype].value_counts(dropna=False).items()}
summary["target_total"]=538768
summary["target_original"]=334780
summary["target_amendments"]=203988
summary["required_missing_rows"]=538768-len(S)
summary["required_missing_D"]=334780-int((S[stype]=="D").sum())
summary["required_missing_DA"]=203988-int((S[stype]=="D/A").sum())

# Download public full longitudinal release, used ONLY to inspect the two missing official quarters
release=ROOT/"formd-tier1-M-2026-08-05.zip"
fetch("https://github.com/wookat/dataforge-pipelines/releases/download/open-data-sec-form-d/formd-tier1-M-2026-08-05.zip",release)
print("RELEASE",release.stat().st_size)
with zipfile.ZipFile(release) as z:
    names=z.namelist()
    summary["release_members"]=names
    pq=[n for n in names if n.endswith("formd_events.parquet")]
    if not pq: pq=[n for n in names if n.endswith(".parquet") and "events" in n.lower()]
    if pq:
        z.extract(pq[0],ROOT)
        ep=ROOT/pq[0]
        e=pd.read_parquet(ep)
        summary["release_columns"]=list(e.columns)
        qcol="quarter" if "quarter" in e.columns else None
        tcol="submission_type" if "submission_type" in e.columns else None
        if qcol:
            patch=e[e[qcol].isin(["2025Q2","2025Q4"])].copy()
            summary["mirror_missing_rows"]=int(len(patch))
            summary["mirror_by_quarter"]={str(k):int(v) for k,v in patch[qcol].value_counts().items()}
            if tcol:
                summary["mirror_types"]={str(k):int(v) for k,v in patch[tcol].value_counts().items()}
                summary["combined_rows"]=int(len(S)+len(patch))
                summary["combined_D"]=int((S[stype]=="D").sum()+(patch[tcol]=="D").sum())
                summary["combined_DA"]=int((S[stype]=="D/A").sum()+(patch[tcol]=="D/A").sum())
                summary["exact_total_match"]=summary["combined_rows"]==538768
                summary["exact_type_match"]=summary["combined_D"]==334780 and summary["combined_DA"]==203988

json.dump(summary,open(OUT/"stage1-count-check.json","w"),indent=2,default=str)
print(json.dumps(summary,indent=2,default=str))
