import io, os, re, json, zipfile, hashlib, urllib.request, time
from pathlib import Path
import pandas as pd

ROOT=Path("jef-run")
RAW=ROOT/"formd"
OUT=ROOT/"output"
RAW.mkdir(parents=True,exist_ok=True); OUT.mkdir(parents=True,exist_ok=True)
UA={"User-Agent":"Qinlei Wang academic research contact via GitHub Actions"}

def get(url,path):
    if path.exists() and path.stat().st_size>0: return
    req=urllib.request.Request(url,headers=UA)
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req,timeout=90) as r, open(path,"wb") as f:
                while True:
                    b=r.read(1024*1024)
                    if not b: break
                    f.write(b)
            return
        except Exception:
            if attempt==4: raise
            time.sleep(2*(attempt+1))

def url_for(y,q):
    if y==2026 and q==2:
        return "https://www.sec.gov/files/datastandardsinnovation/data/form-d-data-sets/2026q2_d.zip"
    return f"https://www.sec.gov/files/structureddata/data/form-d-data-sets/{y}q{q}_d.zip"

quarters=[(y,q) for y in range(2016,2026) for q in range(1,5)] + [(2026,1),(2026,2)]
for y,q in quarters:
    p=RAW/f"{y}q{q}_d.zip"
    get(url_for(y,q),p)
    print("DOWNLOADED",p.name,p.stat().st_size)

# inspect and concatenate key tables
subs=[]; issuers=[]; offers=[]
schema={}
for y,q in quarters:
    p=RAW/f"{y}q{q}_d.zip"
    with zipfile.ZipFile(p) as z:
        names=z.namelist()
        if not schema:
            schema["zip_names"]=names
        def read_member(base):
            cand=[n for n in names if n.upper().split("/")[-1] in {base.upper()+".TSV",base.upper()+".TXT"}]
            if not cand: raise RuntimeError((p,base,names))
            with z.open(cand[0]) as f:
                return pd.read_csv(f,sep="\t",dtype=str,keep_default_na=False,na_values=[],low_memory=False)
        s=read_member("FORMDSUBMISSION"); i=read_member("ISSUERS"); o=read_member("OFFERING")
        if "submission_cols" not in schema:
            schema["submission_cols"]=list(s.columns); schema["issuer_cols"]=list(i.columns); schema["offering_cols"]=list(o.columns)
            schema["submission_sample"]=s.head(3).to_dict("records")
            schema["issuer_sample"]=i.head(3).to_dict("records")
            schema["offering_sample"]=o.head(3).to_dict("records")
        s["SOURCE_Q"]=f"{y}Q{q}"; i["SOURCE_Q"]=f"{y}Q{q}"; o["SOURCE_Q"]=f"{y}Q{q}"
        subs.append(s); issuers.append(i); offers.append(o)

S=pd.concat(subs,ignore_index=True)
I=pd.concat(issuers,ignore_index=True)
O=pd.concat(offers,ignore_index=True)
json.dump(schema,open(OUT/"schema.json","w"),indent=2)

# normalize key column names dynamically
def col(df,*names):
    mp={c.upper():c for c in df.columns}
    for n in names:
        if n.upper() in mp: return mp[n.upper()]
    return None
acc=col(S,"ACCESSIONNUMBER")
stype=col(S,"SUBMISSIONTYPE")
prev=col(S,"PREVIOUSACCESSIONNUMBER")
live=col(S,"ISLIVE","LIVE")
print("KEYS",acc,stype,prev,live)
if live:
    print("LIVE VALUES",S[live].value_counts(dropna=False).to_dict())

# Original paper evidently counts structured submissions; check both all/live if available
counts={
 "raw_rows":int(len(S)),
 "submission_types":S[stype].value_counts(dropna=False).to_dict(),
}
if live: counts["live_rows"]=int((S[live].astype(str).str.upper().isin(["Y","TRUE","1"])).sum())

# issuer CIK keyed by accession
iacc=col(I,"ACCESSIONNUMBER"); cik=col(I,"CIK","ISSUERCIK")
print("ISSUER KEYS",iacc,cik)
imap=I[[iacc,cik]].drop_duplicates(iacc).rename(columns={iacc:"acc",cik:"cik"})
base=S[[acc,stype,prev]].rename(columns={acc:"acc",stype:"stype",prev:"prev"}).copy()
base=base.merge(imap,on="acc",how="left")
base["acc"]=base["acc"].astype(str)
base["prev"]=base["prev"].astype(str)

# DSU within same CIK if previous accession exists in window and CIK consistent
idx={a:j for j,a in enumerate(base["acc"])}
parent=list(range(len(base)))
rank=[0]*len(base)
def find(x):
    while parent[x]!=x:
        parent[x]=parent[parent[x]]; x=parent[x]
    return x
def union(a,b):
    ra,rb=find(a),find(b)
    if ra==rb:return False
    if rank[ra]<rank[rb]: ra,rb=rb,ra
    parent[rb]=ra
    if rank[ra]==rank[rb]:rank[ra]+=1
    return True
cik_by_acc=dict(zip(base.acc,base.cik))
linked=0; prev_missing=0; cik_mismatch=0
for j,r in base.iterrows():
    p=r["prev"]
    if not p or p.lower()=="nan": continue
    if p not in idx: prev_missing+=1; continue
    c1=str(r["cik"]); c0=str(cik_by_acc.get(p,""))
    if c1 and c0 and c1!=c0:
        cik_mismatch+=1
        continue
    if union(j,idx[p]): linked+=1
roots=[find(j) for j in range(len(base))]
counts.update({"linked_edges":linked,"previous_missing":prev_missing,"cik_mismatch_links":cik_mismatch,"components":len(set(roots))})
json.dump(counts,open(OUT/"raw-chain-counts.json","w"),indent=2)
print(json.dumps(counts,indent=2))

# useful categorical values/headers for sample screen reconstruction
diag={}
for label,df in [("S",S),("I",I),("O",O)]:
    for pat in ["FUND","INDUSTRY","ENTITY","EQUITY","DEBT","INVEST","SALE","AMOUNT","INVESTOR","YEAR","STATE","COUNTRY","SECURITY"]:
        cs=[c for c in df.columns if pat in c.upper()]
        if cs: diag[f"{label}_{pat}_columns"]=cs
for c in I.columns:
    if any(k in c.upper() for k in ["INDUSTRY","ENTITY","FUND","COUNTRY","STATE","YEAR"]):
        vc=I[c].value_counts(dropna=False).head(30).to_dict()
        diag[f"I_values_{c}"]=vc
for c in O.columns:
    if any(k in c.upper() for k in ["FUND","INDUSTRY","EQUITY","DEBT","SECURITY","INVESTMENTFUND"]):
        vc=O[c].value_counts(dropna=False).head(30).to_dict()
        diag[f"O_values_{c}"]=vc
json.dump(diag,open(OUT/"screen-diagnostics.json","w"),indent=2,default=str)

# Save merged raw essentials as parquet to avoid re-downloading in next action artifact
S.to_parquet(OUT/"formdsubmission.parquet",index=False)
I.to_parquet(OUT/"issuers.parquet",index=False)
O.to_parquet(OUT/"offering.parquet",index=False)
print("saved parquets",len(S),len(I),len(O))
