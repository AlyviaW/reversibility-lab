import json,re,zipfile,urllib.request,io
from pathlib import Path
import pandas as pd, numpy as np

ROOT=Path("jef-run"); RAW=ROOT/"raw"; OUT=ROOT/"output"
RAW.mkdir(parents=True,exist_ok=True); OUT.mkdir(parents=True,exist_ok=True)
UA={"User-Agent":"JEF-RR-replication/1.0"}
def fetch(url,p):
    if p.exists() and p.stat().st_size>0:return
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=240) as r,open(p,"wb") as f:
        while True:
            b=r.read(1048576)
            if not b:break
            f.write(b)
def getcol(df,*alts):
    m={c.upper():c for c in df.columns}
    for a in alts:
        if a.upper() in m:return m[a.upper()]
    return None
def yes(s):
    return s.fillna("").astype(str).str.upper().isin(["Y","YES","TRUE","1"])

# Zenodo inventory
with urllib.request.urlopen(urllib.request.Request("https://zenodo.org/api/records/24042058",headers=UA),timeout=60) as r:
    rec=json.load(r)
zf={x["key"]:x["links"]["self"] for x in rec["files"]}
quarters=[(y,q) for y in range(2016,2026) for q in range(1,5)]+[(2026,1),(2026,2)]

subs=[]; offs=[]; iss=[]
schemas={}
missing=[]
for y,q in quarters:
    canon=f"{y}q{q}_d.zip"
    keys=[k for k in zf if re.fullmatch(fr"{y}q{q}_d(?: \(1\))?\.zip",k)]
    if not keys:
        missing.append(f"{y}Q{q}");continue
    p=RAW/canon; fetch(zf[keys[0]],p)
    with zipfile.ZipFile(p) as z:
        names=z.namelist()
        def read(base):
            n=next(n for n in names if n.upper().split("/")[-1]==base+".TSV")
            with z.open(n) as f:return pd.read_csv(f,sep="\t",dtype=str,keep_default_na=False,low_memory=False)
        S=read("FORMDSUBMISSION");O=read("OFFERING");I=read("ISSUERS")
    if not schemas:
        schemas={"S":list(S.columns),"O":list(O.columns),"I":list(I.columns)}
    for d in (S,O,I):d["SOURCE_QUARTER"]=f"{y}Q{q}"
    subs.append(S);offs.append(O);iss.append(I)

S=pd.concat(subs,ignore_index=True);O=pd.concat(offs,ignore_index=True);I=pd.concat(iss,ignore_index=True)
json.dump({"schemas":schemas,"missing":missing},open(OUT/"raw-schema.json","w"),indent=2)

sa=getcol(S,"ACCESSIONNUMBER"); st=getcol(S,"SUBMISSIONTYPE"); fd=getcol(S,"FILING_DATE","FILINGDATE")
oa=getcol(O,"ACCESSIONNUMBER"); prev=getcol(O,"PREVIOUSACCESSIONNUMBER")
ia=getcol(I,"ACCESSIONNUMBER"); cik=getcol(I,"CIK"); state=getcol(I,"STATEORCOUNTRY","STATE"); yr=getcol(I,"YEAROFINC","YEAROFINCORPORATION")
entity=getcol(I,"ENTITYTYPE")
industry=getcol(O,"INDUSTRYGROUPTYPE"); fundtype=getcol(O,"INVESTMENTFUNDTYPE")
pooled=getcol(O,"ISPOOLEDINVESTMENTFUNDTYPE"); equity=getcol(O,"ISEQUITYTYPE"); debt=getcol(O,"ISDEBTTYPE")
sold=getcol(O,"TOTALAMOUNTSOLD"); target=getcol(O,"TOTALOFFERINGAMOUNT"); investors=getcol(O,"TOTALNUMBERALREADYINVESTED")
sale=getcol(O,"SALE_DATE","DATEOFFIRSTSALE")
print("KEYS",sa,st,fd,oa,prev,ia,cik,state,yr,entity,industry,fundtype,pooled,equity,debt,sold,target,investors,sale)

# primary issuer = first row per accession in official table order
I1=I.drop_duplicates(ia,keep="first")[[ia,cik,state,yr,entity]].copy()
I1.columns=["accession","cik","state","yearinc","entity"]
O1=O.drop_duplicates(oa,keep="first")[[oa,prev,industry,fundtype,pooled,equity,debt,sold,target,investors,sale]].copy()
O1.columns=["accession","previous","industry","fundtype","pooled","equity","debt","sold","target","investors","sale_date"]
B=S[[sa,st,fd,"SOURCE_QUARTER"]].copy()
B.columns=["accession","submission_type","filing_date","quarter"]
B=B.merge(O1,on="accession",how="left").merge(I1,on="accession",how="left")
B["filing_date"]=pd.to_datetime(B.filing_date,errors="coerce")
for c in ["accession","previous","cik","state","yearinc","entity","industry","fundtype"]:
    B[c]=B[c].fillna("").astype(str)
for c in ["sold","target","investors"]:B[c]=pd.to_numeric(B[c],errors="coerce")
B["pooled_b"]=yes(B["pooled"]);B["equity_b"]=yes(B["equity"]);B["debt_b"]=yes(B["debt"])

# Patch missing quarters from full SEC-derived mirror, with same standardized columns.
rp=RAW/"mirror.zip"
fetch("https://github.com/wookat/dataforge-pipelines/releases/download/open-data-sec-form-d/formd-tier1-M-2026-08-05.zip",rp)
with zipfile.ZipFile(rp) as z:
    n=next(n for n in z.namelist() if n.endswith("formd_events.parquet"));z.extract(n,RAW)
M=pd.read_parquet(RAW/n)
M=M[M.quarter.isin(missing)].copy()
P=pd.DataFrame({
 "accession":M.accession.astype(str),
 "submission_type":M.submission_type.astype(str),
 "filing_date":pd.to_datetime(M.filed_at),
 "quarter":M.quarter.astype(str),
 "previous":M.previous_accession.fillna("").astype(str),
 "industry":M.industry_group.fillna("").astype(str),
 "fundtype":M.investment_fund_type.fillna("").astype(str),
 "pooled_b":M.is_pooled_investment_fund.fillna(False).astype(bool),
 "equity_b":M.is_equity.fillna(False).astype(bool),
 "debt_b":M.is_debt.fillna(False).astype(bool),
 "sold":pd.to_numeric(M.total_amount_sold_usd,errors="coerce"),
 "target":pd.to_numeric(M.total_offering_amount_usd,errors="coerce"),
 "investors":pd.to_numeric(M.investors_accepted,errors="coerce"),
 "sale_date":pd.to_datetime(M.date_of_first_sale,errors="coerce"),
 "cik":M.issuer_cik.fillna("").astype(str),
 "state":M.state.fillna("").astype(str),
 "yearinc":M.year_of_incorporation.fillna("").astype(str),
 "entity":M.entity_type.fillna("").astype(str),
})
B=pd.concat([B,P],ignore_index=True).drop_duplicates("accession",keep="last").sort_values("filing_date").reset_index(drop=True)
assert len(B)==538768,(len(B),missing)

# chain by explicit previous accession in analysis window
idx={a:i for i,a in enumerate(B.accession)}
N=len(B);par=list(range(N));rank=[0]*N
def find(x):
    while par[x]!=x:par[x]=par[par[x]];x=par[x]
    return x
def union(a,b):
    a,b=find(a),find(b)
    if a==b:return
    if rank[a]<rank[b]:a,b=b,a
    par[b]=a
    if rank[a]==rank[b]:rank[a]+=1
for i,p in enumerate(B.previous):
    if p:
        j=idx.get(p)
        if j is not None:union(i,j)
B["root"]=[find(i) for i in range(N)]

pos=B[B.sold>0].sort_values("filing_date")
cp=pos.drop_duplicates("root",keep="first")
fp=cp[cp.cik!=""].sort_values("filing_date").drop_duplicates("cik",keep="first")
fp=fp[fp.filing_date>=pd.Timestamp("2019-01-01")].copy()
US=set("AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC".split())
u=fp[fp.state.isin(US)].copy()

# candidate screens from official filing fields
masks={}
masks["pooledflag"]=u.pooled_b
masks["pooled_industry"]=u.industry.str.casefold().eq("pooled investment fund")
masks["fundtype"]=u.fundtype.str.strip().ne("")
masks["investing"]=u.industry.eq("Investing")
masks["investment_banking"]=u.industry.eq("Investment Banking")
masks["reits_finance"]=u.industry.eq("REITS and Finance")
masks["business_trust"]=u.entity.eq("Business Trust")
# Field-only combinations
tests={
 "pool_any":masks["pooledflag"]|masks["pooled_industry"]|masks["fundtype"],
 "pool_plus_investing":masks["pooledflag"]|masks["pooled_industry"]|masks["fundtype"]|masks["investing"],
 "pool_plus_investing_invbank":masks["pooledflag"]|masks["pooled_industry"]|masks["fundtype"]|masks["investing"]|masks["investment_banking"],
 "pool_plus_investing_btrust":masks["pooledflag"]|masks["pooled_industry"]|masks["fundtype"]|masks["investing"]|masks["business_trust"],
}
out={"raw_rows":len(B),"first_positive_2019":len(fp),"us":len(u),"target_us":152898}
for name,rm in tests.items():
    x=u[~rm].copy()
    inc=pd.to_numeric(x.yearinc,errors="coerce")
    young=(x.filing_date.dt.year-inc).between(0,5)
    out[name]={"after":int(len(x)),"young":int(young.sum()),"young_equity":int((young&x.equity_b).sum())}
out["industry_top"]=u.industry.value_counts().head(40).to_dict()
out["fundtype_top"]=u.fundtype.value_counts().head(20).to_dict()
out["entity_top"]=u.entity.value_counts().head(20).to_dict()
out["yearinc_top"]=u.yearinc.value_counts().head(30).to_dict()
json.dump(out,open(OUT/"stage5-raw-waterfall.json","w"),indent=2,default=str)
fp.to_csv(OUT/"raw-first-positive-2019.csv.gz",index=False,compression="gzip")
B[["accession","root","cik","filing_date","quarter","sold","target","investors","state","yearinc","entity","industry","fundtype","pooled_b","equity_b","debt_b","sale_date"]].to_csv(OUT/"raw-events-essentials.csv.gz",index=False,compression="gzip")
print(json.dumps(out,indent=2,default=str))
