import requests, pandas as pd, pathlib, glob, time

endpoint = "https://sparql.dblp.org/sparql"
outdir = pathlib.Path("dblp_dump"); outdir.mkdir(exist_ok=True)
cols = ["publ","title","doi","year","stream","venue","pers","name","homepage","ord","n"]

base_q = """PREFIX dblp: <https://dblp.org/rdf/schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?publ ?title ?doi ?year ?stream ?venue ?pers ?name ?homepage ?ord ?n WHERE {
  ?publ dblp:publishedInStream ?stream .
  ?publ dblp:yearOfPublication ?year .
  ?publ dblp:numberOfCreators ?n .
  ?publ dblp:title ?title .
  ?publ dblp:hasSignature ?sig .
  ?sig rdf:type dblp:AuthorSignature .
  ?sig dblp:signatureOrdinal ?ord .
  ?sig dblp:signatureCreator ?pers .
  ?publ dblp:doi ?doi .
  ?sig dblp:signatureDblpName ?name .
  OPTIONAL { ?stream dblp:streamTitle ?venue . }
  OPTIONAL { ?pers dblp:homepage ?homepage . }
} ORDER BY ?publ ?ord
LIMIT %d OFFSET %d"""

def fetch(chunk, offset, tries=3):
    q = base_q % (chunk, offset)
    for t in range(tries):
        r = requests.get(endpoint, params={"query": q},
                         headers={"Accept": "application/sparql-results+json"}, timeout=900)
        r.raise_for_status()
        try:
            return r.json()["results"]["bindings"]
        except requests.exceptions.JSONDecodeError:
            print(f"  truncated @offset {offset} (try {t+1}, {len(r.content)/1e6:.0f}MB) -> retry")
            time.sleep(5)
    raise RuntimeError(f"offset {offset}: JSON blieb kaputt nach {tries} tries — chunk kleiner setzen")

chunk = 100_000
done = len(glob.glob(str(outdir / "part_*.parquet")))
offset = done * chunk
if done: print(f"resume ab offset {offset}")

while True:
    b = fetch(chunk, offset)
    if not b:
        break
    part = pd.DataFrame([{k: row.get(k, {}).get("value") for k in cols} for row in b])
    part.to_parquet(outdir / f"part_{offset // chunk:06d}.parquet", index=False)
    print(f"geholt: {offset + len(b):>12,}")
    if len(b) < chunk:
        break
    offset += chunk

parts = sorted(glob.glob(str(outdir / "part_*.parquet")))
df = pd.concat((pd.read_parquet(p) for p in parts), ignore_index=True)
df.to_parquet("dblp_full.parquet", index=False)
print(f"\ntotal: {len(df):,}")