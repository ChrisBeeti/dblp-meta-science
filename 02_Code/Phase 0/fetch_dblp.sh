#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="https://sparql.dblp.org/sparql"
OUTDIR="dblp_by_year"
START=1936
END=2015
mkdir -p "$OUTDIR"

for year in $(seq "$START" "$END"); do
  out="$OUTDIR/dblp_${year}.csv"

  # Resume: schon vorhandene, nicht-leere Dateien überspringen
  if [[ -s "$out" ]]; then
    echo "skip  $year (existiert)"
    continue
  fi

  read -r -d '' query <<EOF || true
PREFIX dblp: <https://dblp.org/rdf/schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?publ ?title ?doi ?year ?stream ?venue ?pers ?name ?homepage ?ord ?n WHERE {
  ?publ dblp:yearOfPublication ?year .
  FILTER(?year = "${year}"^^<http://www.w3.org/2001/XMLSchema#gYear>)
  ?publ dblp:publishedInStream ?stream .
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
}
EOF

  curl -sS -X POST "$ENDPOINT" \
    -H "Accept: text/csv" \
    --data-urlencode "query=${query}" \
    -o "$out"

  lines=$(($(wc -l < "$out") - 1))   # minus Header
  echo "hole  $year : ${lines} Zeilen"

  sleep 1   # höflich zum Server, Rate-Limit vermeiden
done

echo "--- fertig ---"
echo "Zeilen gesamt (ohne Header):"
tail -q -n +2 "$OUTDIR"/dblp_*.csv | wc -l