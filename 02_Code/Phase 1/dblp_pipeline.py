#!/usr/bin/env python3
"""
dblp meta-science pipeline
==========================
Verarbeitet die per-Jahr heruntergeladenen dblp-CSVs (~92 Dateien, ~9 GB) mit
DuckDB out-of-core: CSV -> Parquet konsolidieren, dann alle Aggregate
server-seitig rechnen und nur kleine Ergebnistabellen nach pandas holen.

Achsen des Vorhabens:
  - Volumen  : Paper / Autorschaften pro Jahr
  - Position : mittlere relative Position, First-/Last-Author-Anteil, Team-Groesse
  - Venue    : dieselben Positionsmetriken je Venue
  - Karriere : Position in Abhaengigkeit vom akademischen Alter

Pfade sind relativ zum Skript-Ort (02_Code) aufgeloest, nicht zum
Arbeitsverzeichnis -> laeuft auch aus IDE/Scheduler.

Aufruf:
    python dblp_pipeline.py --cap-hint 6000
"""

import argparse
import os
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt

# Skript liegt in 02_Code/... ; ROOT = Projektwurzel eine Ebene darueber
ROOT = Path(__file__).resolve().parents[2]


# --------------------------------------------------------------------------- #
# Setup
# --------------------------------------------------------------------------- #
def get_connection(db_path: str, memory_limit: str, threads: int) -> duckdb.DuckDBPyConnection:
    """Persistente DuckDB-Verbindung; Zwischentabellen ueberleben so Sessions."""
    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute(f"PRAGMA threads={threads}")
    return con


# --------------------------------------------------------------------------- #
# Phase 1 - CSV -> Parquet
# --------------------------------------------------------------------------- #
def build_parquet(con, csv_glob: str, parquet_path: str, force: bool = False) -> None:
    """Konsolidiert alle Jahres-CSVs einmalig in eine typisierte, zstd-Parquet-Datei."""
    if os.path.exists(parquet_path) and not force:
        print(f"[phase1] {parquet_path} existiert -> ueberspringe (--force zum Neubau)")
        return

    print(f"[phase1] lese {csv_glob} -> {parquet_path}")
    con.execute(f"""
        COPY (
            SELECT
                publ, title, doi,
                CAST(year AS INTEGER) AS year,
                stream, venue, pers, name, homepage,
                CAST(ord AS INTEGER)  AS ord,
                CAST(n   AS INTEGER)  AS n
            FROM read_csv_auto('{csv_glob}',
                               union_by_name=true,   -- schuetzt vor Spalten-Drift
                               header=true,
                               sample_size=-1)        -- ganze Datei fuer Typ-Inferenz
        )
        TO '{parquet_path}' (FORMAT parquet, COMPRESSION zstd)
    """)
    print("[phase1] fertig")


# --------------------------------------------------------------------------- #
# Datenqualitaet
# --------------------------------------------------------------------------- #
def quality_check(con, parquet: str, outdir: str, cap_hint: int | None = None):
    """Zeilen/Paper pro Jahr - deckt am Download-Cap gekappte Jahrgaenge auf."""
    q = con.execute(f"""
        SELECT year,
               COUNT(*)               AS rows,
               COUNT(DISTINCT publ)   AS papers,
               COUNT(DISTINCT pers)   AS authors
        FROM '{parquet}'
        GROUP BY year ORDER BY year
    """).df()
    q.to_csv(os.path.join(outdir, "quality_by_year.csv"), index=False)

    print("\n[quality] Zeilen/Paper pro Jahr (Auszug):")
    print(q.tail(15).to_string(index=False))

    if cap_hint:
        # Jahre, die verdaechtig nah am Cap kleben -> evtl. abgeschnitten
        suspect = q[q["rows"] >= cap_hint * 0.98]
        if len(suspect):
            print(f"\n[quality] WARNUNG: {len(suspect)} Jahr(e) nahe Cap {cap_hint} "
                  f"-> evtl. gekappt, feiner nachladen:")
            print(suspect.to_string(index=False))
        else:
            print(f"\n[quality] kein Jahr nahe Cap {cap_hint} - Download wirkt vollstaendig")
    return q


# --------------------------------------------------------------------------- #
# Phase 2 - Analysen
# --------------------------------------------------------------------------- #
def papers_per_year(con, parquet: str):
    return con.execute(f"""
        SELECT year,
               COUNT(DISTINCT publ) AS papers,
               COUNT(*)             AS authorships
        FROM '{parquet}'
        GROUP BY year ORDER BY year
    """).df()


def position_trend(con, parquet: str):
    """Positionsmetriken pro Jahr (nur Mehrautoren-Paper, n>1)."""
    return con.execute(f"""
        SELECT year,
               AVG(ord * 1.0 / n)                       AS mean_rel_pos,
               AVG(CASE WHEN ord = 1 THEN 1 ELSE 0 END) AS first_share,
               AVG(CASE WHEN ord = n THEN 1 ELSE 0 END) AS last_share,
               AVG(n)                                   AS mean_team_size,
               COUNT(*)                                 AS authorships
        FROM '{parquet}'
        WHERE n > 1
        GROUP BY year ORDER BY year
    """).df()


def position_by_venue(con, parquet: str, min_rows: int = 500):
    """Positionsmetriken je Venue (nur Venues mit ausreichend Daten)."""
    return con.execute(f"""
        SELECT venue,
               AVG(n)                                   AS mean_team_size,
               AVG(CASE WHEN ord = n THEN 1 ELSE 0 END) AS last_share,
               COUNT(DISTINCT publ)                     AS papers,
               COUNT(*)                                 AS authorships
        FROM '{parquet}'
        WHERE n > 1 AND venue IS NOT NULL
        GROUP BY venue
        HAVING authorships >= {min_rows}
        ORDER BY papers DESC
    """).df()


def career_trend(con, parquet: str, min_rows: int = 100):
    """Position in Abhaengigkeit vom akademischen Alter (Jahr - erstes Pub-Jahr)."""
    con.execute(f"""
        CREATE OR REPLACE TABLE first_year AS
        SELECT pers, MIN(year) AS first_year
        FROM '{parquet}'
        GROUP BY pers
    """)
    return con.execute(f"""
        SELECT (a.year - f.first_year)                    AS acad_age,
               AVG(CASE WHEN a.ord = 1   THEN 1 ELSE 0 END) AS first_share,
               AVG(CASE WHEN a.ord = a.n THEN 1 ELSE 0 END) AS last_share,
               AVG(a.n)                                     AS mean_team_size,
               COUNT(*)                                     AS authorships
        FROM '{parquet}' a
        JOIN first_year f USING (pers)
        WHERE a.n > 1 AND a.year >= f.first_year
        GROUP BY acad_age
        HAVING authorships >= {min_rows}
        ORDER BY acad_age
    """).df()


# --------------------------------------------------------------------------- #
# Plots
# --------------------------------------------------------------------------- #
def plot_papers(df, outdir):
    ax = df.plot.bar(x="year", y="papers", rot=90, figsize=(16, 5), legend=False)
    ax.set_xlabel("Jahr"); ax.set_ylabel("Anzahl Paper"); ax.set_title("Paper pro Jahr")
    for i, lbl in enumerate(ax.get_xticklabels()):
        if i % 5 != 0:
            lbl.set_visible(False)
    _save(outdir, "papers_per_year.png")


def plot_position(df, outdir):
    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax1.plot(df["year"], df["first_share"], marker=".", label="First-Author-Anteil")
    ax1.plot(df["year"], df["last_share"],  marker=".", label="Last-Author-Anteil")
    ax1.set_xlabel("Jahr"); ax1.set_ylabel("Anteil"); ax1.legend(loc="upper left")
    ax2 = ax1.twinx()
    ax2.plot(df["year"], df["mean_team_size"], color="tab:red", lw=2, label="Ø Team-Groesse")
    ax2.set_ylabel("Ø Autoren / Paper", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")
    ax2.legend(loc="lower right")
    plt.title("Autor-Position und Team-Groesse ueber die Zeit")
    _save(outdir, "position_trend.png")


def plot_career(df, outdir):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df["acad_age"], df["first_share"], marker=".", label="First-Author-Anteil")
    ax.plot(df["acad_age"], df["last_share"],  marker=".", label="Last-Author-Anteil")
    ax.set_xlabel("Akademisches Alter (Jahre seit erster Publikation)")
    ax.set_ylabel("Anteil"); ax.legend()
    ax.set_title("Autor-Position nach Karrierestatus")
    _save(outdir, "career_trend.png")


def _save(outdir, name):
    plt.tight_layout()
    path = os.path.join(outdir, name)
    plt.savefig(path, dpi=120)
    plt.close()
    print(f"[plot] {path}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-glob", default=str(ROOT / "01_Data/dblp_data/dblp_*.csv"))
    ap.add_argument("--parquet",  default=str(ROOT / "01_Data/dblp.parquet"))
    ap.add_argument("--db",       default=str(ROOT / "01_Data/dblp.duckdb"))
    ap.add_argument("--outdir",   default=str(ROOT / "03_Results"))
    ap.add_argument("--memory-limit", default="6GB")
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 4)
    ap.add_argument("--cap-hint", type=int, default=None,
                    help="Download-Cap pro CSV (z.B. 6000) fuer Qualitaets-Warnung")
    ap.add_argument("--force", action="store_true", help="Parquet neu bauen")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs(os.path.dirname(args.db), exist_ok=True)  # NEU: 01_Data sicherstellen
    con = get_connection(args.db, args.memory_limit, args.threads)

    # Phase 1
    build_parquet(con, args.csv_glob, args.parquet, force=args.force)

    # Qualitaet
    quality_check(con, args.parquet, args.outdir, cap_hint=args.cap_hint)

    # Phase 2 - Analysen + Plots
    for name, df, plotter in [
        ("papers_per_year", papers_per_year(con, args.parquet), plot_papers),
        ("position_trend",  position_trend(con, args.parquet),  plot_position),
        ("career_trend",    career_trend(con, args.parquet),    plot_career),
    ]:
        df.to_csv(os.path.join(args.outdir, f"{name}.csv"), index=False)
        plotter(df, args.outdir)

    # Venue-Tabelle (ohne Standard-Plot; zu viele Venues)
    position_by_venue(con, args.parquet).to_csv(
        os.path.join(args.outdir, "position_by_venue.csv"), index=False)

    con.close()
    print(f"\nfertig. Ergebnisse in {args.outdir}/")


if __name__ == "__main__":
    main()