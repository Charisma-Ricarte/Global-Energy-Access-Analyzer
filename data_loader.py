# data_loader.py
import csv
import os
import sqlite3
from collections import defaultdict
from typing import Dict

import db_designer

# filenames (case-insensitive)
CSV_POP = "population_dataset.csv"
CSV_ELEC = "electricity_dataset.csv"

# years to import from population (1990..2016 inclusive)
POP_YEARS = list(range(1990, 2017))
DB = "electricity_access.db"
BATCH_COMMIT = 500

AGGREGATE_KEYWORDS = [
    "income", "world", "union", "asia", "africa", "america", "states",
    "europe", "caribbean", "pacific", "oecd", "ida", "ibrd", "hipc",
    "demographic", "classification", "fragile", "only", "total",
    "middle east", "sub-saharan", "lower middle", "upper middle",
    "late-", "early-", "pre-", "post-", "region"
]

def find_file_case_insensitive(name):
    if os.path.exists(name):
        return name
    for f in os.listdir("."):
        if f.lower() == name.lower():
            return f
    return None

def is_aggregate(name: str) -> bool:
    if not name:
        return True
    n = name.lower()
    return any(k in n for k in AGGREGATE_KEYWORDS)

def load_population_csv(path: str) -> Dict[str, Dict[int, int]]:
    pop_cache = defaultdict(dict)
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        available_years = [int(h) for h in reader.fieldnames if h.isdigit()]
        desired_years = [y for y in POP_YEARS if y in available_years]
        for row in reader:
            code = (row.get("Country Code") or "").strip()
            if not code:
                continue
            for y in desired_years:
                val = row.get(str(y))
                if val is None or val == "":
                    pop_cache[code][y] = None
                else:
                    try:
                        pop_cache[code][y] = int(float(val))
                    except Exception:
                        pop_cache[code][y] = None
    return pop_cache

def load_electricity_csv(path: str):
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        # detect pwe column by looking for 'without' in header
        pwe_col = None
        for h in reader.fieldnames:
            if h and "without" in h.lower():
                pwe_col = h
                break
        if not pwe_col:
            # fallback to known header or long header that mentions access/electricity
            fallback = "Number of people without access to electricity (people without electricity access)"
            if fallback in reader.fieldnames:
                pwe_col = fallback
            else:
                for h in reader.fieldnames:
                    if h and ("access" in h.lower() or "electricity" in h.lower()):
                        pwe_col = h
                        break
        return rows, pwe_col

def upsert_population_rows(conn, country_cache, pop_cache):
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    ptable = schema["pop_table"]
    if not ptable:
        return
    code_to_cids = {}
    for ent, (cid, code) in country_cache.items():
        if code:
            code_to_cids.setdefault(code, []).append(cid)
    rows_written = 0
    for code, year_map in pop_cache.items():
        if not year_map:
            continue
        cids = code_to_cids.get(code, [])
        if not cids:
            continue
        for cid in cids:
            for yr, popv in year_map.items():
                if yr not in POP_YEARS:
                    continue
                if popv is None:
                    continue
                cur.execute(
                    f"""
                    INSERT INTO {ptable} (country_id, year, population)
                    VALUES (?, ?, ?)
                    ON CONFLICT(country_id, year)
                    DO UPDATE SET population = excluded.population
                    """,
                    (cid, yr, popv)
                )
                rows_written += 1
                if rows_written % BATCH_COMMIT == 0:
                    conn.commit()
    conn.commit()

def load_data():
    print("=== Starting loader ===")
    db_designer.init_db()
    pop_file = find_file_case_insensitive(CSV_POP)
    elec_file = find_file_case_insensitive(CSV_ELEC)
    if not elec_file:
        print(f"electricity CSV not found ({CSV_ELEC}) - put it in this folder and re-run.")
        return
    if not pop_file:
        print(f"population CSV not found ({CSV_POP}) - loader will continue but population may be missing.")
        pop_file = None

    pop_cache = {}
    if pop_file:
        print("Loading population CSV into memory (1990-2016)...")
        pop_cache = load_population_csv(pop_file)
        print(f"Loaded population codes: {len(pop_cache)}")

    print("Loading electricity CSV...")
    elec_rows, pwe_col = load_electricity_csv(elec_file)
    print(f"Found {len(elec_rows)} electricity rows; detected pwe column: {pwe_col}")

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    ctbl = schema["countries_table"]
    etbl = schema["elec_table"]
    ptable = schema["pop_table"]
    ccols = schema["countries_cols"]
    ecols = schema["elec_cols"]
    pop_cols = schema["pop_cols"]
    cname_col = next((c for c in ccols if "name" in c.lower()), ccols[1] if len(ccols)>1 else ccols[0])
    cid_col = next((c for c in ccols if "id" in c.lower()), ccols[0])

    # ensure canonical exists
    db_designer.init_db()
    schema = db_designer._detect_schema()
    etbl = schema["elec_table"]
    ptable = schema["pop_table"]
    ecols = schema["elec_cols"]
    pop_cols = schema["pop_cols"]

    # build country cache for entities present in electricity CSV
    country_cache = {}
    entities = []
    for r in elec_rows:
        ent = (r.get("Entity") or "").strip()
        if not ent:
            ent = "NONE"
        if ent not in entities:
            entities.append(ent)

    for ent in entities:
        # skip aggregates
        if is_aggregate(ent):
            continue
        sample_code = ""
        for r in elec_rows:
            if (r.get("Entity") or "").strip() == ent:
                sample_code = (r.get("Code") or "").strip()
                if sample_code:
                    break
        cur.execute(f"SELECT {cid_col} FROM {ctbl} WHERE {cname_col}=?", (ent,))
        q = cur.fetchone()
        if q:
            cid = q[0]
        else:
            cur.execute(f"INSERT OR IGNORE INTO {ctbl} ({cname_col}, region) VALUES (?, ?)", (ent, None))
            cid = cur.lastrowid
        country_cache[ent] = (cid, sample_code)
    conn.commit()

    # upsert population rows from preloaded pop_cache
    if pop_cache:
        print("Upserting population rows into PopulationData...")
        upsert_population_rows(conn, country_cache, pop_cache)
        print("Population upsert complete.")

    # fields detection
    year_col = next((c for c in ecols if c.lower()=="year"), "year")
    pop_col = next((c for c in ecols if "population" in c.lower()), "population")
    pwe_col_db = next((c for c in ecols if "without" in c.lower()), "people_without_electricity")
    pwith_col_db = next((c for c in ecols if "with" in c.lower()), "people_with_electricity")
    country_fk = next((c for c in ecols if "country" in c.lower()), "country_id")

    # population table columns
    pop_table_cid = next((c for c in pop_cols if "country" in c.lower()), "country_id")
    pop_table_year = next((c for c in pop_cols if c.lower() == "year"), "year")
    pop_table_val = next((c for c in pop_cols if "pop" in c.lower()), "population")

    total = len(elec_rows)
    for i, r in enumerate(elec_rows):
        ent = (r.get("Entity") or "").strip() or "NONE"
        if is_aggregate(ent):
            continue
        code = (r.get("Code") or "").strip()
        try:
            year = int(r.get("Year"))
        except Exception:
            continue
        try:
            pwe_val_raw = r.get(pwe_col)
            pwe = int(float(pwe_val_raw)) if (pwe_val_raw not in (None, "")) else 0
        except Exception:
            pwe = 0

        cid, sample_code = country_cache.get(ent, (None, code))
        if cid is None:
            cur.execute(f"INSERT INTO {ctbl} ({cname_col}, region) VALUES (?, ?)", (ent, None))
            cid = cur.lastrowid
            country_cache[ent] = (cid, code)

        popv = None
        if code and code in pop_cache:
            popv = pop_cache[code].get(year)
        else:
            if ptable:
                cur.execute(f"SELECT {pop_table_val} FROM {ptable} WHERE {pop_table_cid}=? AND {pop_table_year}=?", (cid, year))
                q = cur.fetchone()
                if q:
                    popv = q[0]

        pwith = None
        if popv is not None:
            try:
                pwith_calc = int(popv) - int(pwe)
                if pwith_calc >= 0:
                    pwith = pwith_calc
                else:
                    pwith = 0
            except Exception:
                pwith = None

        # upsert to ElectricityAccess using ON CONFLICT logic (safe for both SQLite versions)
        if pwith is not None and popv is not None:
            cur.execute(
                f"""
                INSERT INTO {etbl} ({country_fk}, {year_col}, {pop_col}, {pwe_col_db}, {pwith_col_db})
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT({country_fk}, {year_col})
                DO UPDATE SET
                    {pop_col}=excluded.{pop_col},
                    {pwe_col_db}=excluded.{pwe_col_db},
                    {pwith_col_db}=excluded.{pwith_col_db}
                """,
                (cid, year, popv, pwe, pwith)
            )
        elif popv is not None:
            cur.execute(
                f"""
                INSERT INTO {etbl} ({country_fk}, {year_col}, {pop_col}, {pwe_col_db})
                VALUES (?, ?, ?, ?)
                ON CONFLICT({country_fk}, {year_col})
                DO UPDATE SET
                    {pop_col}=excluded.{pop_col},
                    {pwe_col_db}=excluded.{pwe_col_db}
                """,
                (cid, year, popv, pwe)
            )
        elif pwith is not None:
            cur.execute(
                f"""
                INSERT INTO {etbl} ({country_fk}, {year_col}, {pwe_col_db}, {pwith_col_db})
                VALUES (?, ?, ?, ?)
                ON CONFLICT({country_fk}, {year_col})
                DO UPDATE SET
                    {pwe_col_db}=excluded.{pwe_col_db},
                    {pwith_col_db}=excluded.{pwith_col_db}
                """,
                (cid, year, pwe, pwith)
            )
        else:
            cur.execute(
                f"""
                INSERT INTO {etbl} ({country_fk}, {year_col}, {pwe_col_db})
                VALUES (?, ?, ?)
                ON CONFLICT({country_fk}, {year_col})
                DO UPDATE SET
                    {pwe_col_db}=excluded.{pwe_col_db}
                """,
                (cid, year, pwe)
            )

        # ensure PopulationData has cached population rows
        if code and code in pop_cache:
            cache_val = pop_cache[code].get(year)
            if cache_val is not None and ptable:
                cur.execute(
                    f"""
                    INSERT INTO {ptable} (country_id, year, population)
                    VALUES (?, ?, ?)
                    ON CONFLICT(country_id, year)
                    DO UPDATE SET population = excluded.population
                    """,
                    (cid, year, cache_val)
                )

        if i % BATCH_COMMIT == 0:
            conn.commit()
            print(f"Processed {i}/{total} rows...")
    conn.commit()
    conn.close()
    print("Data load complete. Database ready.")

if __name__ == "__main__":
    load_data()
