# data_loader.py
# - Reads population_dataset.csv (Kaggle wide format)
# - Reads electricity_dataset.csv (Entity, Code, Year, <people without column>)
# - Loads population only for years 1990..2016 (matching electricity data)
# - Idempotent: uses UPSERTs (ON CONFLICT) and never inserts autoincrement PKs
# - Safe to re-run; delete electricity_access.db once before the *first* run to ensure canonical schema

import csv
import os
import sqlite3
import time
from collections import defaultdict

import db_designer

# filenames (case-insensitive search)
CSV_POP = "population_dataset.csv"
CSV_ELEC = "electricity_dataset.csv"

# electricity CSV column that commonly contains people-without count (fallback detection)
FALLBACK_PWE = "Number of people without access to electricity (people without electricity access)"

# years to import from population (1990..2016 inclusive)
POP_YEARS = list(range(1990, 2017))

DB = "electricity_access.db"
BATCH_COMMIT = 500  # commit every N rows for speed & durability

# -------------------------
# Helpers
# -------------------------
def find_file_case_insensitive(name):
    if os.path.exists(name):
        return name
    for f in os.listdir("."):
        if f.lower() == name.lower():
            return f
    return None

def load_population_csv(path):
    """
    Load Kaggle-style population CSV: headers include 'Country Name','Country Code', then year columns '1960','1961',...
    Returns: pop_cache[ISO3][year] = int(pop) or None (only includes POP_YEARS)
    """
    pop_cache = defaultdict(dict)
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        # detect which year columns are present and intersect with POP_YEARS
        available_years = [int(h) for h in reader.fieldnames if h.isdigit()]
        desired_years = [y for y in POP_YEARS if y in available_years]
        for row in reader:
            code = (row.get("Country Code") or row.get("Country code") or "").strip()
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

def load_electricity_csv(path):
    """
    Read electricity CSV into list of dict rows. Returns (rows, detected_pwe_column_name)
    """
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
            # fallback to exact known header or first numeric-ish column after Year
            if FALLBACK_PWE in reader.fieldnames:
                pwe_col = FALLBACK_PWE
            else:
                # Attempt to find a long header that contains 'access' or 'electricity'
                for h in reader.fieldnames:
                    if h and ("access" in h.lower() or "electricity" in h.lower()):
                        pwe_col = h
                        break
        return rows, pwe_col

# -------------------------
# Upsert helpers
# -------------------------
def upsert_population_rows(conn, country_cache, pop_cache):
    """
    Insert or update PopulationData rows.
    - country_cache: dict entity -> (country_id, iso3_code)
    - pop_cache: dict iso3 -> {year: population}
    """
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    ptable = schema["pop_table"]
    if not ptable:
        return

    # Build mapping code -> list of country_ids that use that code
    code_to_cids = {}
    for ent, (cid, code) in country_cache.items():
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
                # safe upsert — never touch pop_id
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

# -------------------------
# Main loader
# -------------------------
def load_data():
    print("=== Starting loader ===")
    # ensure canonical schema exists
    db_designer.init_db()

    pop_file = find_file_case_insensitive(CSV_POP)
    elec_file = find_file_case_insensitive(CSV_ELEC)

    if not elec_file:
        print(f"electricity CSV not found ({CSV_ELEC}) - place it in this folder and re-run.")
        return
    if not pop_file:
        print(f"population CSV not found ({CSV_POP}) - loader will still continue but population will be missing.")
        pop_file = None

    # Load population CSV into memory (only years 1990..2016)
    pop_cache = {}
    if pop_file:
        print("Loading population CSV into memory (years 1990-2016)...")
        pop_cache = load_population_csv(pop_file)
        print(f"Loaded population codes: {len(pop_cache)}")
    else:
        print("No population CSV; continuing without preloaded population data.")

    # Load electricity CSV
    print("Loading electricity CSV...")
    elec_rows, pwe_col = load_electricity_csv(elec_file)
    print(f"Found {len(elec_rows)} electricity rows")
    if not pwe_col:
        print("Could not auto-detect 'people without' column — using fallback header.")
        pwe_col = FALLBACK_PWE

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    schema = db_designer._detect_schema()
    ctbl = schema["countries_table"]
    etbl = schema["elec_table"]
    ptable = schema["pop_table"]

    # columns and names detection for flexible schemas
    ccols = schema["countries_cols"]
    ecols = schema["elec_cols"]
    pop_cols = schema["pop_cols"]

    cname_col = next((c for c in ccols if "name" in c.lower()), ccols[1] if len(ccols)>1 else ccols[0])
    cid_col = next((c for c in ccols if "id" in c.lower()), ccols[0])

    # ensure canonical tables exist (in case db_designer didn't create)
    db_designer.init_db()
    schema = db_designer._detect_schema()
    etbl = schema["elec_table"]
    ptable = schema["pop_table"]
    ecols = schema["elec_cols"]
    pop_cols = schema["pop_cols"]

    # prepare or refresh country cache from electricity CSV entities
    country_cache = {}  # entity -> (country_id, iso3_code)
    entities = []
    for r in elec_rows:
        ent = (r.get("Entity") or "").strip()
        if not ent:
            ent = "NONE"
        if ent not in entities:
            entities.append(ent)

    for ent in entities:
        # find a sample code for this entity in the rows
        sample_code = ""
        for r in elec_rows:
            if (r.get("Entity") or "").strip() == ent:
                sample_code = (r.get("Code") or "").strip()
                if sample_code:
                    break
        # see if country exists
        cur.execute(f"SELECT {cid_col} FROM {ctbl} WHERE {cname_col}=?", (ent,))
        q = cur.fetchone()
        if q:
            cid = q[0]
        else:
            cur.execute(f"INSERT INTO {ctbl} ({cname_col}, region) VALUES (?, ?)", (ent, None))
            cid = cur.lastrowid
        country_cache[ent] = (cid, sample_code)
    conn.commit()

    # Upsert population rows from the preloaded pop_cache (only years 1990..2016)
    if pop_cache:
        print("Upserting population rows into PopulationData...")
        upsert_population_rows(conn, country_cache, pop_cache)
        print("Population upsert complete.")

    # Now iterate electricity rows and upsert into ElectricityAccess (and also ensure PopulationData written when possible)
    print("Inserting/updating ElectricityAccess rows...")
    # determine column names used in ElectricityAccess table
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
        code = (r.get("Code") or "").strip()
        try:
            year = int(r.get("Year"))
        except Exception:
            # skip malformed year rows
            continue

        # parse people-without value from CSV using detected header
        try:
            pwe_val_raw = r.get(pwe_col)
            pwe = int(float(pwe_val_raw)) if (pwe_val_raw not in (None, "")) else 0
        except Exception:
            pwe = 0

        # determine country_id
        cid, sample_code = country_cache.get(ent, (None, code))
        if cid is None:
            # fallback: insert a country row
            cur.execute(f"INSERT INTO {ctbl} ({cname_col}, region) VALUES (?, ?)", (ent, None))
            cid = cur.lastrowid
            country_cache[ent] = (cid, code)

        # figure population value: prefer preloaded pop_cache via ISO code, else check PopulationData table
        popv = None
        if code and code in pop_cache:
            popv = pop_cache[code].get(year)
        else:
            if ptable:
                cur.execute(f"SELECT {pop_table_val} FROM {ptable} WHERE {pop_table_cid}=? AND {pop_table_year}=?", (cid, year))
                q = cur.fetchone()
                if q:
                    popv = q[0]

        # compute people_with when possible (use population - without)
        pwith = None
        if popv is not None:
            try:
                pwith_calc = int(popv) - int(pwe)
                if pwith_calc >= 0:
                    pwith = pwith_calc
            except Exception:
                pwith = None

        # Upsert into ElectricityAccess using UNIQUE(country_id, year)
        # Build the values to insert/update
        # Use an UPSERT statement so we never touch record_id manually
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

        # If we have a cached population available, ensure it is stored in PopulationData (UPSERT)
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

        # periodic progress / commit
        if i % BATCH_COMMIT == 0:
            conn.commit()
            print(f"Processed {i}/{total} rows...")
    # final commit
    conn.commit()
    conn.close()
    print("Data load complete. Database ready.")

if __name__ == "__main__":
    load_data()
