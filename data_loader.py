import csv
import os
import sqlite3
from collections import defaultdict
import db_designer

CSV_POP = "population_dataset.csv"
CSV_ELEC = "electricity_dataset.csv"
DB = "electricity_access.db"
BATCH_COMMIT = 500  # commit every N rows

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def find_file_case_insensitive(name):
    if os.path.exists(name):
        return name
    for f in os.listdir("."):
        if f.lower() == name.lower():
            return f
    return None

# --------------------------------------------------------------------------------------
# Load population CSV into dictionary {country_code_or_name: {year: population}}
# --------------------------------------------------------------------------------------
def load_population_csv(path):
    pop_cache = defaultdict(dict)
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        years = [y for y in reader.fieldnames if y.isdigit()]

        for row in reader:
            code = (row.get("Country Code") or "").strip()
            name = (row.get("Country Name") or "").strip()
            key = code if code else name

            for y in years:
                val = row.get(y)
                if val:
                    try:
                        pop_cache[key][int(y)] = int(float(val))
                    except:
                        pop_cache[key][int(y)] = None
                else:
                    pop_cache[key][int(y)] = None
    return pop_cache

# --------------------------------------------------------------------------------------
# Load electricity CSV and detect "people without electricity" column
# --------------------------------------------------------------------------------------
def load_electricity_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

        pwe_col = None
        for h in reader.fieldnames:
            if h and "without" in h.lower():
                pwe_col = h
                break
        if not pwe_col:
            fallback = "Number of people without access to electricity (people without electricity access)"
            if fallback in reader.fieldnames:
                pwe_col = fallback
            else:
                raise ValueError("Cannot detect 'people without electricity' column in CSV")
        return rows, pwe_col

# --------------------------------------------------------------------------------------
# Main loader
# --------------------------------------------------------------------------------------
def load_data():
    print("=== Starting loader ===")

    # Initialize DB
    db_designer.init_db()

    # Find files
    pop_file = find_file_case_insensitive(CSV_POP)
    elec_file = find_file_case_insensitive(CSV_ELEC)
    if not elec_file:
        print(f"Electricity CSV not found ({CSV_ELEC})")
        return
    if not pop_file:
        print(f"Population CSV not found ({CSV_POP}) — population will be NULL")
        pop_cache = {}
    else:
        print("Loading population CSV...")
        pop_cache = load_population_csv(pop_file)
        print(f"Loaded population data for {len(pop_cache)} countries")

    # Load electricity CSV
    print("Loading electricity CSV...")
    elec_rows, pwe_col = load_electricity_csv(elec_file)
    print(f"Found {len(elec_rows)} electricity rows. PWE column = {pwe_col}")

    # Connect to DB
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Get schema
    schema = db_designer._detect_schema()
    ctbl = schema["countries_table"]
    etbl = schema["elec_table"]
    ptable = schema["pop_table"]

    cname_col = next((c for c in schema["countries_cols"] if "name" in c.lower()), "country_name")
    cid_col   = next((c for c in schema["countries_cols"] if "id"   in c.lower()), "country_id")
    year_col = next((c for c in schema["elec_cols"] if c.lower() == "year"), "year")
    pwe_col_db = next((c for c in schema["elec_cols"] if "without" in c.lower()), "people_without_electricity")
    pwith_col_db = next((c for c in schema["elec_cols"] if "with" in c.lower() and "without" not in c.lower()), "people_with_electricity")
    country_fk = next((c for c in schema["elec_cols"] if "country" in c.lower()), "country_id")

    # ----------------------------------------------------------------------------------
    # Build country cache
    # ----------------------------------------------------------------------------------
    country_cache = {}
    for r in elec_rows:
        ent = (r.get("Entity") or "").strip()
        code = (r.get("Code") or "").strip()
        if not ent or ent in country_cache:
            continue

        # insert country if not exists
        cur.execute(f"SELECT {cid_col} FROM {ctbl} WHERE {cname_col}=?", (ent,))
        q = cur.fetchone()
        if q:
            cid = q[0]
        else:
            cur.execute(f"INSERT INTO {ctbl} ({cname_col}, region) VALUES (?, ?)", (ent, None))
            cid = cur.lastrowid
        country_cache[ent] = (cid, code)

    conn.commit()

    # ----------------------------------------------------------------------------------
    # Insert electricity & population data
    # ----------------------------------------------------------------------------------
    total = len(elec_rows)
    for i, r in enumerate(elec_rows):
        ent = (r.get("Entity") or "").strip()
        if not ent or ent not in country_cache:
            continue

        cid, code = country_cache[ent]

        try:
            year = int(r.get("Year"))
        except:
            continue

        try:
            pwe = int(float(r.get(pwe_col) or 0))
        except:
            pwe = 0

        # Lookup population
        popv = None
        if code and code in pop_cache:
            popv = pop_cache[code].get(year)
        if popv is None and ent in pop_cache:
            popv = pop_cache[ent].get(year)

        # Compute people with electricity
        pwith = None
        if popv is not None:
            pwith = max(popv - pwe, 0)

        # UPSERT electricity table
        cur.execute(f"""
            INSERT INTO {etbl} ({country_fk}, {year_col}, {pwe_col_db}, {pwith_col_db})
            VALUES (?, ?, ?, ?)
            ON CONFLICT({country_fk}, {year_col})
            DO UPDATE SET
                {pwe_col_db}=excluded.{pwe_col_db},
                {pwith_col_db}=excluded.{pwith_col_db}
        """, (cid, year, pwe, pwith))

        # UPSERT population table
        if ptable:
            cur.execute(f"""
                INSERT INTO {ptable} (country_id, year, population)
                VALUES (?, ?, ?)
                ON CONFLICT(country_id, year)
                DO UPDATE SET population=excluded.population
            """, (cid, year, popv))

        # Commit every batch
        if i % BATCH_COMMIT == 0:
            conn.commit()
            print(f"Processed {i}/{total}")

    conn.commit()
    conn.close()
    print("=== Load complete ===")

# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    load_data()
