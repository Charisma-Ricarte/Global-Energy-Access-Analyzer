import csv
import os
import sqlite3
import requests
import time
import db_designer

CSV_PREFERRED = "electricity_dataset.csv"
DB = "electricity_access.db"

def find_csv():
    if os.path.exists(CSV_PREFERRED):
        return CSV_PREFERRED
    for f in os.listdir("."):
        if f.lower().endswith(".csv"):
            return f
    return None

def fetch_population(code):
    if not code or code.strip() == "":
        return {}
    try:
        url = f"https://api.worldbank.org/v2/country/{code}/indicator/SP.POP.TOTL?format=json"
        r = requests.get(url, timeout=8)
        js = r.json()
        if not isinstance(js, list) or len(js) < 2:
            return {}
        out = {}
        for row in js[1]:
            yr = row.get("date")
            val = row.get("value")
            try:
                if yr and val:
                    out[int(yr)] = int(val)
            except:
                pass
        return out
    except:
        return {}

def load_data():
    db_designer.init_db()

    csv_file = find_csv()
    if not csv_file:
        print("No CSV found.")
        return

    print(f"Using CSV: {csv_file}")

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"✓ Found {len(rows)} rows in CSV.")

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # ensure tables exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS PopulationData (
            pop_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            population INTEGER,
            UNIQUE(country_id, year)
        )
    """)

    schema = db_designer._detect_schema()
    ctbl = schema["countries_table"]
    etbl = schema["elec_table"]
    ptable = schema["pop_table"]

    ccols = schema["countries_cols"]
    ecols = schema["elec_cols"]
    pop_cols = schema["pop_cols"]

    cname = "country_name"
    cid = "country_id"
    year_col = "year"
    pwe_col = "people_without_electricity"
    pwith_col = "people_with_electricity"

    pop_cid = "country_id"
    pop_year = "year"
    pop_val = "population"

    country_cache = {}

    for i, r in enumerate(rows):
        try:
            entity = r.get("Entity", "").strip()
            code = r.get("Code", "").strip()
            year = int(r.get("Year"))
            pwe = int(float(r.get(
                "Number of people without access to electricity (people without electricity access)"
            ) or 0))
        except:
            continue

        if not entity:
            continue

        # -----------------------------------------
        # Insert country
        # -----------------------------------------
        cur.execute(f"SELECT country_id FROM {ctbl} WHERE {cname}=?", (entity,))
        row = cur.fetchone()
        if row:
            country_id = row[0]
        else:
            cur.execute(f"INSERT INTO {ctbl} ({cname}, region) VALUES (?, ?)", (entity, None))
            country_id = cur.lastrowid

        # -----------------------------------------
        # Electricity row
        # -----------------------------------------
        cur.execute(
            f"SELECT 1 FROM {etbl} WHERE country_id=? AND year=?",
            (country_id, year)
        )
        if not cur.fetchone():
            cur.execute(
                f"INSERT INTO {etbl}(country_id, year, people_without_electricity) VALUES (?, ?, ?)",
                (country_id, year, pwe)
            )

        # -----------------------------------------
        # Population — SAFE INSERT
        # -----------------------------------------
        pop_map = fetch_population(code)

        # if no pop data → store NULL
        population_value = pop_map.get(year) if pop_map else None

        cur.execute(
            f"SELECT 1 FROM {ptable} WHERE country_id=? AND year=?",
            (country_id, year)
        )
        if not cur.fetchone():
            cur.execute(
                f"INSERT INTO {ptable} (country_id, year, population) VALUES (?, ?, ?)",
                (country_id, year, population_value)   # can be NULL safely
            )

        if i % 600 == 0:
            print(f"Processed {i}/{len(rows)}")

        time.sleep(0.01)

    conn.commit()
    conn.close()
    print("✓ Load complete")

if __name__ == "__main__":
    load_data()
