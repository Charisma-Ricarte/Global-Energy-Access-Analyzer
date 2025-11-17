# data_loader.py
# Final robust loader for Global Electricity Access Analyzer

import csv
import os
import time
import requests
import db_designer

CSV_PREFERRED = "electricity_dataset.csv"

def find_csv():
    if os.path.exists(CSV_PREFERRED):
        return CSV_PREFERRED
    for f in os.listdir("."):
        if f.lower().endswith(".csv"):
            return f
    return None

def fetch_population(country_code):
    """Fetch population from World Bank API. Returns dict year -> population or empty dict."""
    if not country_code or country_code.strip() == "":
        return {}
    try:
        resp = requests.get(
            f"https://api.worldbank.org/v2/country/{country_code}/indicator/SP.POP.TOTL?format=json",
            timeout=6
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        if not isinstance(data, list) or len(data) < 2:
            return {}
        res = {}
        for e in data[1]:
            yr = e.get("date")
            val = e.get("value")
            if yr:
                try:
                    res[int(yr)] = int(val) if val is not None else None
                except Exception:
                    pass
        return res
    except Exception:
        return {}

def load_data():
    print("=== Starting loader ===")
    db_designer.init_db()

    csv_file = find_csv()
    if not csv_file:
        print("❌ No CSV file found. Place the CSV in project folder.")
        return

    print(f"Using CSV: {csv_file}")
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"✅ Found {len(rows)} rows in CSV.")

    conn = db_designer._connect()
    cur = conn.cursor()
    schema = db_designer._detect_schema()

    ctbl, etbl, ptable = schema["countries_table"], schema["elec_table"], schema["pop_table"]
    ccols, ecols, pop_cols = schema["countries_cols"], schema["elec_cols"], schema["pop_cols"]

    # Safe fallback for column names
    cname_col = next((c for c in ccols if "name" in c.lower()), "country_name")
    cid_col = next((c for c in ccols if "id" in c.lower()), "country_id")

    year_col = next((c for c in ecols if c.lower() == "year"), "year")
    pwe_col = next((c for c in ecols if "without" in c.lower()), "people_without_electricity")
    pwith_col = next((c for c in ecols if "with" in c.lower()), "people_with_electricity")

    pop_cid = next((c for c in pop_cols if "country" in c.lower()), None)
    pop_year = next((c for c in pop_cols if c.lower() == "year"), None)
    pop_val = next((c for c in pop_cols if "pop" in c.lower()), None)

    country_cache = {}

    for i, r in enumerate(rows):
        entity = r.get("Entity", "").strip() or "NONE"
        code = r.get("Code", "").strip()
        try:
            year = int(r.get("Year"))
        except Exception:
            continue

        # CSV columns for electricity access
        pwe_csv = "Number of people without access to electricity (people without electricity access)"
        pwe = int(float(r.get(pwe_csv) or 0))
        pwith_csv = "Number of people with access to electricity"
        pwith = int(float(r.get(pwith_csv) or 0))

        # Insert or get country
        if entity not in country_cache:
            cur.execute(f"SELECT {cid_col} FROM {ctbl} WHERE {cname_col}=?", (entity,))
            q = cur.fetchone()
            if q:
                country_id = q[0]
            else:
                cur.execute(f"INSERT INTO {ctbl} ({cname_col}, region) VALUES (?, ?)", (entity, None))
                country_id = cur.lastrowid
            country_cache[entity] = (country_id, code)
        else:
            country_id, code = country_cache[entity]

        # Insert electricity record
        cur.execute(f"SELECT 1 FROM {etbl} WHERE country_id=? AND {year_col}=?", (country_id, year))
        if not cur.fetchone():
            cur.execute(
                f"INSERT INTO {etbl} (country_id, {year_col}, {pwe_col}, {pwith_col}) VALUES (?, ?, ?, ?)",
                (country_id, year, pwe, pwith)
            )

        # Insert population safely
        pop_map = fetch_population(code)
        popv = pop_map.get(year) if pop_map else None

        if ptable and pop_cid and pop_year and pop_val and popv is not None:
            # Only insert if population value exists
            cur.execute(f"SELECT 1 FROM {ptable} WHERE {pop_cid}=? AND {pop_year}=?", (country_id, year))
            if not cur.fetchone():
                cur.execute(
                    f"INSERT INTO {ptable} ({pop_cid}, {pop_year}, {pop_val}) VALUES (?, ?, ?)",
                    (country_id, year, popv)
                )
        else:
            print(f"⚠ Population for {entity} {year} unavailable, will display 'NONE' in GUI")

        if i % 500 == 0:
            print(f"Processed {i}/{len(rows)} rows...")
        time.sleep(0.01)

    conn.commit()
    conn.close()
    print("✅ Data load complete. Database ready.")

if __name__ == "__main__":
    load_data()
