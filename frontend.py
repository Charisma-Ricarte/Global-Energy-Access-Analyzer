# data_loader.py
# Loads electricity access CSV + automatically fetches population data.

import csv
import sqlite3
import requests
import time
import os
import sys
from db_designer import init_db, add_country, add_record

DB_NAME = "electricity_access.db"
print("Waiting for database to be fully ready...")
while True:
    if os.path.exists(DB_NAME):
        try:
            conn = sqlite3.connect(DB_NAME)
            cur = conn.cursor()
            # Check if the 'Countries' table exists and has data
            cur.execute("SELECT COUNT(*) FROM Countries")
            count = cur.fetchone()[0]
            conn.close()
            if count > 0:
                break  # database is ready
        except sqlite3.OperationalError:
            # DB exists but tables not created yet
            pass
    print("Database not ready yet, waiting 20 seconds...")
    time.sleep(20)

print("Database ready! Launching frontend...")

while True:
    if os.path.exists(DB_NAME):
        try:
            conn = sqlite3.connect(DB_NAME)
            cur = conn.cursor()
            # Check if the 'Countries' table exists and has data
            cur.execute("SELECT COUNT(*) FROM Countries")
            count = cur.fetchone()[0]
            conn.close()
            if count > 0:
                break  # database is ready
        except sqlite3.OperationalError:
            # DB exists but tables not created yet
            pass
    print("Database not ready yet, waiting 2 seconds...")
    time.sleep(2)

print("✅ Database ready! Launching frontend...")
# ---------------------------------------------------------
# Auto-detect CSV file in project folder
# ---------------------------------------------------------
def find_csv_file():
    """Automatically find a CSV file in the project directory."""
    for file in os.listdir("."):
        if file.lower().endswith(".csv"):
            print(f"CSV detected: {file}")
            return file
    print("❌ No CSV file found in project directory.")
    print("Place your electricity CSV in the same folder as data_loader.py.")
    sys.exit(1)


# ---------------------------------------------------------
# Fetch population from World Bank API
# ---------------------------------------------------------
def fetch_population(country_code, year):
    """Fetch population using World Bank API. Returns int or None."""
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/SP.POP.TOTL?format=json"

    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()

        if not isinstance(data, list) or len(data) < 2:
            return None

        for entry in data[1]:
            if entry["date"] == str(year) and entry["value"] is not None:
                return int(entry["value"])
        return None

    except Exception:
        return None


# ---------------------------------------------------------
# Main load function
# ---------------------------------------------------------
def load_data():
    print("Initializing database...")
    init_db()

    csv_file = find_csv_file()  # auto detection
    print(f"Reading CSV: {csv_file}")

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    seen_countries = set()
    population_cache = {}
    countries_added = 0
    records_added = 0

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            country = row["Entity"].strip()
            code = row["Code"].strip()
            year = int(row["Year"])
            people_without = int(float(row["Number of people without access to electricity (people without electricity access)"]))

            # Skip invalid aggregate regions, keep real countries
            if code == "" or len(code) != 3:
                continue

            # Add country if needed
            if country not in seen_countries:
                add_country(country, region="Unknown")
                seen_countries.add(country)
                countries_added += 1

            # Get population (cached to reduce API calls)
            pop_key = (code, year)
            if pop_key not in population_cache:
                pop = fetch_population(code, year)
                population_cache[pop_key] = pop
                time.sleep(0.25)  # avoid hammering API
            else:
                pop = population_cache[pop_key]

            # Calculate people with electricity
            people_with = None
            if pop and pop > people_without:
                people_with = pop - people_without

            # Insert ElectricityAccess record
            cur.execute("SELECT country_id FROM Countries WHERE country_name=?", (country,))
            row_c = cur.fetchone()

            if row_c:
                country_id = row_c[0]
                add_record(country_id, year, people_without, people_with)
                records_added += 1

    conn.close()

    print("\nLOAD COMPLETE!")
    print(f"➡ Countries added: {countries_added}")
    print(f"➡ Records added:   {records_added}")


if __name__ == "__main__":
    load_data()
