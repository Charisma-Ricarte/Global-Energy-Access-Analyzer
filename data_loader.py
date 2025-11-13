# data_loader.py
# Loads data from your Kaggle electricity dataset (people without electricity)
# PLUS automatically fetches population data from the World Bank API.

import csv
import sqlite3
import requests
import time
import db_designer

DB_NAME = "electricity_access.db"

# -------------------------------------------------------------------
# Helpers for Region + Country lookup/insertion
# -------------------------------------------------------------------

def get_or_create_region(cur, region_name="Unknown"):
    cur.execute("SELECT region_id FROM Regions WHERE region_name=?", (region_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO Regions (region_name) VALUES (?)", (region_name,))
    return cur.lastrowid

def get_or_create_country(cur, country_name, region_id):
    cur.execute("SELECT country_id FROM Countries WHERE country_name=?", (country_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO Countries (country_name, region_id) VALUES (?, ?)", (country_name, region_id))
    return cur.lastrowid

# -------------------------------------------------------------------
# Fetch population data from World Bank API
# -------------------------------------------------------------------

def fetch_population_data():
    print("Fetching population data from World Bank API …")
    # URL endpoint for all countries, indicator SP.POP.TOTL
    url = "http://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&per_page=20000"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    # data[1] contains list of records
    records = data[1]
    pop_by_country_year = {}  # {(country_name, year): population}
    for rec in records:
        country_name = rec.get("country", {}).get("value")
        year = rec.get("date")
        value = rec.get("value")
        if country_name and year and value is not None:
            pop_by_country_year[(country_name.strip(), int(year))] = int(value)
    print(f"Fetched {len(pop_by_country_year)} population records.")
    return pop_by_country_year


def load_data(elec_csv_path):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    print("\n=== Starting data loading ===")

    # Step 1: Insert population records
    pop_data = fetch_population_data()
    for (country_name, year), population in pop_data.items():
        region_id = get_or_create_region(cur, "Unknown")
        country_id = get_or_create_country(cur, country_name, region_id)
        cur.execute("""
            INSERT OR IGNORE INTO PopulationData (country_id, year, population)
            VALUES (?, ?, ?)
        """, (country_id, year, population))
    conn.commit()
    print("Population data loaded.")

    # Step 2: Load electricity access data
    print("Loading electricity access dataset …")
    with open(elec_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country = row["Entity"].strip()
            year = int(row["Year"])
            people_without = int(float(row["Number of people without access to electricity (people without electricity access)"]))


            region_id = get_or_create_region(cur, "Unknown")
            country_id = get_or_create_country(cur, country, region_id)

            # Try to find population for same country & year
            cur.execute("""
                SELECT population FROM PopulationData
                WHERE country_id=? AND year=?
            """, (country_id, year))
            pop_row = cur.fetchone()
            population = pop_row[0] if (pop_row and pop_row[0] is not None) else None

            # Insert electricity access
            if population is not None:
                people_with = population - people_without
            else:
                people_with = None

            cur.execute("""
                INSERT INTO ElectricityAccess 
                (country_id, year, people_without_electricity, people_with_electricity)
                VALUES (?, ?, ?, ?)
            """, (country_id, year, people_without, people_with))

    conn.commit()
    conn.close()
    print("Electricity access data loaded.")
    print("=== Data loading complete! ===")

if __name__ == "__main__":
    db_designer.init_db()
    print("Database initialized.")

    load_data("electricity_dataset.csv")  # update path if different

    print("All data loaded successfully!")

