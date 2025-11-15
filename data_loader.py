# data_loader.py
import csv
import sqlite3
import os
import requests
import time

DB_NAME = "electricity_access.db"

# -------------------------------------------------------------------
# Database setup
# -------------------------------------------------------------------

def create_tables():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Regions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS regions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    """)

    # Countries table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS countries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            region_id INTEGER,
            population INTEGER,
            FOREIGN KEY(region_id) REFERENCES regions(id)
        )
    """)

    # Electricity access table (yearly data)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS electricity_access (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER,
            year INTEGER,
            people_without_access INTEGER,
            FOREIGN KEY(country_id) REFERENCES countries(id)
        )
    """)

    conn.commit()
    conn.close()

# -------------------------------------------------------------------
# Helpers for Region + Country
# -------------------------------------------------------------------

def get_or_create_region(cur, region_name="Unknown"):
    cur.execute("SELECT id FROM regions WHERE name = ?", (region_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO regions (name) VALUES (?)", (region_name,))
    return cur.lastrowid

def add_country(name, region="Unknown"):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    region_id = get_or_create_region(cur, region)

    cur.execute("SELECT id FROM countries WHERE name = ?", (name,))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO countries (name, region_id) VALUES (?, ?)",
            (name, region_id)
        )

    conn.commit()
    conn.close()

# -------------------------------------------------------------------
# Helpers for Population
# -------------------------------------------------------------------

def get_population(country_code):
    """Fetch population from World Bank API using ISO3 country code."""
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/SP.POP.TOTL?format=json&date=2023"
    try:
        response = requests.get(url)
        data = response.json()

        if not isinstance(data, list) or len(data) < 2:
            print(f"No population data found for {country_code}")
            return None

        for entry in data[1]:
            if entry.get("value") is not None:
                return int(entry["value"])
        print(f"No population value found for {country_code}")
        return None
    except Exception as e:
        print(f"Error fetching population for {country_code}: {e}")
        return None

def add_population(country_name, population):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT id FROM countries WHERE name = ?", (country_name,))
    row = cur.fetchone()
    if row:
        country_id = row[0]
        cur.execute(
            "UPDATE countries SET population = ? WHERE id = ?",
            (population, country_id)
        )
    conn.commit()
    conn.close()

# -------------------------------------------------------------------
# Load CSV Data with progress messages
# -------------------------------------------------------------------

def load_data():
    create_tables()

    csv_file = os.path.join(os.path.dirname(__file__), "electricity_dataset.csv")
    if not os.path.exists(csv_file):
        print(f"CSV file not found: {csv_file}")
        return

    # Count total rows for progress
    with open(csv_file, newline="", encoding="utf-8") as f:
        total_rows = sum(1 for _ in f) - 1  # minus header

    print(f"Starting data load: {total_rows} rows to process...")

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            country = row.get("Entity")
            country_code = row.get("Code")
            year = row.get("Year")
            people_without = row.get("Number of people without access to electricity (people without electricity access)")

            if not country or not country_code or not year or not people_without:
                continue

            add_country(country, region="Unknown")

            # Fetch population
            population = get_population(country_code)
            if population:
                add_population(country, population)

            # Add yearly electricity access data
            conn = sqlite3.connect(DB_NAME)
            cur = conn.cursor()
            cur.execute("SELECT id FROM countries WHERE name = ?", (country,))
            country_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO electricity_access (country_id, year, people_without_access)
                VALUES (?, ?, ?)
            """, (country_id, int(year), int(people_without)))
            conn.commit()
            conn.close()

            # Progress message every 10 rows or last row
            if idx % 10 == 0 or idx == total_rows:
                print(f"Processed {idx}/{total_rows} rows...")

            time.sleep(0.1)  # avoid hitting API rate limits

    print("Data loading complete! Database is ready for frontend.")


if __name__ == "__main__":
    load_data()
