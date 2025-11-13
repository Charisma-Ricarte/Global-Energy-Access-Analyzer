# db_designer.py
# Simple and complete database designer code for Global Energy Access Analyzer
import sqlite3

DB_NAME = "electricity_access.db"

# ----------------------------
# 1. Create Database + Tables
# ----------------------------
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Turn on foreign key support
    cur.execute("PRAGMA foreign_keys = ON;")

    # Table: Countries
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Countries (
            country_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_name TEXT NOT NULL UNIQUE,
            region TEXT
        )
    """)

    # Table: ElectricityAccess
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ElectricityAccess (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            people_without_electricity INTEGER NOT NULL,
            people_with_electricity INTEGER,
            FOREIGN KEY(country_id) REFERENCES Countries(country_id)
        )
    """)

    conn.commit()
    conn.close()

# ----------------------------
# 2. CRUD Operations
# ----------------------------
def add_country(country_name, region=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO Countries (country_name, region) VALUES (?, ?)", (country_name, region))
    conn.commit()
    conn.close()

def get_countries():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT * FROM Countries")
    rows = cur.fetchall()
    conn.close()
    return rows

def add_record(country_id, year, pwe, pwe_with=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ElectricityAccess (country_id, year, people_without_electricity, people_with_electricity)
        VALUES (?, ?, ?, ?)
    """, (country_id, year, pwe, pwe_with))
    conn.commit()
    conn.close()

def get_records():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT e.record_id, c.country_name, e.year,
               e.people_without_electricity, e.people_with_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON e.country_id = c.country_id
        ORDER BY c.country_name, e.year
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def update_record(record_id, pwe=None, pwe_with=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    if pwe is not None:
        cur.execute("UPDATE ElectricityAccess SET people_without_electricity=? WHERE record_id=?", (pwe, record_id))
    if pwe_with is not None:
        cur.execute("UPDATE ElectricityAccess SET people_with_electricity=? WHERE record_id=?", (pwe_with, record_id))
    conn.commit()
    conn.close()

def delete_record(record_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM ElectricityAccess WHERE record_id=?", (record_id,))
    conn.commit()
    conn.close()

# ----------------------------
# 3. Analytical Queries
# ----------------------------
def get_high_unserved_countries(threshold=1000000):
    """1. Countries with more than X people without electricity"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.country_name, e.year, e.people_without_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON e.country_id = c.country_id
        WHERE e.people_without_electricity > ?
        ORDER BY e.people_without_electricity DESC
    """, (threshold,))
    data = cur.fetchall()
    conn.close()
    return data

def get_yearly_access_trend():
    """2. Total people with electricity each year"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT year, SUM(COALESCE(people_with_electricity, 0))
        FROM ElectricityAccess
        GROUP BY year
        ORDER BY year ASC
    """)
    data = cur.fetchall()
    conn.close()
    return data

def get_access_percentage_by_country(populations, year):
    """3. Percent electricity access by country for a year (needs dict of populations)"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.country_name, e.people_without_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON e.country_id = c.country_id
        WHERE e.year = ?
    """, (year,))
    results = []
    for country, pwe in cur.fetchall():
        if country in populations:
            pop = populations[country]
            access_pct = round((pop - pwe) / pop * 100, 2)
            results.append((country, access_pct))
    conn.close()
    return sorted(results, key=lambda x: x[1])

def get_regional_access_comparison(populations, year):
    """4. Average access % by region"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.region, c.country_name, e.people_without_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON e.country_id = c.country_id
        WHERE e.year = ?
    """, (year,))
    region_totals = {}
    region_counts = {}
    for region, country, pwe in cur.fetchall():
        if country in populations:
            pop = populations[country]
            access_pct = (pop - pwe) / pop * 100
            region_totals[region] = region_totals.get(region, 0) + access_pct
            region_counts[region] = region_counts.get(region, 0) + 1
    results = [(r, round(region_totals[r] / region_counts[r], 2)) for r in region_totals]
    conn.close()
    return sorted(results, key=lambda x: x[1], reverse=True)

def get_most_improved_countries():
    """5. Countries that improved the most (drop in people without electricity)"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.country_name, e.year, e.people_without_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON e.country_id = c.country_id
        ORDER BY c.country_name, e.year
    """)
    data = cur.fetchall()
    changes = {}
    for country, year, pwe in data:
        if country not in changes:
            changes[country] = [pwe, pwe]  # [min, max]
        else:
            changes[country][0] = min(changes[country][0], pwe)
            changes[country][1] = max(changes[country][1], pwe)
    improvements = [(c, old - new) for c, (new, old) in changes.items()]
    conn.close()
    return sorted(improvements, key=lambda x: x[1], reverse=True)

# ----------------------------
# 4. Example Run (for testing)
# ----------------------------
if __name__ == "__main__":
    init_db()
    add_country("Kenya", "Africa")
    add_country("India", "Asia")
    add_record(1, 2020, 5000000, 40000000)
    add_record(2, 2020, 10000000, 900000000)

    print("Countries:", get_countries())
    print("Records:", get_records())
    print("High unserved:", get_high_unserved_countries(2000000))
    print("Yearly trend:", get_yearly_access_trend())
    populations = {"Kenya": 50000000, "India": 1300000000}
    print("Access %:", get_access_percentage_by_country(populations, 2020))
    print("Regional compare:", get_regional_access_comparison(populations, 2020))
    print("Most improved:", get_most_improved_countries())
