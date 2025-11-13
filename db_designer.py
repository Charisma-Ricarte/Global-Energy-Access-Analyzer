# db_designer.py
# Complete database schema + CRUD + analytical queries
# Updated to include population table + regions + full integration.

import sqlite3

DB_NAME = "electricity_access.db"

# ---------------------------------------------------------
# 1. CREATE DATABASE + TABLES
# ---------------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    # REGIONS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Regions (
            region_id INTEGER PRIMARY KEY AUTOINCREMENT,
            region_name TEXT UNIQUE NOT NULL
        )
    """)

    # COUNTRIES
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Countries (
            country_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_name TEXT UNIQUE NOT NULL,
            region_id INTEGER,
            FOREIGN KEY (region_id) REFERENCES Regions(region_id)
        )
    """)

    # POPULATION DATA
    cur.execute("""
        CREATE TABLE IF NOT EXISTS PopulationData (
            pop_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            population INTEGER NOT NULL,
            UNIQUE(country_id, year),
            FOREIGN KEY (country_id) REFERENCES Countries(country_id)
        )
    """)

    # ELECTRICITY ACCESS DATA
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ElectricityAccess (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            people_without_electricity INTEGER NOT NULL,
            people_with_electricity INTEGER,
            FOREIGN KEY (country_id) REFERENCES Countries(country_id)
        )
    """)

    conn.commit()
    conn.close()


# ---------------------------------------------------------
# 2. CRUD OPERATIONS
# ---------------------------------------------------------

# ---- Countries ----
def add_country(country_name, region_id=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO Countries (country_name, region_id)
        VALUES (?, ?)
    """, (country_name, region_id))
    conn.commit()
    conn.close()

def get_countries():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.country_id, c.country_name, r.region_name
        FROM Countries c
        LEFT JOIN Regions r ON c.region_id = r.region_id
        ORDER BY country_name
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

# ---- Electricity Access ----
def add_record(country_id, year, pwe, pwe_with=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ElectricityAccess 
        (country_id, year, people_without_electricity, people_with_electricity)
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
        JOIN Countries c ON c.country_id = e.country_id
        ORDER BY c.country_name, e.year
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def update_record(record_id, pwe=None, pwe_with=None):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    if pwe is not None:
        cur.execute("""
            UPDATE ElectricityAccess
            SET people_without_electricity=?
            WHERE record_id=?
        """, (pwe, record_id))

    if pwe_with is not None:
        cur.execute("""
            UPDATE ElectricityAccess
            SET people_with_electricity=?
            WHERE record_id=?
        """, (pwe_with, record_id))
    
    conn.commit()
    conn.close()

def delete_record(record_id):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM ElectricityAccess WHERE record_id=?", (record_id,))
    conn.commit()
    conn.close()


# ---------------------------------------------------------
# 3. ANALYTICAL QUERIES (5 REQUIRED)
# ---------------------------------------------------------

### 1️⃣ Countries with more than X unserved (people w/o electricity)
def get_high_unserved_countries(threshold):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.country_name, e.year, e.people_without_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON c.country_id = e.country_id
        WHERE e.people_without_electricity > ?
        ORDER BY e.people_without_electricity DESC
    """, (threshold,))
    data = cur.fetchall()
    conn.close()
    return data


### 2️⃣ Yearly global electricity access trend
def get_yearly_access_trend():
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


### 3️⃣ Access percentage by country for a given year
def get_access_percentage_by_country(year):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.country_name,
               e.people_without_electricity,
               p.population
        FROM ElectricityAccess e
        JOIN PopulationData p
            ON p.country_id = e.country_id AND p.year = e.year
        JOIN Countries c
            ON c.country_id = e.country_id
        WHERE e.year = ? AND p.population IS NOT NULL
    """, (year,))

    rows = cur.fetchall()
    conn.close()

    results = []
    for name, pwe, pop in rows:
        pct = round((pop - pwe) / pop * 100, 2)
        results.append((name, pct))

    return sorted(results, key=lambda x: x[1], reverse=True)


### 4️⃣ Average regional access comparison
def get_regional_access_comparison(year):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT r.region_name,
               c.country_name,
               e.people_without_electricity,
               p.population
        FROM ElectricityAccess e
        JOIN PopulationData p
            ON p.country_id = e.country_id AND p.year = e.year
        JOIN Countries c
            ON c.country_id = e.country_id
        JOIN Regions r
            ON r.region_id = c.region_id
        WHERE e.year = ? AND p.population IS NOT NULL
    """, (year,))

    rows = cur.fetchall()
    conn.close()

    region_totals = {}
    region_counts = {}

    for region, country, pwe, pop in rows:
        access_pct = (pop - pwe) / pop * 100

        region_totals[region] = region_totals.get(region, 0) + access_pct
        region_counts[region] = region_counts.get(region, 0) + 1

    results = [
        (region, round(region_totals[region] / region_counts[region], 2))
        for region in region_totals
    ]

    return sorted(results, key=lambda x: x[1], reverse=True)


### 5️⃣ Most improved countries (biggest drop in unserved population)
def get_most_improved_countries():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.country_name, e.year, e.people_without_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON c.country_id = e.country_id
        ORDER BY c.country_name, e.year
    """)
    rows = cur.fetchall()
    conn.close()

    tracking = {}  # {country: {"min": x, "max": y}}

    for country, year, pwe in rows:
        if country not in tracking:
            tracking[country] = {"min": pwe, "max": pwe}
        else:
            tracking[country]["min"] = min(tracking[country]["min"], pwe)
            tracking[country]["max"] = max(tracking[country]["max"], pwe)

    improvements = [
        (country, tracking[country]["max"] - tracking[country]["min"])
        for country in tracking
    ]

    return sorted(improvements, key=lambda x: x[1], reverse=True)
