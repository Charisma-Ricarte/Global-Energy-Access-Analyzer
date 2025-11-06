# backend_dev.py
import sqlite3
from typing import List, Tuple, Dict

DB_NAME = "electricity_access.db"

# ----------------------------
# 1. Database Initialization
# ----------------------------
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Countries (
            country_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_name TEXT NOT NULL,
            region TEXT
        )
    """)

    cursor.execute("""
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
# 2. CRUD - Countries
# ----------------------------
def add_country(name: str, region: str = None):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Countries (country_name, region) VALUES (?, ?)", (name, region))
    conn.commit()
    conn.close()

def get_countries() -> List[Tuple]:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Countries")
    results = cursor.fetchall()
    conn.close()
    return results

def update_country(country_id: int, name: str = None, region: str = None):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    if name:
        cursor.execute("UPDATE Countries SET country_name=? WHERE country_id=?", (name, country_id))
    if region:
        cursor.execute("UPDATE Countries SET region=? WHERE country_id=?", (region, country_id))
    conn.commit()
    conn.close()

def delete_country(country_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM Countries WHERE country_id=?", (country_id,))
    conn.commit()
    conn.close()

# ----------------------------
# 3. CRUD - ElectricityAccess
# ----------------------------
def add_record(country_id: int, year: int, pwe: int, pwe_with: int = None):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO ElectricityAccess (country_id, year, people_without_electricity, people_with_electricity)
        VALUES (?, ?, ?, ?)
    """, (country_id, year, pwe, pwe_with))
    conn.commit()
    conn.close()

def get_records() -> List[Tuple]:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT r.record_id, c.country_name, r.year, r.people_without_electricity, r.people_with_electricity
        FROM ElectricityAccess r
        JOIN Countries c ON r.country_id = c.country_id
    """)
    results = cursor.fetchall()
    conn.close()
    return results

def update_record(record_id: int, pwe: int = None, pwe_with: int = None):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    if pwe is not None:
        cursor.execute("UPDATE ElectricityAccess SET people_without_electricity=? WHERE record_id=?", (pwe, record_id))
    if pwe_with is not None:
        cursor.execute("UPDATE ElectricityAccess SET people_with_electricity=? WHERE record_id=?", (pwe_with, record_id))
    conn.commit()
    conn.close()

def delete_record(record_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ElectricityAccess WHERE record_id=?", (record_id,))
    conn.commit()
    conn.close()

# ----------------------------
# 4. Analytical Queries
# ----------------------------
def get_high_unserved_countries(threshold: int = 1000000) -> List[Tuple]:
    """Countries with more than threshold people without electricity"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.country_name, e.year, e.people_without_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON e.country_id = c.country_id
        WHERE e.people_without_electricity > ?
        ORDER BY e.people_without_electricity DESC
    """, (threshold,))
    results = cursor.fetchall()
    conn.close()
    return results

def get_yearly_access_trend() -> List[Tuple[int, int]]:
    """Summarize electricity access trends over time globally"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT year, SUM(people_with_electricity) as total_access
        FROM ElectricityAccess
        GROUP BY year
        ORDER BY year ASC
    """)
    results = cursor.fetchall()
    conn.close()
    return results

def get_access_percentage_by_country(populations: Dict[str, int], target_year: int) -> List[Tuple[str, float]]:
    """List countries by electricity access percentage for a given year"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.country_name, e.people_without_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON e.country_id = c.country_id
        WHERE e.year = ?
    """, (target_year,))
    results = []
    for country_name, pwe in cursor.fetchall():
        total_pop = populations.get(country_name, None)
        if total_pop:
            access_pct = (total_pop - pwe) / total_pop * 100
            results.append((country_name, access_pct))
    results.sort(key=lambda x: x[1])
    conn.close()
    return results

def get_regional_access_comparison(populations: Dict[str, int]) -> List[Tuple[str, float]]:
    """Compare average electricity access across regions"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.region, c.country_name, e.people_without_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON e.country_id = c.country_id
    """)
    region_totals = {}
    region_counts = {}
    for region, country_name, pwe in cursor.fetchall():
        total_pop = populations.get(country_name)
        if total_pop:
            access_pct = (total_pop - pwe) / total_pop * 100
            region_totals[region] = region_totals.get(region, 0) + access_pct
            region_counts[region] = region_counts.get(region, 0) + 1
    results = [(region, region_totals[region]/region_counts[region]) for region in region_totals]
    results.sort(key=lambda x: x[1], reverse=True)
    conn.close()
    return results

def get_most_improved_countries() -> List[Tuple[str, int]]:
    """Find countries that reduced electricity deprivation the most over time"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.country_name, e.people_without_electricity
        FROM ElectricityAccess e
        JOIN Countries c ON e.country_id = c.country_id
    """)
    country_min = {}
    country_max = {}
    for country, pwe in cursor.fetchall():
        country_min[country] = min(pwe, country_min.get(country, pwe))
        country_max[country] = max(pwe, country_max.get(country, pwe))
    results = [(country, country_max[country]-country_min[country]) for country in country_max]
    results.sort(key=lambda x: x[1], reverse=True)
    conn.close()
    return results

# ----------------------------
# Example usage
# ----------------------------
if __name__ == "__main__":
    init_db()
    add_country("Testland", "Region1")
    add_country("Samplestan", "Region2")
    add_record(1, 2020, 500000, 1500000)
    add_record(2, 2020, 1200000, 800000)

    print("Countries:", get_countries())
    print("Records:", get_records())
    print("High unserved countries:", get_high_unserved_countries())
