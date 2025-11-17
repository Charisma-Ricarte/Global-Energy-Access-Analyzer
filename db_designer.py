# db_designer.py
# Robust schema-safe database layer for Global Electricity Access Analyzer
# Preserves schema flexibility and adds CSV column mapping

import sqlite3
from datetime import datetime

DB_NAME = "electricity_access.db"

# CSV column mapping
CSV_COLUMN_MAP = {
    "Number of people without access to electricity (people without electricity access)": "people_without_electricity",
    "Number of people with access to electricity": "people_with_electricity",
    "Entity": "country_name",
    "Code": "code",
    "Year": "year"
}

def map_csv_column(csv_col_name):
    return CSV_COLUMN_MAP.get(csv_col_name, csv_col_name)

# -------------------------
# Database connection
# -------------------------
def _connect():
    return sqlite3.connect(DB_NAME)

def _table_exists(cur, name):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def _get_columns(cur, table):
    cur.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]

def _get_table_candidates(cur, candidates):
    for t in candidates:
        if _table_exists(cur, t):
            return t
    return None

def _ensure_querylogs(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS QueryLogs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_name TEXT NOT NULL,
            parameters TEXT,
            timestamp TEXT
        )
    """)

def log_query(cur, query_name, params=""):
    timestamp = datetime.utcnow().isoformat()
    cur.execute("INSERT INTO QueryLogs (query_name, parameters, timestamp) VALUES (?, ?, ?)",
                (query_name, str(params), timestamp))

# -------------------------
# Initialize DB
# -------------------------
def init_db():
    conn = _connect()
    cur = conn.cursor()

    countries_tbl = _get_table_candidates(cur, ["Countries", "countries"])
    electricity_tbl = _get_table_candidates(cur, ["ElectricityAccess", "electricity_access"])
    population_tbl = _get_table_candidates(cur, ["PopulationData", "populationdata"])

    # Create tables if missing
    if not countries_tbl:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Countries (
                country_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_name TEXT UNIQUE NOT NULL,
                region TEXT
            )
        """)
    if not electricity_tbl:
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
    if not population_tbl:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS PopulationData (
                pop_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                population INTEGER NOT NULL,
                UNIQUE(country_id, year),
                FOREIGN KEY(country_id) REFERENCES Countries(country_id)
            )
        """)

    _ensure_querylogs(cur)
    conn.commit()
    conn.close()

# -------------------------
# Schema detection
# -------------------------
def _detect_schema():
    conn = _connect()
    cur = conn.cursor()

    countries = _get_table_candidates(cur, ["Countries", "countries"])
    elec = _get_table_candidates(cur, ["ElectricityAccess", "electricity_access"])
    pop = _get_table_candidates(cur, ["PopulationData", "populationdata", "population_data"])

    schema = {
        "countries_table": countries,
        "elec_table": elec,
        "pop_table": pop,
        "countries_cols": _get_columns(cur, countries) if countries else [],
        "elec_cols": _get_columns(cur, elec) if elec else [],
        "pop_cols": _get_columns(cur, pop) if pop else []
    }

    conn.close()
    return schema
