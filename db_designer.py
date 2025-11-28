# db_designer.py
import sqlite3
from datetime import datetime

DB_NAME = "electricity_access.db"

# ======================================================================
# UTILITY HELPERS
# ======================================================================

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
    cur.execute("""
        INSERT INTO QueryLogs (query_name, parameters, timestamp)
        VALUES (?, ?, ?)
    """, (query_name, str(params), timestamp))

# ======================================================================
# INITIALIZATION
# ======================================================================

def init_db():
    conn = _connect()
    cur = conn.cursor()
    
    countries_tbl = _get_table_candidates(cur, ["Countries", "countries"])
    electricity_tbl = _get_table_candidates(cur, ["ElectricityAccess", "electricity_access"])
    pop_tbl = _get_table_candidates(cur, ["PopulationData", "populationdata"])

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
                UNIQUE(country_id, year)
            )
        """)
    if not pop_tbl:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS PopulationData (
                pop_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                population INTEGER,
                UNIQUE(country_id, year)
            )
        """)
    _ensure_querylogs(cur)
    conn.commit()
    conn.close()

# ======================================================================
# SCHEMA DETECTOR
# ======================================================================

def _detect_schema():
    conn = _connect()
    cur = conn.cursor()
    countries = _get_table_candidates(cur, ["Countries", "countries"])
    elec = _get_table_candidates(cur, ["ElectricityAccess", "electricity_access"])
    pop = _get_table_candidates(cur, ["PopulationData", "populationdata"])
    schema = {
        "countries_table": countries,
        "elec_table": elec,
        "pop_table": pop,
        "countries_cols": _get_columns(cur, countries) if countries else [],
        "elec_cols": _get_columns(cur, elec) if elec else [],
        "pop_cols": _get_columns(cur, pop) if pop else [],
    }
    conn.close()
    return schema

# ======================================================================
# COUNTRY FUNCTIONS
# ======================================================================

def add_country(country_name, region=None):
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()
    ctbl = schema["countries_table"]
    cols = schema["countries_cols"]
    if not ctbl:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Countries (
                country_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_name TEXT UNIQUE NOT NULL,
                region TEXT
            )
        """)
        ctbl = "Countries"
        cols = _get_columns(cur, ctbl)
    name_col = next((c for c in cols if "name" in c.lower()), "country_name")
    region_col = next((c for c in cols if "region" in c.lower()), None)
    if region_col:
        cur.execute(f"INSERT OR IGNORE INTO {ctbl} ({name_col}, {region_col}) VALUES (?, ?)", (country_name, region))
    else:
        cur.execute(f"INSERT OR IGNORE INTO {ctbl} ({name_col}) VALUES (?)", (country_name,))
    conn.commit()
    conn.close()

def get_countries():
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()
    ctbl = schema["countries_table"]
    if not ctbl:
        conn.close()
        return []
    cols = schema["countries_cols"]
    id_col = next((c for c in cols if "id" in c.lower()), cols[0])
    name_col = next((c for c in cols if "name" in c.lower()), cols[1] if len(cols) > 1 else cols[0])
    region_col = next((c for c in cols if "region" in c.lower()), None)
    fields = [id_col, name_col]
    if region_col:
        fields.append(region_col)
    cur.execute(f"SELECT {', '.join(fields)} FROM {ctbl} ORDER BY {name_col}")
    rows = cur.fetchall()
    conn.close()
    return rows

# ======================================================================
# ELECTRICITY RECORDS
# ======================================================================

def add_record(country_id, year, pwe, pwith=None):
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()
    etbl = schema["elec_table"]
    ecols = schema["elec_cols"]
    if not etbl:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ElectricityAccess (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                people_without_electricity INTEGER NOT NULL,
                people_with_electricity INTEGER,
                UNIQUE(country_id, year)
            )
        """)
        etbl = "ElectricityAccess"
        ecols = _get_columns(cur, etbl)
    year_col = next((c for c in ecols if c.lower() == "year"), "year")
    pwe_col = next((c for c in ecols if "without" in c.lower()), None)
    pwith_col = next((c for c in ecols if "with" in c.lower() and "without" not in c.lower()), None)
    cols = [pwe_col]
    vals = [pwe]
    if pwith_col: cols.append(pwith_col); vals.append(pwith)
    col_str = ", ".join(cols)
    placeholders = ", ".join("?" for _ in cols)
    update_str = ", ".join(f"{c}=excluded.{c}" for c in cols)
    cur.execute(f"""
        INSERT INTO {etbl} (country_id, {year_col}, {col_str})
        VALUES (?, ?, {placeholders})
        ON CONFLICT(country_id, {year_col}) DO UPDATE SET {update_str}
    """, [country_id, year] + vals)
    conn.commit()
    conn.close()

def get_records():
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()
    etbl = schema["elec_table"]
    ctbl = schema["countries_table"]
    ptbl = schema["pop_table"]
    if not etbl or not ctbl:
        conn.close()
        return []
    cur.execute(f"""
        SELECT e.record_id, c.country_name, e.year,
               p.population, e.people_without_electricity, e.people_with_electricity
        FROM {etbl} e
        JOIN {ctbl} c ON e.country_id = c.country_id
        LEFT JOIN {ptbl} p ON e.country_id = p.country_id AND e.year = p.year
        ORDER BY e.year
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def update_record(record_id, pwe=None, pwe_with=None):
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()
    etbl = schema["elec_table"]
    ecols = schema["elec_cols"]
    if not etbl:
        conn.close()
        return
    updates=[]
    params=[]
    pwe_col = next((c for c in ecols if "without" in c.lower()), None)
    pwith_col = next((c for c in ecols if "with" in c.lower() and "without" not in c.lower()), None)
    if pwe is not None and pwe_col: updates.append(f"{pwe_col}=?"); params.append(pwe)
    if pwe_with is not None and pwith_col: updates.append(f"{pwith_col}=?"); params.append(pwe_with)
    if updates:
        sql=f"UPDATE {etbl} SET {', '.join(updates)} WHERE record_id=?"
        params.append(record_id)
        cur.execute(sql,params)
    conn.commit()
    conn.close()

def delete_record(record_id):
    conn=_connect()
    cur=conn.cursor()
    schema=_detect_schema()
    etbl=schema["elec_table"]
    if etbl: cur.execute(f"DELETE FROM {etbl} WHERE record_id=?", (record_id,))
    conn.commit()
    conn.close()

# ======================================================================
# POPULATION RECORDS
# ======================================================================

def add_population(country_id, year, population):
    conn=_connect()
    cur=conn.cursor()
    schema=_detect_schema()
    ptable=schema["pop_table"]
    if not ptable:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS PopulationData (
                pop_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                population INTEGER,
                UNIQUE(country_id, year)
            )
        """)
        ptable="PopulationData"
    cur.execute(f"""
        INSERT INTO {ptable} (country_id, year, population)
        VALUES (?, ?, ?)
        ON CONFLICT(country_id, year) DO UPDATE SET population=excluded.population
    """,(country_id, year, population))
    conn.commit()
    conn.close()
