# db_designer.py

import sqlite3
from datetime import datetime
from typing import List

DB_NAME = "electricity_access.db"

def _connect():
    # return a plain connection; callers will manage row/tuple handling
    return sqlite3.connect(DB_NAME)

def _table_exists(cur, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def _get_columns(cur, table: str) -> List[str]:
    if not table:
        return []
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

# ---------------------------
# Init / migrations
# ---------------------------
def init_db():
    conn = _connect()
    cur = conn.cursor()

    countries_tbl = _get_table_candidates(cur, ["Countries", "countries"])
    electricity_tbl = _get_table_candidates(cur, ["ElectricityAccess", "electricity_access"])
    pop_tbl = _get_table_candidates(cur, ["PopulationData", "populationdata"])

    # If DB empty (no countries & no electricity), create canonical schema
    if not countries_tbl and not electricity_tbl:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Countries (
                country_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_name TEXT UNIQUE NOT NULL,
                region TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ElectricityAccess (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                population INTEGER,
                people_without_electricity INTEGER NOT NULL,
                people_with_electricity INTEGER,
                UNIQUE(country_id, year)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS PopulationData (
                pop_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                population INTEGER,
                UNIQUE(country_id, year)
            )
        """)
        conn.commit()

    # Ensure PopulationData exists
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
        conn.commit()

    # Ensure electricity table has population column (if table exists but older)
    if electricity_tbl:
        cols = _get_columns(cur, electricity_tbl)
        lc = [c.lower() for c in cols]
        if "population" not in lc:
            try:
                cur.execute(f"ALTER TABLE {electricity_tbl} ADD COLUMN population INTEGER")
            except Exception:
                # ignore if alter fails
                pass

    _ensure_querylogs(cur)
    conn.commit()
    conn.close()

# ---------------------------
# Schema inspection helper
# ---------------------------
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
        "pop_cols": _get_columns(cur, pop) if pop else []
    }
    conn.close()
    return schema

# ---------------------------
# Countries CRUD
# ---------------------------
def add_country(country_name: str, region: str = None):
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()
    ctbl = schema["countries_table"] or "Countries"
    # create table if missing (safe)
    if not schema["countries_table"]:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Countries (
                country_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_name TEXT UNIQUE NOT NULL,
                region TEXT
            )
        """)
        conn.commit()
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

# ---------------------------
# Electricity CRUD
# ---------------------------
def add_record(country_id: int, year: int, pwe: int, pwith: int = None, population: int = None):
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()
    etbl = schema["elec_table"] or "ElectricityAccess"
    if not schema["elec_table"]:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ElectricityAccess (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                population INTEGER,
                people_without_electricity INTEGER NOT NULL,
                people_with_electricity INTEGER,
                UNIQUE(country_id, year)
            )
        """)
        conn.commit()
    ecols = _get_columns(cur, etbl)
    # detect canonical names (fallback to defaults)
    year_col = next((c for c in ecols if c and c.lower() == "year"), "year")
    pop_col = next((c for c in ecols if c and "population" in c.lower()), "population")
    pwe_col = next((c for c in ecols if c and "without" in c.lower()), "people_without_electricity")
    pwith_col = next((c for c in ecols if c and "with" in c.lower()), "people_with_electricity")
    country_fk = next((c for c in ecols if c and "country" in c.lower()), "country_id")

    cur.execute(f"SELECT record_id FROM {etbl} WHERE {country_fk}=? AND {year_col}=?", (country_id, year))
    q = cur.fetchone()
    if q:
        rid = q[0]
        sets = []
        params = []
        if population is not None:
            sets.append(f"{pop_col}=?"); params.append(population)
        if pwe is not None:
            sets.append(f"{pwe_col}=?"); params.append(pwe)
        if pwith is not None:
            sets.append(f"{pwith_col}=?"); params.append(pwith)
        if sets:
            params.append(rid)
            cur.execute(f"UPDATE {etbl} SET {', '.join(sets)} WHERE record_id=?", tuple(params))
    else:
        cols = [country_fk, year_col]
        vals = [country_id, year]
        if population is not None:
            cols.append(pop_col); vals.append(population)
        if pwe is not None:
            cols.append(pwe_col); vals.append(pwe)
        if pwith is not None:
            cols.append(pwith_col); vals.append(pwith)
        placeholders = ", ".join(["?"] * len(vals))
        cur.execute(f"INSERT INTO {etbl} ({', '.join(cols)}) VALUES ({placeholders})", tuple(vals))
    conn.commit()
    conn.close()

def get_records(limit=None):
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()
    etbl = schema["elec_table"]
    ctbl = schema["countries_table"]
    if not etbl or not ctbl:
        conn.close()
        return []
    ecols = schema["elec_cols"]
    ccols = schema["countries_cols"]

    record_id = next((c for c in ecols if "record" in c.lower()), next((c for c in ecols if "id" in c.lower()), ecols[0]))
    year_col = next((c for c in ecols if c.lower()=="year"), "year")
    pop_col = next((c for c in ecols if "population" in c.lower()), None)
    pwe_col = next((c for c in ecols if "without" in c.lower()), None)
    pwith_col = next((c for c in ecols if "with" in c.lower()), None)
    country_fk = next((c for c in ecols if "country" in c.lower()), "country_id")
    cname = next((c for c in ccols if "name" in c.lower()), "country_name")
    cid = next((c for c in ccols if "id" in c.lower()), "country_id")

    select = [f"e.{record_id}", f"c.{cname}", f"e.{year_col}"]
    if pop_col:
        select.append(f"e.{pop_col} AS population")
    else:
        select.append("NULL AS population")
    # ensure COALESCE uses at least 2 args; but here we want raw numeric values for Python to coerce
    if pwe_col:
        select.append(f"COALESCE(e.{pwe_col}, 0) AS people_without")
    else:
        select.append("0 AS people_without")
    if pwith_col:
        select.append(f"e.{pwith_col} AS people_with")
    else:
        select.append("NULL AS people_with")

    sql = f"""
        SELECT {', '.join(select)}
        FROM {etbl} e
        JOIN {ctbl} c ON e.{country_fk} = c.{cid}
        ORDER BY c.{cname}, e.{year_col}
    """
    if limit:
        sql += f" LIMIT {limit}"
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    # Do not return the string "None"; return real None for missing values
    out = []
    for r in rows:
        rec = list(r)
        # keep Python None, numeric zeros remain numeric
        # Python sqlite returns None for SQL NULL already; no transformation
        out.append(tuple(rec))
    return out

def update_record(record_id: int, pwe=None, pwe_with=None, population=None):
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()
    etbl = schema["elec_table"]
    ecols = schema["elec_cols"]
    id_col = next((c for c in ecols if "record" in c.lower()), next((c for c in ecols if "id" in c.lower()), ecols[0]))
    pwe_col = next((c for c in ecols if "without" in c.lower()), None)
    pwith_col = next((c for c in ecols if "with" in c.lower()), None)
    pop_col = next((c for c in ecols if "population" in c.lower()), None)
    updates = []; params = []
    if population is not None and pop_col:
        updates.append(f"{pop_col}=?"); params.append(population)
    if pwe is not None and pwe_col:
        updates.append(f"{pwe_col}=?"); params.append(pwe)
    if pwe_with is not None and pwith_col:
        updates.append(f"{pwith_col}=?"); params.append(pwe_with)
    if updates:
        params.append(record_id)
        cur.execute(f"UPDATE {etbl} SET {', '.join(updates)} WHERE {id_col}=?", tuple(params))
    conn.commit()
    conn.close()

def delete_record(record_id: int):
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()
    etbl = schema["elec_table"]
    ecols = schema["elec_cols"]
    id_col = next((c for c in ecols if "record" in c.lower()), next((c for c in ecols if "id" in c.lower()), ecols[0]))
    cur.execute(f"DELETE FROM {etbl} WHERE {id_col}=?", (record_id,))
    conn.commit()
    conn.close()
