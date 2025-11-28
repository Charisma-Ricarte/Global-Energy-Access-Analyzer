# db_designer.py
# Robust and schema-safe database layer for the Global Electricity Access Analyzer.

import sqlite3
from datetime import datetime

DB_NAME = "electricity_access.db"

# ======================================================================
# UTIL HELPERS
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

    # Create canonical schema when empty
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
                people_without_electricity INTEGER NOT NULL,
                people_with_electricity INTEGER
            )
        """)

        # ✔ FIXED: population CAN BE NULL now
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
        cur.execute(
            f"INSERT OR IGNORE INTO {ctbl} ({name_col}, {region_col}) VALUES (?, ?)",
            (country_name, region)
        )
    else:
        cur.execute(
            f"INSERT OR IGNORE INTO {ctbl} ({name_col}) VALUES (?)",
            (country_name,)
        )

    conn.commit()
    conn.close()


# ======================================================================
# GET COUNTRIES
# ======================================================================

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
# ADD ELECTRICITY RECORD
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
                people_with_electricity INTEGER
            )
        """)
        etbl = "ElectricityAccess"
        ecols = _get_columns(cur, etbl)

    year_col = next((c for c in ecols if c.lower() == "year"), "year")
    pwe_col = next((c for c in ecols if "without" in c.lower()), None)
    pwith_col = next((c for c in ecols if "with" in c.lower() and "without" not in c.lower()), None)

    if pwith_col:
        cur.execute(
            f"INSERT INTO {etbl} (country_id, {year_col}, {pwe_col}, {pwith_col}) VALUES (?, ?, ?, ?)",
            (country_id, year, pwe, pwith)
        )
    else:
        cur.execute(
            f"INSERT INTO {etbl} (country_id, {year_col}, {pwe_col}) VALUES (?, ?, ?)",
            (country_id, year, pwe)
        )

    conn.commit()
    conn.close()


# ======================================================================
# GET RECORDS 
# ======================================================================

def get_records(limit=None):
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()

    etbl = schema["elec_table"]
    ctbl = schema["countries_table"]
    ecols = schema["elec_cols"]
    ccols = schema["countries_cols"]

    if not etbl or not ctbl:
        conn.close()
        return []

    record_id = next((c for c in ecols if "record" in c.lower()), next((c for c in ecols if "id" in c.lower()), ecols[0]))
    year_col = next((c for c in ecols if c.lower() == "year"), "year")
    pwe_col = next((c for c in ecols if "without" in c.lower()), None)
    pwith_col = next((c for c in ecols if "with" in c.lower() and "without" not in c.lower()), None)
    country_fk = next((c for c in ecols if "country" in c.lower()), "country_id")

    cname = next((c for c in ccols if "name" in c.lower()), "country_name")
    cid = next((c for c in ccols if "id" in c.lower()), "country_id")

    sql = f"""
        SELECT e.{record_id}, c.{cname}, e.{year_col},
               COALESCE(e.{pwe_col}, 0),
               COALESCE(e.{pwith_col}, NULL)
        FROM {etbl} e
        JOIN {ctbl} c ON e.{country_fk} = c.{cid}
        ORDER BY c.{cname}, e.{year_col}
    """

    if limit:
        sql += f" LIMIT {limit}"

    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()

    # GUI wants "None" visible
    return [(r[0], r[1], r[2], r[3], (r[4] if r[4] is not None else "None")) for r in rows]


# ======================================================================
# UPDATE RECORD
# ======================================================================

def update_record(record_id, country_id, year, pwe, pwith):
    conn = _connect()
    cur = conn.cursor()
    schema = _detect_schema()

    etbl = schema["elec_table"]
    ecols = schema["elec_cols"]

    if not etbl:
        conn.close()
        return 0

    record_id_col = next(
        (c for c in ecols if "record" in c.lower()),
        next((c for c in ecols if "id" in c.lower()), ecols[0])
    )
    year_col = next((c for c in ecols if c.lower() == "year"), "year")
    pwe_col = next((c for c in ecols if "without" in c.lower()), None)
    pwith_col = next((c for c in ecols if "with" in c.lower() and "without" not in c.lower()), None)
    country_fk = next((c for c in ecols if "country" in c.lower()), "country_id")

    if not pwe_col:
        conn.close()
        return 0

    if pwith_col:
        sql = f"""
            UPDATE {etbl}
            SET {country_fk} = ?,
                {year_col} = ?,
                {pwe_col} = ?,
                {pwith_col} = ?
            WHERE {record_id_col} = ?
        """
        params = (country_id, year, pwe, pwith, record_id)
    else:
        sql = f"""
            UPDATE {etbl}
            SET {country_fk} = ?,
                {year_col} = ?,
                {pwe_col} = ?
            WHERE {record_id_col} = ?
        """
        params = (country_id, year, pwe, record_id)

    cur.execute(sql, params)
    updated = cur.rowcount
    conn.commit()
    conn.close()
    return updated




# ======================================================================
# DELETE RECORD
# ======================================================================

def delete_record(record_id):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM ElectricityAccess WHERE record_id = ?", (record_id,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted

