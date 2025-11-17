# backend.py
# Thin wrappers around db_designer with query logging for the frontend.

import db_designer

# Ensure DB initialized and QueryLogs exist
db_designer.init_db()

# CRUD wrappers
def add_country(name, region=None):
    db_designer.add_country(name, region)

def get_countries():
    return db_designer.get_countries()

def add_electricity_record(country_id, year, people_without, people_with=None):
    db_designer.add_record(country_id, year, people_without, people_with)

def get_electricity_records(limit=None):
    return db_designer.get_records(limit)

def update_electricity_record(record_id, pwe=None, pwe_with=None):
    db_designer.update_record(record_id, pwe, pwe_with)

def delete_electricity_record(record_id):
    db_designer.delete_record(record_id)

# Analytical queries (log then run)
def _log(name, params=""):
    # small helper to ensure QueryLogs exists and to write a row
    conn = db_designer._connect()
    cur = conn.cursor()
    try:
        db_designer._ensure_querylogs(cur)
    except Exception:
        # if the internal helper name isn't available for some reason, still proceed
        pass
    try:
        db_designer.log_query(cur, name, params)
    except Exception:
        # fallback: insert directly if naming differences
        try:
            cur.execute("INSERT INTO QueryLogs (query_name, parameters, timestamp) VALUES (?, ?, datetime('now'))", (name, str(params)))
        except Exception:
            pass
    conn.commit()
    conn.close()

def query_high_unserved(threshold):
    _log("high_unserved", threshold)
    return db_designer.get_high_unserved_countries(threshold)

def query_yearly_trend():
    _log("yearly_trend")
    return db_designer.get_yearly_access_trend()

def query_access_percent(year):
    _log("access_percent", year)
    return db_designer.get_access_percentage_by_country(year)

def query_regional_comparison(year):
    _log("regional_comparison", year)
    return db_designer.get_regional_access_comparison(year)

def query_most_improved():
    _log("most_improved")
    return db_designer.get_most_improved_countries()
