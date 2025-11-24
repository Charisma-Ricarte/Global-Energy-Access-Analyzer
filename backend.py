# backend.py

import sqlite3
from typing import List, Tuple, Optional
import db_designer

# ensure DB exists and migrations applied
db_designer.init_db()

# keywords to treat as aggregates (lowercase)
AGGREGATE_WORDS = {
    "world", "income", "ida", "ibrd", "oecd", "region", "regions",
    "asia", "africa", "europe", "america", "caribbean", "pacific",
    "blend", "only", "total", "demographic", "fragile", "hipc",
    "high income", "low income", "middle income", "small states",
    "least", "developed", "developing"
}

def _is_aggregate(name: Optional[str]) -> bool:
    if not name:
        return True
    ln = str(name).strip().lower()
    # treat as aggregate if any keyword occurs
    return any(k in ln for k in AGGREGATE_WORDS)

# --- simple pass-throughs used by the frontend ---
def get_countries() -> List[Tuple]:
    """Return list of (id, name[, region])"""
    return db_designer.get_countries()

def add_country(name: str, region: Optional[str] = None):
    """Expose add_country for frontend (used by Add Entry screen)."""
    return db_designer.add_country(name, region)

def get_electricity_records(search: Optional[str] = None) -> List[Tuple]:
    """Return normalized records: (record_id, country, year, population, without, with)."""
    raw = db_designer.get_records()
    out = []
    for r in raw:
        # defensive unpacking
        rec_id = None; country = None; year = None; population = None; without = None; with_ = None
        try:
            if len(r) >= 6:
                rec_id, country, year, population, without, with_ = r[:6]
            elif len(r) == 5:
                rec_id, country, year, without, with_ = r
                population = None
            elif len(r) == 4:
                rec_id, country, year, without = r
                population = None; with_ = None
            else:
                rec_id = r[0] if len(r) > 0 else None
                country = r[1] if len(r) > 1 else None
                year = r[2] if len(r) > 2 else None
                population = r[3] if len(r) > 3 else None
                without = r[4] if len(r) > 4 else None
                with_ = r[5] if len(r) > 5 else None
        except Exception:
            # badly shaped row -> skip
            continue

        # normalize string "None"
        if isinstance(population, str) and population.lower() == "none":
            population = None
        if isinstance(without, str) and without.lower() == "none":
            without = None
        if isinstance(with_, str) and with_.lower() == "none":
            with_ = None

        # safe numeric coercion
        def to_int_safe(v):
            try:
                if v is None:
                    return None
                return int(float(v))
            except Exception:
                return None

        pop_val = to_int_safe(population)
        without_val = to_int_safe(without) if without is not None else 0
        with_val = to_int_safe(with_) if with_ is not None else None

        out.append((rec_id, country, year, pop_val, without_val, with_val))

    if search:
        s = search.strip().lower()
        out = [row for row in out if row[1] and s in str(row[1]).lower()]
    return out

def add_electricity_record(country_name: str, year: int, people_without: int, people_with: Optional[int] = None, population: Optional[int] = None):
    """Create (or reuse) country, then delegate to db_designer.add_record."""
    # find or create country id
    cid = None
    for c in db_designer.get_countries():
        if len(c) >= 2 and c[1] and c[1].strip().lower() == country_name.strip().lower():
            cid = c[0]; break
    if cid is None:
        db_designer.add_country(country_name, None)
        for c in db_designer.get_countries():
            if len(c) >= 2 and c[1] and c[1].strip().lower() == country_name.strip().lower():
                cid = c[0]; break
    if cid is None:
        raise RuntimeError("Failed to create or resolve country")
    db_designer.add_record(cid, year, people_without, people_with, population)

def delete_electricity_record(record_id: int):
    db_designer.delete_record(record_id)

# -----------------------
# Analytical queries
# -----------------------
def query_high_unserved(threshold: int = 1_000_000) -> List[Tuple[str,int]]:
    """
    Return list of (country, total_without) aggregated across years,
    excluding aggregate-like names.
    """
    conn = db_designer._connect()
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    etbl = schema["elec_table"]; ctbl = schema["countries_table"]
    if not etbl or not ctbl:
        conn.close(); return []
    ecols = schema["elec_cols"]; ccols = schema["countries_cols"]
    pwe_col = next((c for c in ecols if c and "without" in c.lower()), "people_without_electricity")
    cname_col = next((c for c in ccols if c and "name" in c.lower()), ccols[1] if len(ccols)>1 else ccols[0])
    cid_col = next((c for c in ccols if c and "id" in c.lower()), ccols[0])

    sql = f"""
        SELECT c.{cname_col} AS country, SUM(COALESCE(e.{pwe_col},0)) AS total_without
        FROM {etbl} e
        JOIN {ctbl} c ON e.country_id = c.{cid_col}
        GROUP BY c.{cname_col}
        HAVING total_without > ?
        ORDER BY total_without DESC
    """
    cur.execute(sql, (threshold,))
    rows = []
    for name, total in cur.fetchall():
        if not name:
            continue
        if _is_aggregate(name):
            continue
        try:
            rows.append((name, int(total)))
        except Exception:
            continue
    conn.close()
    return rows

def query_yearly_trend() -> List[Tuple[int,int]]:
    """
    Return list [(year, total_with_people)] using people_with if present,
    otherwise compute (population - people_without) where population is available.
    Results are clamped to >= 0.
    """
    conn = db_designer._connect()
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    etbl = schema["elec_table"]; ptbl = schema["pop_table"]
    if not etbl:
        conn.close(); return []
    ecols = schema["elec_cols"]
    year_col = next((c for c in ecols if c and c.lower()=="year"), "year")
    pwe_col = next((c for c in ecols if c and "without" in c.lower()), "people_without_electricity")
    pwith_col = next((c for c in ecols if c and "with" in c.lower()), None)
    pop_col = next((c for c in ecols if c and "population" in c.lower()), None)

    # build SQL safely depending on which columns exist
    if pwith_col:  # table provides people_with column
        sql = f"""
            SELECT e.{year_col} AS year,
                   SUM(COALESCE(e.{pwith_col}, 0)) AS total_with
            FROM {etbl} e
            GROUP BY e.{year_col}
            ORDER BY e.{year_col}
        """
        cur.execute(sql)
    else:
        # compute from population sources where present
        if ptbl and pop_col:
            # prefer PopulationData but fallback to ElectricityAccess.population
            # compute as SUM( (COALESCE(p.population, e.pop_col,0) - COALESCE(e.pwe_col,0)) )
            sql = f"""
                SELECT e.{year_col} AS year,
                       SUM( (COALESCE(p.population, e.{pop_col}, 0) - COALESCE(e.{pwe_col}, 0)) ) AS total_with
                FROM {etbl} e
                LEFT JOIN {ptbl} p ON p.country_id = e.country_id AND p.year = e.{year_col}
                GROUP BY e.{year_col}
                ORDER BY e.{year_col}
            """
            cur.execute(sql)
        elif pop_col:
            sql = f"""
                SELECT {year_col} AS year,
                       SUM( COALESCE({pop_col},0) - COALESCE({pwe_col},0) ) AS total_with
                FROM {etbl}
                GROUP BY {year_col}
                ORDER BY {year_col}
            """
            cur.execute(sql)
        elif ptbl:
            # population data table exists but electricity table has no population column:
            sql = f"""
                SELECT e.{year_col} AS year,
                       SUM( COALESCE(p.population, 0) - COALESCE(e.{pwe_col},0) ) AS total_with
                FROM {etbl} e
                LEFT JOIN {ptbl} p ON p.country_id = e.country_id AND p.year = e.{year_col}
                GROUP BY e.{year_col}
                ORDER BY e.{year_col}
            """
            cur.execute(sql)
        else:
            # no population or people_with information: return empty list (frontend will handle)
            conn.close(); return []

    rows = cur.fetchall()
    conn.close()
    out = []
    for y, v in rows:
        try:
            vv = int(v) if v is not None else 0
            # clamp negatives to 0 to avoid nonsense totals
            if vv < 0:
                vv = 0
        except Exception:
            vv = 0
        out.append((y, vv))
    return out

def query_access_percent(year: int) -> List[Tuple[str,int,int,float]]:
    """
    Return rows (country, population, without, access_pct) for the given year.
    Excludes aggregate-like names and entries with missing/zero population.
    """
    conn = db_designer._connect()
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    etbl = schema["elec_table"]; ctbl = schema["countries_table"]; ptbl = schema["pop_table"]
    if not etbl or not ctbl:
        conn.close(); return []
    ecols = schema["elec_cols"]; ccols = schema["countries_cols"]

    cname_col = next((c for c in ccols if c and "name" in c.lower()), ccols[1] if len(ccols)>1 else ccols[0])
    pwe_col = next((c for c in ecols if c and "without" in c.lower()), "people_without_electricity")
    elec_pop_col = next((c for c in ecols if c and "population" in c.lower()), None)
    cid_col = next((c for c in ccols if c and "id" in c.lower()), ccols[0])
    year_col = next((c for c in ecols if c and c.lower()=='year'), 'year')

    # Build SQL safely: always select individual columns and let Python compute percent/clamps.
    if ptbl and elec_pop_col:
        sql = f"""
            SELECT c.{cname_col} AS country,
                   COALESCE(p.population, e.{elec_pop_col}) AS population,
                   COALESCE(e.{pwe_col}, 0) AS without
            FROM {etbl} e
            JOIN {ctbl} c ON e.country_id = c.{cid_col}
            LEFT JOIN {ptbl} p ON e.country_id = p.country_id AND e.{year_col} = p.year
            WHERE e.{year_col} = ?
        """
        params = (year,)
    elif ptbl and not elec_pop_col:
        sql = f"""
            SELECT c.{cname_col} AS country,
                   p.population AS population,
                   COALESCE(e.{pwe_col}, 0) AS without
            FROM {etbl} e
            JOIN {ctbl} c ON e.country_id = c.{cid_col}
            LEFT JOIN {ptbl} p ON e.country_id = p.country_id AND e.{year_col} = p.year
            WHERE e.{year_col} = ?
        """
        params = (year,)
    elif elec_pop_col:
        sql = f"""
            SELECT c.{cname_col} AS country,
                   e.{elec_pop_col} AS population,
                   COALESCE(e.{pwe_col}, 0) AS without
            FROM {etbl} e
            JOIN {ctbl} c ON e.country_id = c.{cid_col}
            WHERE e.{year_col} = ?
        """
        params = (year,)
    else:
        # population unknown -> cannot compute percent reliably
        conn.close(); return []

    cur.execute(sql, params)
    rows = []
    for country, pop, without in cur.fetchall():
        if not country:
            continue
        if _is_aggregate(country):
            continue
        if pop is None:
            continue
        try:
            p = int(pop)
            if p <= 0:
                continue
            w = int(without) if without is not None else 0
            # clamp
            if w < 0:
                w = 0
            if w > p:
                w = p
            access_pct = round((p - w) / float(p) * 100.0, 2)
        except Exception:
            continue
        rows.append((country, p, int(w), access_pct))
    conn.close()
    rows.sort(key=lambda x: x[3], reverse=True)
    return rows

def query_regional_comparison(year: int) -> List[Tuple[str,float]]:
    """
    Return (region, access_pct) for each region for the given year.
    Excludes empty region strings.
    """
    conn = db_designer._connect()
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    etbl = schema["elec_table"]; ctbl = schema["countries_table"]; ptbl = schema["pop_table"]
    if not etbl or not ctbl:
        conn.close(); return []
    ecols = schema["elec_cols"]; ccols = schema["countries_cols"]

    region_col = next((c for c in ccols if c and "region" in c.lower()), None)
    if not region_col:
        conn.close(); return []
    pwe_col = next((c for c in ecols if c and "without" in c.lower()), "people_without_electricity")
    elec_pop_col = next((c for c in ecols if c and "population" in c.lower()), None)
    cid_col = next((c for c in ccols if c and "id" in c.lower()), ccols[0])
    year_col = next((c for c in ecols if c and c.lower()=='year'), 'year')

    # Build SQL safely: compute region totals using explicit COALESCE per column and then compute percent in Python
    if ptbl and elec_pop_col:
        sql = f"""
            SELECT TRIM(COALESCE(c.{region_col}, '')) AS region,
                   SUM( COALESCE(p.population, e.{elec_pop_col}, 0) ) AS total_pop,
                   SUM( COALESCE(e.{pwe_col}, 0) ) AS total_without
            FROM {etbl} e
            JOIN {ctbl} c ON e.country_id = c.{cid_col}
            LEFT JOIN {ptbl} p ON p.country_id = e.country_id AND p.year = e.{year_col}
            WHERE e.{year_col} = ?
            GROUP BY TRIM(COALESCE(c.{region_col}, ''))
        """
        params = (year,)
    elif elec_pop_col:
        sql = f"""
            SELECT TRIM(COALESCE(c.{region_col}, '')) AS region,
                   SUM( COALESCE(e.{elec_pop_col}, 0) ) AS total_pop,
                   SUM( COALESCE(e.{pwe_col}, 0) ) AS total_without
            FROM {etbl} e
            JOIN {ctbl} c ON e.country_id = c.{cid_col}
            WHERE e.{year_col} = ?
            GROUP BY TRIM(COALESCE(c.{region_col}, ''))
        """
        params = (year,)
    elif ptbl:
        sql = f"""
            SELECT TRIM(COALESCE(c.{region_col}, '')) AS region,
                   SUM( COALESCE(p.population, 0) ) AS total_pop,
                   SUM( COALESCE(e.{pwe_col}, 0) ) AS total_without
            FROM {etbl} e
            JOIN {ctbl} c ON e.country_id = c.{cid_col}
            LEFT JOIN {ptbl} p ON p.country_id = e.country_id AND p.year = e.{year_col}
            WHERE e.{year_col} = ?
            GROUP BY TRIM(COALESCE(c.{region_col}, ''))
        """
        params = (year,)
    else:
        conn.close(); return []

    cur.execute(sql, params)
    results = []
    for region, total_pop, total_without in cur.fetchall():
        if not region:
            continue
        if total_pop is None or total_pop == 0:
            continue
        try:
            # clamp and compute safely
            tp = float(total_pop)
            tw = float(total_without) if total_without is not None else 0.0
            if tw < 0:
                tw = 0.0
            if tw > tp:
                tw = tp
            access_pct = round((tp - tw) / tp * 100.0, 2)
            if access_pct < 0:
                access_pct = 0.0
            if access_pct > 100.0:
                access_pct = 100.0
        except Exception:
            continue
        results.append((region, access_pct))
    conn.close()
    results.sort(key=lambda x: x[1], reverse=True)
    return results

def query_two_country_compare(year: int, c1: str, c2: str):
    """
    Returns [(country, access_pct, population, with_people, without_people), ...]
    Uses query_access_percent() as the canonical source and therefore inherits its filtering.
    """
    rows = query_access_percent(year)
    lookup = {r[0].strip().lower(): r for r in rows}
    out = []
    for cname in (c1, c2):
        key = cname.strip().lower()
        if key in lookup:
            name, pop, without, access_pct = lookup[key]
            with_people = max(int(pop) - int(without), 0) if pop is not None and without is not None else None
            out.append((name, access_pct, pop if pop is not None else "None",
                        with_people if with_people is not None else "None",
                        without if without is not None else "None"))
        else:
            out.append((cname, None, "None", "None", "None"))
    return out

def query_most_improved(start_year: int = 1990, end_year: int = 2016) -> List[Tuple[str,float,float,float]]:
    """
    Compute access% at start_year and end_year for each country and return
    (country, access_start, access_end, improvement). Skip aggregates and
    countries missing reliable population or 'without' data for either year.
    """
    conn = db_designer._connect()
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    etbl = schema["elec_table"]; ctbl = schema["countries_table"]; ptbl = schema["pop_table"]
    if not etbl or not ctbl:
        conn.close(); return []
    ccols = schema["countries_cols"]; ecols = schema["elec_cols"]
    cid_col = next((c for c in ccols if c and "id" in c.lower()), ccols[0])
    cname_col = next((c for c in ccols if c and "name" in c.lower()), ccols[1] if len(ccols)>1 else ccols[0])
    pwe_col = next((c for c in ecols if c and "without" in c.lower()), "people_without_electricity")
    pop_col = next((c for c in ecols if c and "population" in c.lower()), None)
    year_col = next((c for c in ecols if c and c.lower()=='year'), 'year')

    # helper to fetch (pop, without) for a given country and year
    def get_pop_without(cid, y):
        pop_val = None; without_val = None
        if ptbl:
            cur.execute(f"SELECT population FROM {ptbl} WHERE country_id=? AND year=?", (cid, y))
            q = cur.fetchone(); pop_val = q[0] if q else None
        if pop_val is None and pop_col:
            cur.execute(f"SELECT {pop_col} FROM {etbl} WHERE country_id=? AND {year_col}=?", (cid, y))
            q = cur.fetchone(); pop_val = q[0] if q else None
        if pwe_col:
            cur.execute(f"SELECT {pwe_col} FROM {etbl} WHERE country_id=? AND {year_col}=?", (cid, y))
            q = cur.fetchone(); without_val = q[0] if q else None
        return pop_val, without_val

    cur.execute(f"SELECT {cid_col}, {cname_col} FROM {ctbl} ORDER BY {cname_col}")
    countries = cur.fetchall()
    results = []
    for cid, cname in countries:
        if not cname:
            continue
        if _is_aggregate(cname):
            continue
        pop_s, without_s = get_pop_without(cid, start_year)
        pop_e, without_e = get_pop_without(cid, end_year)
        try:
            if pop_s is None or pop_e is None:
                continue
            p_s = int(pop_s); p_e = int(pop_e)
            if p_s <= 0 or p_e <= 0:
                continue
            w_s = int(without_s) if without_s is not None else 0
            w_e = int(without_e) if without_e is not None else 0
            # clamp inconsistent values
            if w_s < 0: w_s = 0
            if w_e < 0: w_e = 0
            if w_s > p_s: w_s = p_s
            if w_e > p_e: w_e = p_e
            access_s = round((p_s - w_s) / float(p_s) * 100.0, 2)
            access_e = round((p_e - w_e) / float(p_e) * 100.0, 2)
            improvement = round(access_e - access_s, 2)
            results.append((cname, access_s, access_e, improvement))
        except Exception:
            continue
    conn.close()
    results.sort(key=lambda x: x[3], reverse=True)
    return results
