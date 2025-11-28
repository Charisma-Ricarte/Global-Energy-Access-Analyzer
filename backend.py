# backend.py
import sqlite3
from typing import List, Tuple, Optional
import db_designer

# Ensure DB exists and migrations are applied
db_designer.init_db()

# Keywords treated as aggregates (lowercase)
AGGREGATE_WORDS = {
    "world", "income", "ida", "ibrd", "oecd", "region", "regions", "asia",
    "africa", "europe", "america", "caribbean", "pacific", "blend", "only",
    "total", "demographic", "fragile", "hipc", "high income", "low income",
    "middle income", "small states", "least", "developed", "developing"
}

def _is_aggregate(name: Optional[str]) -> bool:
    if not name:
        return True
    ln = str(name).strip().lower()
    return any(k in ln for k in AGGREGATE_WORDS)

# --------------------------
# Country CRUD
# --------------------------
def get_countries() -> List[Tuple]:
    return db_designer.get_countries()

def add_country(name: str, region: Optional[str] = None):
    return db_designer.add_country(name, region)

# --------------------------
# Electricity Records CRUD
# --------------------------
def get_electricity_records(search: Optional[str] = None) -> List[Tuple]:
    """Return electricity records joined with country names and population from PopulationData table."""
    raw = db_designer.get_records()
    out = []
    for r in raw:
        rec_id = r[0] if len(r) > 0 else None
        country = r[1] if len(r) > 1 else None
        year = r[2] if len(r) > 2 else None
        population = r[3] if len(r) > 3 else None
        without = r[4] if len(r) > 4 else 0
        with_ = r[5] if len(r) > 5 else None

        def to_int_safe(v):
            try:
                if v is None: return None
                return int(float(v))
            except:
                return None

        pop_val = to_int_safe(population)
        without_val = to_int_safe(without) if without is not None else 0
        with_val = to_int_safe(with_) if with_ is not None else max(pop_val - without_val, 0) if pop_val else None

        # Filter aggregates
        if country and not _is_aggregate(country):
            out.append((rec_id, country, year, pop_val, without_val, with_val))

    if search:
        s = search.strip().lower()
        out = [row for row in out if row[1] and s in row[1].lower()]

    # Sort by record_id ascending
    out.sort(key=lambda x: x[0] if x[0] is not None else 0)
    return out

def add_electricity_record(country_name: str, year: int, people_without: int,
                           people_with: Optional[int] = None, population: Optional[int] = None):
    cid = None
    for c in db_designer.get_countries():
        if len(c) >= 2 and c[1] and c[1].strip().lower() == country_name.strip().lower():
            cid = c[0]
            break

    if cid is None:
        db_designer.add_country(country_name, None)
        for c in db_designer.get_countries():
            if len(c) >= 2 and c[1] and c[1].strip().lower() == country_name.strip().lower():
                cid = c[0]
                break
    if cid is None:
        raise RuntimeError("Failed to create or resolve country")

    db_designer.add_record(cid, year, people_without, people_with)
    if population is not None:
        db_designer.add_population(cid, year, population)

def update_electricity_record(record_id: int, without: Optional[int] = None,
                              with_people: Optional[int] = None, population: Optional[int] = None):
    db_designer.update_record(record_id, pwe=without, pwe_with=with_people)
    if population is not None:
        conn = db_designer._connect()
        cur = conn.cursor()
        cur.execute("SELECT country_id, year FROM ElectricityAccess WHERE record_id=?", (record_id,))
        q = cur.fetchone()
        if q:
            cid, yr = q
            db_designer.add_population(cid, yr, population)
        conn.close()

def delete_electricity_record(record_id: int):
    db_designer.delete_record(record_id)

# --------------------------
# Analytical Queries
# --------------------------
def query_high_unserved(threshold: int = 1_000_000) -> List[Tuple[str, int]]:
    conn = db_designer._connect()
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    etbl, ctbl = schema["elec_table"], schema["countries_table"]
    if not etbl or not ctbl:
        conn.close()
        return []

    ecols, ccols = schema["elec_cols"], schema["countries_cols"]
    pwe_col = next((c for c in ecols if "without" in c.lower()), "people_without_electricity")
    cname_col = next((c for c in ccols if "name" in c.lower()), ccols[1] if len(ccols) > 1 else ccols[0])
    cid_col = next((c for c in ccols if "id" in c.lower()), ccols[0])

    sql = f"""
        SELECT c.{cname_col} AS country, SUM(COALESCE(e.{pwe_col},0)) AS total_without
        FROM {etbl} e
        JOIN {ctbl} c ON e.country_id = c.{cid_col}
        GROUP BY c.{cname_col}
        HAVING total_without > ?
        ORDER BY total_without DESC
    """
    cur.execute(sql, (threshold,))
    rows = [(name, int(total)) for name, total in cur.fetchall() if name and not _is_aggregate(name)]
    conn.close()
    return rows

def query_yearly_trend() -> List[Tuple[int, int]]:
    conn = db_designer._connect()
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    etbl, ptbl = schema["elec_table"], schema["pop_table"]
    ecols = schema["elec_cols"]
    year_col = next((c for c in ecols if "year" in c.lower()), "year")
    pwe_col = next((c for c in ecols if "without" in c.lower()), "people_without_electricity")
    pwith_col = next((c for c in ecols if "with" in c.lower() and "without" not in c.lower()), None)

    if pwith_col:
        sql = f"SELECT {year_col}, SUM(COALESCE({pwith_col},0)) FROM {etbl} GROUP BY {year_col} ORDER BY {year_col}"
        cur.execute(sql)
    elif ptbl:
        sql = f"""
            SELECT e.{year_col}, SUM(COALESCE(p.population,0) - COALESCE(e.{pwe_col},0))
            FROM {etbl} e
            LEFT JOIN {ptbl} p ON e.country_id=p.country_id AND e.{year_col}=p.year
            GROUP BY e.{year_col} ORDER BY e.{year_col}
        """
        cur.execute(sql)
    else:
        conn.close()
        return []

    out = []
    for y, v in cur.fetchall():
        try: vv = max(int(v),0)
        except: vv = 0
        out.append((y,vv))
    conn.close()
    return out

def query_access_percent(year: int) -> List[Tuple[str,int,int,float]]:
    conn = db_designer._connect()
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    etbl, ctbl, ptbl = schema["elec_table"], schema["countries_table"], schema["pop_table"]
    if not etbl or not ctbl:
        conn.close()
        return []

    ecols, ccols = schema["elec_cols"], schema["countries_cols"]
    cname_col = next((c for c in ccols if "name" in c.lower()), "country_name")
    cid_col = next((c for c in ccols if "id" in c.lower()), "country_id")
    pwe_col = next((c for c in ecols if "without" in c.lower()), "people_without_electricity")
    year_col = next((c for c in ecols if c.lower() == "year"), "year")

    sql = f"""
        SELECT c.{cname_col} AS country, COALESCE(p.population,0) AS population,
               COALESCE(e.{pwe_col},0) AS without
        FROM {etbl} e
        JOIN {ctbl} c ON e.country_id=c.{cid_col}
        LEFT JOIN {ptbl} p ON e.country_id=p.country_id AND e.{year_col}=p.year
        WHERE e.{year_col}=? 
    """
    cur.execute(sql,(year,))
    rows = []
    for country, pop, without in cur.fetchall():
        if not country or not pop or pop <= 0 or _is_aggregate(country):
            continue
        without = min(max(without or 0,0), pop)
        access_pct = round((pop-without)/pop*100,2)
        rows.append((country,pop,without,access_pct))
    conn.close()
    rows.sort(key=lambda x:x[3], reverse=True)
    return rows

def query_regional_comparison(year:int)->List[Tuple[str,float]]:
    conn = db_designer._connect()
    cur = conn.cursor()
    schema = db_designer._detect_schema()
    etbl, ctbl, ptbl = schema["elec_table"], schema["countries_table"], schema["pop_table"]
    if not etbl or not ctbl:
        conn.close()
        return []

    ecols, ccols = schema["elec_cols"], schema["countries_cols"]
    region_col = next((c for c in ccols if "region" in c.lower()), None)
    if not region_col:
        conn.close()
        return []

    pwe_col = next((c for c in ecols if "without" in c.lower()), "people_without_electricity")
    cid_col = next((c for c in ccols if "id" in c.lower()), ccols[0])
    year_col = next((c for c in ecols if c.lower()=='year'), 'year')

    sql = f"""
        SELECT TRIM(COALESCE(c.{region_col},'')) AS region,
               SUM(COALESCE(p.population,e.{pwe_col}+0)) AS total_pop,
               SUM(COALESCE(e.{pwe_col},0)) AS total_without
        FROM {etbl} e
        JOIN {ctbl} c ON e.country_id=c.{cid_col}
        LEFT JOIN {ptbl} p ON e.country_id=p.country_id AND e.{year_col}=p.year
        WHERE e.{year_col}=?
        GROUP BY TRIM(COALESCE(c.{region_col},'')) 
    """
    cur.execute(sql,(year,))
    results=[]
    for region,total_pop,total_without in cur.fetchall():
        if not region or not total_pop or total_pop<=0:
            continue
        tw=min(max(total_without or 0,0),total_pop)
        access_pct=round((total_pop-tw)/total_pop*100.0,2)
        results.append((region,access_pct))
    conn.close()
    results.sort(key=lambda x:x[1],reverse=True)
    return results

def query_two_country_compare(year:int, c1:str, c2:str):
    rows=query_access_percent(year)
    lookup={r[0].strip().lower():r for r in rows}
    out=[]
    for cname in (c1,c2):
        key=cname.strip().lower()
        if key in lookup:
            name,pop,without,access_pct=lookup[key]
            with_people=max(pop-without,0)
            out.append((name,access_pct,pop,with_people,without))
        else:
            out.append((cname,None,"None","None","None"))
    return out

def query_most_improved(start_year:int=1990,end_year:int=2016)->List[Tuple[str,float,float,float]]:
    conn=db_designer._connect()
    cur=conn.cursor()
    schema=db_designer._detect_schema()
    etbl,ctbl,ptbl=schema["elec_table"],schema["countries_table"],schema["pop_table"]
    if not etbl or not ctbl:
        conn.close()
        return []

    ecols,ccols=schema["elec_cols"],schema["countries_cols"]
    cid_col=next((c for c in ccols if "id" in c.lower()), ccols[0])
    cname_col=next((c for c in ccols if "name" in c.lower()), ccols[1] if len(ccols)>1 else ccols[0])
    pwe_col=next((c for c in ecols if "without" in c.lower()), "people_without_electricity")
    year_col=next((c for c in ecols if c.lower()=='year'), 'year')

    def get_pop_without(cid,y):
        pop_val,without_val=None,None
        if ptbl:
            cur.execute(f"SELECT population FROM {ptbl} WHERE country_id=? AND year=?",(cid,y))
            q=cur.fetchone()
            pop_val=q[0] if q else None
        cur.execute(f"SELECT {pwe_col} FROM {etbl} WHERE country_id=? AND {year_col}=?",(cid,y))
        q=cur.fetchone()
        without_val=q[0] if q else 0
        if pop_val is None: pop_val=without_val
        return pop_val,without_val

    results=[]
    for c in db_designer.get_countries():
        cid,cname=c[0],c[1]
        if _is_aggregate(cname): continue
        start_pop,start_without=get_pop_without(cid,start_year)
        end_pop,end_without=get_pop_without(cid,end_year)
        if not start_pop or not end_pop: continue
        start_access=max(0,(start_pop-start_without)/start_pop*100.0)
        end_access=max(0,(end_pop-end_without)/end_pop*100.0)
        diff=round(end_access-start_access,2)
        results.append((cname,round(start_access,2),round(end_access,2),diff))
    results.sort(key=lambda x:x[3],reverse=True)
    conn.close()
    return results
