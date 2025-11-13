# backend.py
# Acts as the interface between frontend.py and db_designer.py

import db_designer

# Ensure database is created
db_designer.init_db()

# ----------------------------
# CRUD Wrappers
# ----------------------------

def add_country(name, region=None):
    db_designer.add_country(name, region)

def get_countries():
    return db_designer.get_countries()

def add_electricity_record(country_id, year, people_without, people_with=None):
    db_designer.add_record(country_id, year, people_without, people_with)

def get_electricity_records():
    return db_designer.get_records()

def update_electricity_record(record_id, pwe=None, pwe_with=None):
    db_designer.update_record(record_id, pwe, pwe_with)

def delete_electricity_record(record_id):
    db_designer.delete_record(record_id)

# ----------------------------
# Analytical Queries
# ----------------------------

def query_high_unserved(threshold):
    return db_designer.get_high_unserved_countries(threshold)

def query_yearly_trend():
    return db_designer.get_yearly_access_trend()

def query_access_percent(year):
    return db_designer.get_access_percentage_by_country(year)

def query_regional_comparison(year):
    return db_designer.get_regional_access_comparison(year)

def query_most_improved():
    return db_designer.get_most_improved_countries()
