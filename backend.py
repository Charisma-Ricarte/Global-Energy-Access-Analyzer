# backend.py
# Backend API layer connecting GUI to db_designer

import db_designer

db_designer.init_db()

# CRUD
def add_electricity_record(country_id, year, pwe, pwe_with=None):
    db_designer.add_record(country_id, year, pwe, pwe_with)

def get_electricity_records():
    return db_designer.get_records()

def delete_electricity_record(record_id):
    db_designer.delete_record(record_id)

# Analytical queries
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
