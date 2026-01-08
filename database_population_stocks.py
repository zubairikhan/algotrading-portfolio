import sqlite3

import config
import csv

records = []
filepath = ''
with open(filepath, 'r') as file:
    reader = csv.reader(file)
    headers = next(reader)  # Get the headers
    print("Headers:", headers)
    for i, row in enumerate(reader):
        records.append((row[0], row[1], row[8], 0))  # Adjust based on needed columns


db_conn = sqlite3.connect(config.db_management_engine_name)
db_cursor = db_conn.cursor()
db_cursor.executemany("""
INSERT OR IGNORE INTO stocks (symbol, name, stock_float, is_blacklisted, created_at, modified_at)
VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
""" , records)
db_conn.commit()
db_conn.close()


