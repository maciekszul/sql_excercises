import sqlite3
from sqlite3 import Error
from sqlfunc import db_connection
import pandas as pd
import numpy as np

db_path = "test_database.db"
table_name = "personal_info"

connection = db_connection(db_path)
cursor = connection.cursor()

"""
SELECT, AGGREGATE, EXPORT
https://stackabuse.com/a-sqlite-tutorial-with-python/
"""

connection.commit()
connection.close()
