import sqlite3
from sqlite3 import Error
import json


def db_connection(path, *args):
    """
    Connect with the database.
    :path: path to the database (*.db file), using ':memory:' a database 
    is created in RAM.
    """
    print(sqlite3.version)
    connection = None
    try:
        connection = sqlite3.connect(path, *args)
        print("Connected to {}".format(path))
    except Error as error:
        print(error)
    
    return connection

"""
Creating a SQL database using a JSON dataset with personal information
Following the: 
https://www.sqlitetutorial.net/sqlite-python/creating-database/
https://sebastianraschka.com/Articles/2014_sqlite_in_python_tutorial.html
https://docs.python.org/2/library/sqlite3.html
"""

table_headers_type = {
    "PRIMARY KEY": {"Subject_ID":"TEXT"},
    "first_name": "TEXT", 
    "last_name": "TEXT", 
    "age": "INTEGER", 
    "birthday": "TEXT", 
    "email": "TEXT", 
    "city": "TEXT", 
    "street": "TEXT", 
    "zip_code": "INTEGER", 
    "condition": "INTEGER", 
    "performance": "INTEGER", 
    "comments": "TEXT",
    "responses": "INTEGER"
}

db_path = "test_database.db"
table_name = "personal_info"

json_path = "generated_dataset.json"

with open(json_path) as json_data:
    data = json.load(json_data)

connection = db_connection(db_path)
cursor = connection.cursor()


for row in data[:70]:
    id_pk = list(table_headers_type["PRIMARY KEY"].keys())[0]
    id_val = row[id_pk]
    table_update = "INSERT OR IGNORE INTO {table_name} ({column_name}) VALUES (?)".format(
        table_name=table_name,
        column_name=id_pk
    )
    cursor.execute(table_update, (id_val,))
    for key in row.keys():
        if key != "PRIMARY KEY":
            table_update = "UPDATE {table_name} SET {column_name}=(?) WHERE {pk_name}=(?)".format(
                table_name=table_name,
                column_name=key,
                pk_name=id_pk
            )
            cursor.execute(table_update, (row[key], id_val,))

connection.commit()
connection.close()