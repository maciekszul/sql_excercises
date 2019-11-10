import sqlite3
from sqlite3 import Error

"""
Creating a SQL database using a JSON dataset with personal information
Following the: 
https://www.sqlitetutorial.net/sqlite-python/creating-database/
https://sebastianraschka.com/Articles/2014_sqlite_in_python_tutorial.html
https://docs.python.org/2/library/sqlite3.html
"""


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


db_path = "test_database.db"

"""
Creating the empty table from the dictionary
key - name of the header, preferably correspondent to the 
INTEGER: A signed integer up to 8 bytes depending on the magnitude of the value.
REAL: An 8-byte floating point value.
TEXT: A text string, typically UTF-8 encoded (depending on the database encoding).
BLOB: A blob of data (binary large object) for storing binary data.
NULL: A NULL value, represents missing data or an empty cell.

"""

table_headers_type = {
    "PRIMARY KEY": {"Subject_ID":"TEXT"},
    # "Subject_ID":"TEXT", 
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

table_name = "personal_info"


# establish a connection with the database
connection = db_connection(db_path)
cursor = connection.cursor()


column_pk = list(table_headers_type["PRIMARY KEY"].keys())[0]
type_pk = table_headers_type["PRIMARY KEY"][column_pk]
create_table_content = "CREATE TABLE {table_name} ({new_column} {field_type} PRIMARY KEY)".format(
    table_name=table_name,
    new_column=column_pk,
    field_type=type_pk
)

try:
    cursor.execute(create_table_content)
except Error as error:
    print(error)

for key_pick in table_headers_type.keys():
    if key_pick == "PRIMARY KEY":
        create_table_content = ""
    else:
        create_table_content = "ALTER TABLE {table_name} ADD COLUMN '{new_column}' {field_type}".format(
            table_name=table_name,
            new_column=key_pick,
            field_type=table_headers_type[key_pick]
        )

    try:
        cursor.execute(create_table_content)
    except Error as error:
        print(error)

connection.close()