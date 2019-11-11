import sqlite3
from sqlite3 import Error


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
