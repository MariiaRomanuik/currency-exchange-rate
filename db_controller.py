import pandas as pd
import psycopg2

conn = psycopg2.connect(
    database="postgres", user='postgres', password='posql', host='localhost', port='5432'
)
conn.autocommit = True
cursor = conn.cursor()


def create_database(name):
    create_db = f'''CREATE database {name}''';
    cursor.execute(create_db)
    print("Database created successfully......")


def crate_table(name):
    sql = f'''CREATE TABLE {name}(data str NOT NULL, rate float;'''
    cursor.execute(sql)
    print(f"Table - {name} crated successfully")



# sql3 = '''select * from details;'''
# cursor.execute(sql3)
# for i in cursor.fetchall():
#     print(i)

conn.close()