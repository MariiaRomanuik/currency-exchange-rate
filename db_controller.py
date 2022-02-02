import pandas as pd
import psycopg2


def create_database(name):
    cursor.execute(f'''CREATE database {name}''')
    print("Database created successfully......")


def create_table(name):
    cursor.execute(f'''CREATE TABLE {name}(data varchar NOT NULL, rate float)''')
    print(f"Table - {name} crated successfully")


if __name__ == "__main__":
    conn = psycopg2.connect(
        database="postgres", user='postgres', password='posql', host='localhost', port='5432'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    # create_table("CURRENCY_RATE")
    cursor.execute('''select * from CURRENCY_RATE;''')
    for i in cursor.fetchall():
        print(i)

    conn.close()
