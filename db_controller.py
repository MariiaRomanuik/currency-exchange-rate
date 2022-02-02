import os
import psycopg2


def create_database(name):
    cursor.execute(f'''CREATE database {name}''')
    print("Database created successfully......")


def create_table(name):
    cursor.execute(f'''CREATE TABLE {name}(data varchar NOT NULL, rate float)''')
    print(f"Table - {name} crated successfully")


def get_db_creds(path):
    data = []
    with open(path, "r") as myfile:
        data = myfile.readlines()

    var_dict = {}
    for var in data:
        var = var.replace('\n', '').split('=')
        var_dict[var[0]] = var[1]

    return var_dict


def connect_to_db():
    creds = get_db_creds("./db_credentials.txt")
    connection = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        user=creds["user"],
        password=creds["password"],
        database=creds["database"]
    )
    return connection


if __name__ == "__main__":
    conn = connect_to_db()
    conn.autocommit = True
    cursor = conn.cursor()
    # create_table("CURRENCY_RATE")
    cursor.execute('''select * from CURRENCY_RATE;''')
    for i in cursor.fetchall():
        print(i)

    conn.close()
