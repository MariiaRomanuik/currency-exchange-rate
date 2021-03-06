import datetime
import os
import psycopg2
from smart_open import smart_open
import pandas as pd


def create_database(name, cursor):
    try:
        cursor.execute(f'''CREATE database {name}''')
        print("Database created successfully......")
    except psycopg2.errors.DuplicateDatabase as e:
        print(e)
        pass


def create_table(name, cursor):
    try:
        cursor.execute(f'''CREATE TABLE {name}(date varchar NOT NULL CONSTRAINT date UNIQUE, rate varchar)''')
        print(f"Table - {name} crated successfully")
    except psycopg2.errors.DuplicateTable as e:
        print(e)
        pass


def get_db_credentials(path):
    try:
        with open(path, "r") as myfile:
            data = myfile.readlines()
    except Exception as e:
        raise e
    else:
        var_dict = {}
        for var in data:
            var = var.replace('\n', '').split('=')
            var_dict[var[0]] = var[1]

        return var_dict


def connect_to_db():
    try:
        credentials = get_db_credentials("./db_credentials.txt")
        connection = psycopg2.connect(
            host=credentials["host"],
            port=credentials["port"],
            user=credentials["user"],
            password=credentials["password"],
            database=credentials["database"]
        )
        print('Database connected')
        return connection
    except Exception as e:
        print('Database not connected')
        raise e


def from_s3_to_postgres(bucketName, fileName, cursor, connection):
    try:
        with smart_open(f's3://{bucketName}/{fileName}', 'rb') as s3_source:
            for line in s3_source:
                rate = line.decode('utf8').split(",")[1]
                date = line.decode('utf8').split(",")[2].split("\r\n")[0]
                if date == "rate":
                    continue
                try:
                    postgres_insert_query = """ INSERT INTO CURRENCY_RATE (date, rate) VALUES (%s, %s)"""
                    cursor.execute(postgres_insert_query, (rate, date,))
                    # delete_query = """DELETE FROM CURRENCY_RATE WHERE date = 'date'"""
                    # cursor.execute(delete_query)
                except psycopg2.Error as e:
                    print(e)
                    pass
            connection.commit()
            return "Data successfully written to database!"
    except Exception as e:
        return e


def get_data_from_db(cursor, connection, start_date, end_date, rate_from, rate_to):
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').strftime('%d.%m.%Y')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').strftime('%d.%m.%Y')
    try:
        select_date_query = """ SELECT date FROM CURRENCY_RATE WHERE date BETWEEN %s AND %s"""
        cursor.execute(select_date_query, (start_date,  end_date))
        date = cursor.fetchall()
        # select_rate_query = """ SELECT "rate" FROM CURRENCY_RATE WHERE "rate" BETWEEN rate_from and rate_to"""
        select_rate_query = """SELECT rate FROM (SELECT date, rate FROM currency_rate cr2 
                               WHERE date BETWEEN %s AND %s) a
                               WHERE rate BETWEEN %s AND %s"""
        cursor.execute(select_rate_query, (start_date, end_date, rate_from, rate_to))
        rate = cursor.fetchall()
        date_list = []
        rate_list = []
        for i, j in zip(date, rate):
            date_list.append(i[0])
            rate_list.append(float(j[0]))
        data_dict = dict(zip(date_list, rate_list))
        data_items = list(data_dict.items())
        df = pd.DataFrame(data_items, columns=["date", "rate"])
        connection.commit()
        return df, date_list, rate_list
    except Exception as e:
        return e

