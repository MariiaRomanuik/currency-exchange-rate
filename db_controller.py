import os
import psycopg2
from smart_open import smart_open
import pandas as pd


def create_database(name, cursor):
    try:
        cursor.execute(f'''CREATE database {name}''')
        return "Database created successfully......"
    except Exception as e:
        return e


def create_table(name, cursor):
    try:
        cursor.execute(f'''CREATE TABLE {name}(date varchar NOT NULL CONSTRAINT date UNIQUE, rate varchar)''')
        return f"Table - {name} crated successfully"
    except Exception as e:
        return e


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
    with smart_open(f's3://{bucketName}/{fileName}', 'rb') as s3_source:
        for line in s3_source:
            # print(line.decode('utf8'))

            # TODO: write without first line
            rate = line.decode('utf8').split(",")[1]
            date = line.decode('utf8').split(",")[2].split("\r\n")[0]
            try:
                postgres_insert_query = """ INSERT INTO CURRENCY_RATE (date, rate) VALUES (%s, %s)"""
                cursor.execute(postgres_insert_query, (rate, date,))
            except psycopg2.Error as e:
                print(e)
                pass
        connection.commit()
        return "Data successfully written to database!"


def get_data_from_db(cursor, connection):
    try:
        select_date_query = """ SELECT "date" FROM CURRENCY_RATE"""
        cursor.execute(select_date_query)
        date = cursor.fetchall()
        select_rate_query = """ SELECT "rate" FROM CURRENCY_RATE"""
        cursor.execute(select_rate_query)
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
        return df
    except Exception as e:
        return e


# if __name__ == "__main__":
#     file_name = "currency.csv"
#     csv_file_name = f'data/{file_name}'
#     bucket_name = 's3-all-data'
#
#     conn = connect_to_db()
#     conn.autocommit = True
#     cursor = conn.cursor()
#     # print(create_table("CURRENCY_RATE"))
#     # print(from_s3_to_postgres(bucket_name, file_name, cursor, conn))
#     dataframe = get_data_from_db(cursor, conn)
#     conn.close()
