import csv
import errno
import os
import requests
import moment
import pandas as pd

from aws_controller import create_bucket, upload_file_to_s3, is_in_s3
from db_controller import connect_to_db, get_data_from_db, create_database, create_table, from_s3_to_postgres


def dates_between_two_dates(startDate, endDate):
    diff = abs(startDate.diff(endDate).days)
    for n in range(0, diff):
        yield startDate.strftime("%Y%m%d")
        startDate = startDate.add(days=1)


def get_url(date):
    return f"https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?date={date}&json"


def get_list_of_data(data):
    list_of_rate = []
    list_of_date = []
    for item in data:
        for i in range(len(item)):
            if item[i]["cc"] == "USD":
                rate = item[i]["rate"]
                exchange_date = item[i]["exchangedate"]
                list_of_rate.append(rate)
                list_of_date.append(exchange_date)
    return list_of_date, list_of_rate


def most_frequent_rates(df):
    rates_frequency = {}
    for index, row in df.iterrows():
        if str(row['rate']) in rates_frequency:
            rates_frequency[str(row['rate'])] += 1
        else:
            rates_frequency[str(row['rate'])] = 1

    sorted_frequency = {k: v for k, v in sorted(rates_frequency.items(), key=lambda item: item[1], reverse=True)}
    rates_dict = {k: sorted_frequency[k] for k in list(sorted_frequency)[:5]}
    for key, value in zip(rates_dict.keys(), rates_dict.values()):
        print(f"{key} - {value} times")


def exchange_rate_analysis(df):
    try:
        average_for_year = df["rate"].mean()
        print("Average exchange rate for the year:", average_for_year)

        median_for_year = df["rate"].median()
        print("Median exchange rate:", median_for_year)

        max_for_year = df["rate"].max()
        print("The maximum value of the course for the year:", max_for_year)

        min_for_year = df["rate"].min()
        print("The minimum value of the course for the year:", min_for_year)

        month_list = []
        for i in range(0, len(df)):
            month = df["date"][i][3:5]
            month_list.append(month)
        df["month_number"] = month_list
        grouped_data = df.groupby(["month_number"]).mean()
        favorable_year_rate = grouped_data.nsmallest(1, "rate")
        print("Favorable year rate:", favorable_year_rate["rate"][0])
        print("Most frequent rates: ")
        most_frequent_rates(df)
    except ValueError as e:
        return e


def write_dataframe_to_csv(csv_name, dataframe):
    if not os.path.exists(os.path.dirname(csv_name)):
        try:
            os.makedirs(os.path.dirname(csv_name))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
    csv_file = dataframe.to_csv(csv_name, encoding='utf-8')
    print("Data successfully written to csv")
    return csv_file


if __name__ == "__main__":
    start_date = '2020-02-01'
    end_date = '2021-03-01'
    file_name = f"{start_date}-{end_date}.csv"
    folder_name = "data"
    path_to_csv_file = f'./{folder_name}/{file_name}'
    bucket_name = 's3-all-data'
    s3_path_to_file = f's3://{bucket_name}/{file_name}'
    region = 'us-east-2'
    # if not os.path.isfile(path_to_csv_file):  # check not local if it is exist, but in s3
    if not is_in_s3(file_name, bucket_name):
        start_moment_date = moment.date(start_date)
        end_moment_date = moment.date(end_date)
        dates = list(dates_between_two_dates(start_moment_date, end_moment_date))
        list_of_url = list(map(get_url, dates))
        response = list(map(lambda url: requests.get(url).json(), list_of_url))
        data_dict = {"date": get_list_of_data(response)[0], "rate": get_list_of_data(response)[1]}
        df = pd.DataFrame(data_dict)
        write_dataframe_to_csv(path_to_csv_file, df)
        create_bucket(bucket_name, region)
        upload_file_to_s3(path_to_csv_file, bucket_name)

    conn = connect_to_db()
    conn.autocommit = True
    cursor = conn.cursor()
    create_database("postgres", cursor)
    create_table("CURRENCY_RATE", cursor)
    print(from_s3_to_postgres(bucket_name, file_name, cursor, conn))
    # dataframe = get_data_from_db(cursor, conn)
    # print(dataframe)
    labels = get_data_from_db(cursor, conn)[1]
    values = get_data_from_db(cursor, conn)[2]
    print("labels", labels)
    print("values", values)

    # exchange_rate_analysis(dataframe)
    conn.close()

