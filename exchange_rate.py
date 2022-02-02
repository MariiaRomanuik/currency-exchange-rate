import csv

import requests
import moment
import pandas as pd


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
    return {k: sorted_frequency[k] for k in list(sorted_frequency)[:5]}


def exchange_rate_analysis(df):
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
        month = get_list_of_data(response)[0][i][3:5]
        month_list.append(month)

    data_month_dict = tuple(zip(month_list, get_list_of_data(response)[1]))
    data_month_list = list(data_month_dict)
    df_month = pd.DataFrame(data_month_list, columns=["month", "rate"])
    grouped_data = df_month.groupby(["rate"]).mean()
    print(grouped_data)

    favorable_year_rate = grouped_data["rate"].min()
    print("Favorable year rate", favorable_year_rate)
    # favorable_month = grouped_data[grouped_data["rate"] == favorable_year_rate]
    # print("The most favorable average monthly rate for the year was:", favorable_month, favorable_year_rate)

    most_frequent = most_frequent_rates(df)
    print(most_frequent)


def dataframe_to_csv(csv_name, dictionary):
    df = pd.DataFrame(dictionary)
    csv_file = df.to_csv(f'{csv_name}.csv', encoding='utf-8')
    print("Data successfully written to csv")
    return csv_file


if __name__ == "__main__":
    start_date = moment.date('2021-01-01')
    end_date = moment.date('2022-01-01')
    dates = list(dates_between_two_dates(start_date, end_date))
    list_of_url = list(map(get_url, dates))
    response = list(map(lambda url: requests.get(url).json(), list_of_url))
    data_dict = {"date": get_list_of_data(response)[0], "rate": get_list_of_data(response)[1]}
    csv_file_name = "currency.csv"
    dataframe_to_csv(csv_file_name, data_dict)



    # dict_to_csv(data_dict, file_name, columns)
    # dataframe = pd.DataFrame(data_items, columns=["data", "rate"])
    # csv_data = to_csv(dataframe, "currency")
    # exchange_rate_analysis(dataframe)

    # Question: df to csv or  csv to df?
    # l did df to csv, but l need to write from csv to database then read tas dataframe

    # json to csv,  csv to s3, s3 to postrgres,  postrger read as dataframe
