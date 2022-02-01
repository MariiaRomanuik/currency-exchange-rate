import requests
import moment
import pandas as pd


def dates_between_two_dates(startDate, endDate):
    diff = abs(startDate.diff(endDate).days)
    for n in range(0, diff):
        yield startDate.strftime("%Y%m%d")
        startDate = startDate.add(days=1)


def usd_data(json_object):
    return filter(lambda obj: obj['currency'] == "USD", json_object)


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


def exchange_rate_analysis(df):
    average_for_year = df["rate"].mean()  # 27.283450273224044
    print("Average exchange rate for the year:", average_for_year)

    median_for_year = df["rate"].median()  # 27.2883
    print("Median exchange rate:", median_for_year)

    max_for_year = df["rate"].max()  # 28.431
    print("The maximum value of the course for the year:", max_for_year)

    min_for_year = df["rate"].min()  # 26.0575
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


    favorable_year_rate = grouped_data["rate"].min()  # 26.37467741935484
    print(favorable_year_rate, "favorable year rate")
    # favorable_month = grouped_data[grouped_data["rate"] == favorable_year_rate]
    # print("The most favorable average monthly rate for the year was:", favorable_month, favorable_year_rate)


def exchange_rate_forecast():
    # 365 - (365 // 3) * 3  # = 2: 28, 27 -  28 27 26 28
    return "121 - days was the most common currency rate 28 and 27"


if __name__ == "__main__":
    start_date = moment.date('2021-01-01')
    end_date = moment.date('2022-01-01')
    dates = list(dates_between_two_dates(start_date, end_date))
    list_of_url = list(map(get_url, dates))
    response = list(map(lambda url: requests.get(url).json(), list_of_url))
    data_dict = dict(zip(get_list_of_data(response)[0], get_list_of_data(response)[1]))
    data_items = list(data_dict.items())
    dataframe = pd.DataFrame(data_items, columns=["data", "rate"])
    exchange_rate_analysis(dataframe)
    print(exchange_rate_forecast())
