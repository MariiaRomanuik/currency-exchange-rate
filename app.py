from flask import Flask, request, render_template
from exchange_rate import *

app = Flask(__name__)


@app.route('/api/info')
def api_info():
    return 'Information about ...'


# url = http://localhost:5000/api/timeline?startDate=2019-01-01&endDate=2020-01-01
@app.route('/api/timeline')
def api_timeline():
    start_date = request.args.get('start_date')  # key
    end_date = request.args.get('end_date')
    range_from = request.args.get('range_from')  # value
    range_to = request.args.get('range_to')
    timeline = {'timeline': [{'date': start_date, 'value': range_from}, {'date': end_date, 'value': range_to}]}
    return timeline


# url = http://localhost:5000/api/get_graph?start_date=2021-01-01&end_date=2022-01-01
@app.route('/api/get_graph')
def get_graph():
    start_date = request.args.get('start_date')  # key
    end_date = request.args.get('end_date')
    file_name = "currency.csv"
    folder_name = "data"
    path_to_csv_file = f'{folder_name}/{file_name}'
    bucket_name = 's3-all-data'
    region = 'us-east-2'

    if not os.path.isfile(path_to_csv_file):
        start_moment_date = moment.date(start_date)
        end_moment_date = moment.date(end_date)
        dates = list(dates_between_two_dates(start_moment_date, end_moment_date))
        list_of_url = list(map(get_url, dates))
        response = list(map(lambda url: requests.get(url).json(), list_of_url))
        data_dict = {"date": get_list_of_data(response)[0], "rate": get_list_of_data(response)[1]}
        df = pd.DataFrame(data_dict)
        path_to_csv_file = "./data/currency.csv"
        write_dataframe_to_csv(path_to_csv_file, df)
        print(create_bucket(bucket_name, region))
        print(upload_file_to_s3(path_to_csv_file, bucket_name))

    connection = connect_to_db()
    connection.autocommit = True
    with connection:
        with connection.cursor() as cursor:
            # create_database("postgres", cursor)
            # create_table("CURRENCY_RATE", cursor)
            print(from_s3_to_postgres(bucket_name, file_name, cursor, connection))
            dataframe = get_data_from_db(cursor, connection)[0]
            labels = get_data_from_db(cursor, connection)[1]
            values = get_data_from_db(cursor, connection)[2]

    connection.close()
    return render_template("graph.html", labels=labels, values=values)


if __name__ == '__main__':
    app.run(debug=True)
