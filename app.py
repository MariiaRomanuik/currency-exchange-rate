from flask import Flask, request

app = Flask(__name__)


@app.route('/api/info')
def api_info():
    return 'Information about ...'


# url = http://localhost:5000/api/timeline?startDate=2019-01-01&endDate=2020-01-01
@app.route('/api/timeline')
def api_timeline():
    startDate = request.args.get('startDate')  # key
    endDate = request.args.get('endDate')
    rangeFrom = request.args.get('rangeFrom')  # value
    rangeTo = request.args.get('rangeTo')

    timeline = {'timeline': [{'date': {startDate}, 'value': rangeFrom}, {'date': {endDate}, 'value': rangeTo}]}

    return timeline


if __name__ == '__main__':
    app.run(debug=True)
