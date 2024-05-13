from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("/", methods=["GET"])
def index():
    return jsonify({"message": "Report microservice is running"}), 200


@app.route("/reports", methods=["POST"])
def get_orders():
    # Mock data
    exchange_reports = [
        {"OrderID": "ORDER_15", "Instrument": "INST_003", "Client": "CLIENT_109", "Status": "Failed Policy Check"}
    ]
    
    client_reports = [
        {"Client": "CLIENT_107", "Instrument": "INST_000", "Position": 17500.0},
        {"Client": "CLIENT_106", "Instrument": "INST_001", "Position": 4350.0}
    ]
    
    instrument_reports = [
        {
            "Instrument": "INST_000",
            "OpenPrice": 30.5,
            "ClosePrice": 31.0,
            "TotalVolume": 20000,
            "DayHigh": 32.0,
            "DayLow": 30.0,
            "VWAP": 31.5
        }
    ]
    
    # Combine all reports into a single dictionary to return as JSON
    reports = {
        "ExchangeReports": exchange_reports,
        "ClientReports": client_reports,
        "InstrumentReports": instrument_reports
    }
    return jsonify(reports)


def generate_report():
    


if __name__ == "__main__":
    app.run(debug=True, port=5001)
