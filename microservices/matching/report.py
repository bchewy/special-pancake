from flask import Flask, request, jsonify
from redis import Redis
import json
import heapq
import pandas as pd
from flask import send_file
import pika
import json


app = Flask(__name__)
ORDERS = []
CLIENTS = []
INSTRUMENTS = []
REPORTS = []

# SEED DATA
INSTRUMENTS.extend(pd.read_csv("input_instruments.csv").to_dict(orient="records"))
ORDERS.extend(pd.read_csv("input_orders.csv").to_dict(orient="records"))
CLIENTS.extend(pd.read_csv("input_clients.csv").to_dict(orient="records"))


@app.route("/report", methods=["GET"])
def get_report():
    exchange_report = "exchange_report"  # PUSH FAILED MATCHED POLICIES INTO EXCHANGE
    client_report = "client_report"  # PUSH ALL CLIENTS EOD REPORT INTO CLIENT

    instrument_report_final = []
    for instrument in INSTRUMENTS:
        instrument_id = list(instrument.keys())[
            0
        ]  # Convert dict_keys to list and access the first element
        instrument_data = instrument[instrument_id]
        instrument_report = {
            "instrument_id": instrument_id,
            "open_price": instrument_data.get("OpenPrice"),
            "closed_price": instrument_data.get("ClosedPrice"),
            "total_traded_vol": instrument_data.get("TotalTradedVol"),
            "day_high": instrument_data.get("DayHigh"),
            "day_low": instrument_data.get("DayLow"),
            "vwap": instrument_data.get("VWAP"),
        }
        instrument_report_final.append(instrument_report)

    REPORTS.append({"exchange_report": exchange_report})
    REPORTS.append({"client_report": client_report})
    REPORTS.append({"instrument_report": instrument_report_final})
    return REPORTS, 200


# UPDATE EXCHANGE REPORT:
@app.route("/update_exchange_report", methods=["POST"])
def update_exchange_report():

    # body: {exchange_report: [{order_id: 1, reason: "insufficient_balance"}]}

    

    # Load the exchange report CSV to simulate fetching failed transactions
    exchange_report_df = pd.read_csv("output_exchange_report.csv")

    # Convert DataFrame to dictionary for easier manipulation
    exchange_report_dict = exchange_report_df.to_dict(orient="records")

    # Update the global REPORTS list with the new exchange report data
    for report in REPORTS:
        if "exchange_report" in report:
            report["exchange_report"] = exchange_report_dict
            break
    else:
        # If exchange_report key is not found, add it
        REPORTS.append({"exchange_report": exchange_report_dict})


# Call the function to update the exchange report
update_exchange_report()


if __name__ == "__main__":
    app.run(debug=True, port=5000)
