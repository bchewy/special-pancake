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
    order_id = request.json["order_id"]
    reason = request.json["reason"]

    exchange_report_dict = {"order_id": order_id, "reason": reason}

    found = False
    for report in REPORTS:
        if "exchange_report" in report:
            report["exchange_report"].append(exchange_report_dict)
            found = True
            break
    if not found:
        REPORTS.append({"exchange_report": [exchange_report_dict]})
    print(REPORTS)

    return "ok", 200


# UPDATE CLIENT REPORT
@app.route("/update_client_report", methods=["POST"])
def update_client_report():
    # body: {client_report: {client_id: 1, order_id: 1, reason: "insufficient_balance"}]}
    client_id = request.json["client_id"]
    order_id = request.json["order_id"]
    reason = request.json["reason"]

    client_report_dict = {
        "client_id": client_id,
        "order_id": order_id,
        "reason": reason,
    }

    found = False
    for report in REPORTS:
        if "client_report" in report:
            report["client_report"].append(client_report_dict)
            found = True
            break
    if not found:
        REPORTS.append({"client_report": [client_report_dict]})
    print(REPORTS)

    return "ok", 200


# UPDATE INSTRUMENT REPORT
@app.route("/update_instrument_report", methods=["POST"])
def update_instrument_report():
    # body: {instrument_report: {instrument_id, openprice, closed price, total traded vol, day high, day low, vwap}    }
    instrument_id = request.json.get("instrument_id")
    open_price = request.json.get("openprice")
    closed_price = request.json.get("closed_price")
    total_traded_vol = request.json.get("total_traded_vol")
    day_high = request.json.get("day_high")
    day_low = request.json.get("day_low")
    vwap = request.json.get("vwap")
    timestamp = request.json.get("timestamp")

    instrument_report_dict = {
        "instrument_id": instrument_id,
        "open_price": open_price,
        "closed_price": closed_price,
        "total_traded_vol": total_traded_vol,
        "day_high": day_high,
        "day_low": day_low,
        "vwap": vwap,
        "timestamp": timestamp,
    }

    found = False
    for report in REPORTS:
        if "instrument_report" in report:
            for existing_report in report["instrument_report"]:
                if existing_report["instrument_id"] == instrument_id:
                    existing_report.update(instrument_report_dict)
                    found = True
                    break
            if not found:
                report["instrument_report"].append(instrument_report_dict)
                found = True
            break
    if not found:
        REPORTS.append({"instrument_report": [instrument_report_dict]})
    print(REPORTS)

    return "ok", 200


if __name__ == "__main__":
    app.run(debug=True, port=5000)
