from flask import Flask, request, jsonify
from redis import Redis
import json
import heapq
import pandas as pd
from flask import send_file
import pika

app = Flask(__name__)
# mega list, sorted by PRICE, THEN by RATING
seller_queue = []
buyer_queue = []
error_queue = []

ORDERS = []
CLIENTS = []
INSTRUMENTS = []
REPORTS = []


# validation checks
def validate_order(
    client_id,
    instrument_id,
    instrument_curr,
    client_curr,
    quantity,
    lot_size,
    position_check,
):
    if instrument_id not in redis_client_instrument.keys():
        return False
    if client_id not in redis_client_client.keys():
        return False
    if not validate_instrument(
        instrument_id, instrument_curr, client_curr, quantity, lot_size
    ):
        # log error and reason
        return False
    if not validate_client(client_id, position_check):
        # log error and reason
        return False
    return True


def validate_instrument(
    instrument_id, instrument_curr, client_curr, quantity, lot_size
):
    if instrument_curr not in client_curr:
        did_not_pass("currency")
        return False
    if quantity % lot_size != 0:
        did_not_pass("lot_size")
        return False
    return True

    # client_data = queue_list[0]
    # client_id = client_data['client_id']
    # client_position_check = client_data['position_check']


def validate_client(client_id, position_check):
    if position_check == "Y" or "y":
        # pull from data store NEED TO EDIT,, SHD HAVE IF-ELSE LATER
        did_not_pass("position")
        return False
    return True


# for creating and queueing mega list
# # get ALL data from redis
# # order_id = redis_client.keys()
# # data = redis_client.values()
# # order_value = json.loads(data)
# def queue_client(order_id, order_value):

#     # seller or buyer
#     if order_value['Side'] == 'Sell':

#     elif order_value['Side'] == 'Buy':


# # matching engine
# # seller_order = seller_list[0]
# # buyer_order = buyer_list[0]
# # NEEDA HAVE PORTION WHERE IT CHECKS MARKET-MARKET by INDEXING [-1]

# def match_order(seller_order, buyer_order):
#     #market is highest prio
#     #calculates quantity
#     #updates redis


### Initialize endpoints ##################################################################
# INIT ORDERS
@app.route("/orders", methods=["POST"])
def init_orders():
    # the post request contains a .csv attached, read it and display the output.
    # eg:
    # Time,OrderID,Instrument,Quantity,Client,Price,Side
    # 9:00:01,A1,SIA,1500,A,Market,Buy
    # 9:02:00,B1,SIA,4500,B,32.1,Sell
    # 9:05:00,C1,SIA,100,C,32,Buy
    # Check if the file exists
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400
    if file:
        orders_df = pd.read_csv(file)

    orders_html = orders_df.to_html()

    # Store all the orders as key value in redis order, then an array of their attributes
    for index, row in orders_df.iterrows():
        order_id = row["OrderID"]
        attributes = row.to_dict()
        for key, value in attributes.items():
            ORDERS.append({order_id: attributes})

    # Return HTML string
    print("completing")
    return ORDERS, 200


# INIT CLIENTS
@app.route("/clients", methods=["POST"])
def init_clients():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400
    if file:
        clients_df = pd.read_csv(file)

    clients_html = clients_df.to_html()

    # store the clients in redis
    for index, row in clients_df.iterrows():
        client_id = row["ClientID"]
        attributes = row.to_dict()
        for key, value in attributes.items():
            CLIENTS.append({client_id: attributes})

    return CLIENTS, 200


# INIT INSTRUMENTS
@app.route("/instruments", methods=["POST"])
def init_instruments():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400
    if file:
        instruments_df = pd.read_csv(file)

    instruments_html = instruments_df.to_html()

    # store the instruments in redis!
    for index, row in instruments_df.iterrows():
        instrument_id = row["InstrumentID"]
        attributes = row.to_dict()
        # Safely add additional attributes with default values if they are missing
        attributes['OpenPrice'] = row.get("OpenPrice", None)
        attributes['ClosedPrice'] = row.get("ClosedPrice", None)
        attributes['TotalTradedVol'] = row.get("TotalTradedVol", None)
        attributes['DayHigh'] = row.get("DayHigh", None)
        attributes['DayLow'] = row.get("DayLow", None)
        attributes['VWAP'] = row.get("VWAP", None)
        INSTRUMENTS.append({instrument_id: attributes})

    return INSTRUMENTS, 200


@app.route("/report", methods=["GET"])
def get_report():
    exchange_report = "exchange_report"  # PUSH FAILED MATCHED POLICIES INTO EXCHANGE
    client_report = "client_report"  # PUSH ALL CLIENTS EOD REPORT INTO CLIENT
    instrument_report = (
        "instrument_report"  # PUSH ALL INSTRUMENTS EOD REPORT INTO INSTRUMENT
    )
    REPORTS.append({"exchange_report": exchange_report})
    REPORTS.append({"client_report": client_report})
    REPORTS.append({"instrument_report": instrument_report})
    return REPORTS, 200


# helper function for instrument report
def generate_instrument_report():
    # for each instrument, i need to retrieve highest
    pass


if __name__ == "__main__":
    app.run(debug=True, port=5000)
