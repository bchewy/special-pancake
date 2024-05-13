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
        for key, value in attributes.items():
            INSTRUMENTS.append({instrument_id: attributes})

    return INSTRUMENTS, 200


#### INSTRUMENTS ###################################################################


# "check" defines which kind of policy it is
# Call this function if the any matching policies fail
def did_not_pass(check):
    if check == "currency":
        error_queue.append("REJECTED - MISMATCH CURRENCY")
        return jsonify({"message": "REJECTED - MISMATCH CURRENCY"}), 400
    elif check == "lot_size":
        error_queue.append("REJECTED - INVALID LOT SIZE")
        return jsonify({"message": "REJECTED - INVALID LOT SIZE"}), 400
    elif check == "position":
        error_queue.append("REJECTED - POSITION CHECK FAILED")
        return jsonify({"message": "REJECTED - POSITION CHECK FAILED"}), 400


##### REPORT ###############################################################


@app.route("/exchange_report", methods=["GET"])
def get_exchange_report():
    # sample: need to call store_failed_orders each time a policy check fails
    # store_failed_orders("A1", "instrument")
    return generate_xchange_report()


@app.route("/client_report", methods=["GET"])
def get_client_report():
    # sample: need to call update client pos each time a trade is made
    # update_client_position("A1", "SIA", 100)
    return generate_client_report()


@app.route("/instrument_report", methods=["POST"])
def instrument_report():
    # sample : need to call update_instrument_track each time a trade is made
    update_instrument_track("SIA", "OpenPrice", 100)
    update_instrument_track("SIA", "ClosePrice", 100)
    update_instrument_track("SIA", "TotalVolume", 100)
    return generate_instrument_report()


#### HELPER FUNCTIONS FOR REPORTS #####
# Matching Policies failed? storing in redis exchange_report hash with order id as key
def store_failed_orders(order_id, reason):
    redis_client_xchange_report.hset("exchange_report", order_id, reason)


# Updating client position into redis directly, so it can be taken out later on for report...
def update_client_position(client_id, instrument_id, quantity):
    redis_client_client_report.hset(client_id, instrument_id, quantity)


# Updating the instrument track, for each item and value
def update_instrument_track(instrument, item, value):
    # instrument = decoded id from redis
    # item = the item to update eg: open price, closed price, tvol, day high, day low, vwap
    # value = the value to update to
    validate = [
        "OpenPrice",
        "ClosePrice",
        "TotalVolume",
        "DayHigh",
        "DayLow",
        "VWAP",
    ]
    if item in validate:
        redis_client_instrument_report.hset(instrument, item, value)


# Calculate VWAP (Volume Weighted Average Price)
# VWAP = (Typical Price * Volume) / Total Volume
def calculate_vwap(instrument):
    success = False
    vwap = 0
    instrument_data = redis_client_instrument.hgetall(instrument)
    if instrument_data:
        day_high = float(instrument_data.get("DayHigh", 0))
        day_low = float(instrument_data.get("DayLow", 0))
        closed_price = float(instrument_data.get("ClosedPrice", 0))
        total_volume_traded = float(instrument_data.get("TotalVolume", 0))

        if total_volume_traded != 0:
            typical_price = (day_high + day_low + closed_price) / 3
            vwap = (typical_price * total_volume_traded) / total_volume_traded
            redis_client_instrument.hset(instrument, "VWAP", vwap)
            success = True
            return vwap, success
        else:
            print("Total volume traded is 0!!, cannot calculate VWAP.")
            return vwap, success
    else:
        print(f"No data found for instrument {instrument}.")
        return vwap, success


### REPORT GENERATION FUNCS
def generate_xchange_report():
    exchange_report = redis_client_xchange_report.hgetall("exchange_report")
    # Convert the exchange report dictionary to a DataFrame for better CSV display
    report_df = pd.DataFrame(
        list(exchange_report.items()), columns=["Order Id", "Rejection Reason"]
    )
    # Convert DataFrame to CSV format for display
    exchange_report_csv = report_df.to_csv(index=False)
    print(exchange_report_csv)
    return jsonify({"exchange_report": exchange_report_csv}), 200


def generate_client_report():
    client_ids = redis_client_client_report.keys()
    print(client_ids)

    client_positions = []
    for client_id in client_ids:
        client_data = redis_client_client_report.hgetall(client_id)
        for instrument_id, quantity in client_data.items():
            client_positions.append(
                [
                    client_id.decode("utf-8"),
                    instrument_id.decode("utf-8"),
                    int(quantity),
                ]
            )

    client_report = pd.DataFrame(
        client_positions, columns=["Client ID", "Instrument ID", "Net Position"]
    )
    client_report_csv = client_report.to_csv(index=False)
    print(client_report_csv)
    # this client report consits a position of each client at the end of the trading day for each instrument.

    return jsonify({"client_report": client_report_csv}), 200


def generate_instrument_report():
    instrument_ids = redis_client_instrument.keys()
    print(instrument_ids)

    instrument_report = []
    for instrument_id in instrument_ids:
        instrument_data = redis_client_instrument.hgetall(instrument_id)
        print(
            "All instrument data:",
            {k.decode("utf-8"): v.decode("utf-8") for k, v in instrument_data.items()},
        )

        instrument_report.append(
            [
                instrument_id.decode("utf-8"),
                float(instrument_data.get(b"OpenPrice", b"0")),
                float(instrument_data.get(b"ClosePrice", b"0")),
                int(instrument_data.get(b"TotalVolume", b"0")),
            ]
        )

    instrument_report_df = pd.DataFrame(
        instrument_report,
        columns=["Instrument ID", "Open Price", "Close Price", "Total Volume"],
    )

    instrument_report_csv = instrument_report_df.to_csv(index=False)
    print("instrument report here: ")
    print(instrument_report_csv)
    return jsonify({"instrument_report": instrument_report_csv}), 200


if __name__ == "__main__":
    app.run(debug=True, port=5000)
