from flask import Flask, request, jsonify
from redis import Redis
import json
import heapq
import pandas as pd
from flask import send_file
import pika

app = Flask(__name__)

# Initialize Redis client
redis_client_orders = Redis(host="localhost", port=6379, db=0)
redis_client_client = Redis(host="localhost", port=6379, db=1)
redis_client_instrument = Redis(host="localhost", port=6379, db=2)

# Reports
redis_client_xchange_report = Redis(host="localhost", port=6379, db=3)
redis_client_client_report = Redis(host="localhost", port=6379, db=4)
redis_client_instrument_report = Redis(host="localhost", port=6379, db=5)


# Initialize RabbitMQ connection
rabbitmq_credentials = pika.PlainCredentials("guest", "guest")
rabbitmq_parameters = pika.ConnectionParameters(
    "localhost", 5672, "/", rabbitmq_credentials
)
rabbitmq_connection = pika.BlockingConnection(rabbitmq_parameters)
rabbitmq_channel = rabbitmq_connection.channel()

rabbitmq_channel.exchange_declare(exchange="order_exchange", exchange_type="direct")

rabbitmq_channel.queue_declare(queue="order_queue")
rabbitmq_channel.queue_declare(queue="log_queue")
rabbitmq_channel.queue_bind(
    exchange="order_exchange", queue="order_queue", routing_key="order.process"
)
rabbitmq_channel.queue_bind(
    exchange="order_exchange", queue="log_queue", routing_key="order.log"
)

# mega list, sorted by PRICE, THEN by RATING
seller_queue = []
buyer_queue = []


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
        return False
    if quantity % lot_size != 0:
        return False
    return True

    # client_data = queue_list[0]
    # client_id = client_data['client_id']
    # client_position_check = client_data['position_check']


def validate_client(client_id, position_check):
    if position_check == "Y" or "y":
        # pull from data store NEED TO EDIT,, SHD HAVE IF-ELSE LATER
        return False
    return True


# for creating and queueing mega list


# get ALL data from redis
# order_id = redis_client.keys()
# data = redis_client.values()
# order_value = json.loads(data)
def queue_client(order_id, order_value):

    # seller or buyer
    if order_value["Side"] == "Sell":
        pass
    elif order_value["Side"] == "Buy":
        pass


# matching engine
# seller_order = seller_list[0]
# buyer_order = buyer_list[0]
# NEEDA HAVE PORTION WHERE IT CHECKS MARKET-MARKET by INDEXING [-1]


def match_order(seller_order, buyer_order):
    # market is highest prio
    # calculates quantity
    # updates redis
    pass


@app.route("/", methods=["GET"])
def index():
    return jsonify({"message": "healthy"}), 200


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
            redis_client_orders.hset(order_id, key, value)

    # Return HTML string
    return orders_html, 200


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
            redis_client_client.hset(client_id, key, value)

    return clients_html, 200


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
            redis_client_instrument.hset(instrument_id, key, value)

    return instruments_html, 200


#### INSTRUMENTS ###################################################################
@app.route("/instrument_trigger", methods=["POST"])
def instrument_trigger():
    print("Initializing instrument init; ")
    return init_instrument_track()


@app.route("/instrument_update", methods=["POST"])
def instrument_update():
    instrument = request.json["instrument"]
    item = request.json["item"]
    value = request.json["value"]
    return update_instrument_track(instrument, item, value)


def init_instrument_track():
    global INSTRUMENT_TRACK
    all_instrument_keys = redis_client_instrument.keys("*")

    print(all_instrument_keys)
    instrument_id = all_instrument_keys[0].decode("utf-8")
    print(instrument_id)
    instrument_data = redis_client_instrument.hgetall(instrument_id)
    print(instrument_data)

    INSTRUMENT_TRACK = {}
    for instrument_id in all_instrument_keys:
        instrument_id_decoded = instrument_id.decode("utf-8")
        instrument_data = redis_client_instrument.hgetall(instrument_id_decoded)
        instrument_details = {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in instrument_data.items()
        }
        print(instrument_details)

    # if open price, closed price, totalvoltraded, day high, day low, vwap not in there - add them
    INSTRUMENT_TRACK[instrument_id_decoded] = {
        "OpenPrice": instrument_details.get("OpenPrice", 0),
        "ClosedPrice": instrument_details.get("ClosedPrice", 0),
        "TotalVolumeTraded": instrument_details.get("TotalVolumeTraded", 0),
        "DayHigh": instrument_details.get("DayHigh", 0),
        "DayLow": instrument_details.get("DayLow", 0),
        "VWAP": instrument_details.get("VWAP", 0),
    }
    print(INSTRUMENT_TRACK)
    return INSTRUMENT_TRACK, 200


def update_instrument_track(instrument, item, value):
    # instrument = decoded id from redis
    # item = the item to update eg: open price, closed price, tvol, day high, day low, vwap
    # value = the value to update to
    validate = [
        "OpenPrice",
        "ClosedPrice",
        "TotalVolumeTraded",
        "DayHigh",
        "DayLow",
        "VWAP",
    ]
    global INSTRUMENT_TRACK
    if instrument in INSTRUMENT_TRACK:
        if item in validate:
            INSTRUMENT_TRACK[instrument][item] = value
            redis_client_instrument.hset(instrument, item, value)
    else:
        print(f"Error: Instrument {instrument} not found in INSTRUMENT_TRACK.")


##### REPORT ###############################################################


@app.route("/exchange_report", methods=["GET"])
def get_exchange_report():
    # sample: need to call store_failed_orders each time a policy check fails
    # store_failed_orders("A1", "instrument")
    return generate_xchange_report()


@app.route("/client_report", methods=["GET"])
def get_client_report():
    # sample: need to call update client pos each time a trade is made
    update_client_position("A1", "SIA", 100)
    return generate_client_report()


#### HELPER FUNCTIONS FOR REPORTS #####
# Matching Policies failed? storing in redis exchange_report hash with order id as key
def store_failed_orders(order_id, reason):
    redis_client_xchange_report.hset("exchange_report", order_id, reason)


# Updating client position into redis directly, so it can be taken out later on for report...
def update_client_position(client_id, instrument_id, quantity):
    redis_client_client_report.hset(client_id, instrument_id, quantity)


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
        total_volume_traded = float(instrument_data.get("TotalVolumeTraded", 0))

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
    instrument_report = redis_client_instrument_report.hgetall("instrument_report")
    instrument_report_csv = instrument_report.to_csv(index=False)
    print(instrument_report_csv)
    return jsonify({"instrument_report": instrument_report_csv}), 200


if __name__ == "__main__":
    app.run(debug=True, port=5000)
