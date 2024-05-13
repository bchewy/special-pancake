from flask import Flask, request, jsonify
from redis import Redis
import json
import heapq
import pandas as pd
from flask import send_file
import pika

app = Flask(__name__)

# Initialize Redis client
redis_client_orders  = Redis(host="localhost", port=6379, db=0)
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
    return jsonify({"message": "Order microservice is running"}), 200


# initialize orders
# this /orders feeds the input_orders in.
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
            redis_client.hset(order_id, key, value)

    # Return HTML string
    return orders_html, 200


# initalize clients
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


# iniitlaize instruments
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


@app.route("/order", methods=["POST"])
def add_order():
    order_data = request.json
    return jsonify({"message": "Order received", "order_data": order_data}), 201


##### REPORT #####
def generate_xchange_report():
    




if __name__ == "__main__":
    app.run(debug=True, port=5000)
