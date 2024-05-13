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


@app.route("/exchange_report", methods=["GET"])
def get_exchange_report():
    # sample: need to call store_failed_orders each time a policy check fails
    # store_failed_orders("A1", "instrument")
    return generate_xchange_report()


@app.route("/client_report", methods=["GET"])
def get_client_report():
    return generate_client_report()


# Matching Policies failed? storing in redis exchange_report hash with order id as key
def store_failed_orders(order_id, reason):
    redis_client_xchange_report.hset("exchange_report", order_id, reason)


# Updating client position into redis directly, so it can be taken out later on for report...
def update_client_position(client_id, instrument_id, quantity):
    redis_client_client.hset(client_id, instrument_id, quantity)


##### REPORT #####
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
    client_report = pd.DataFrame(client_ids, columns=["Client ID"])
    client_report_csv = client_report.to_csv(index=False)
    print(client_report_csv)
    # this client report consits a position of each client at the end of the trading day for each instrument.

    return jsonify({"client_report": client_report_csv}), 200


if __name__ == "__main__":
    app.run(debug=True, port=5000)
