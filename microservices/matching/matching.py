from flask import Flask, request, jsonify
from redis import Redis
import pandas as pd
from flask import send_file
import pika

app = Flask(__name__)

# Initialize Redis client
redis_client = Redis(host="localhost", port=6379, db=0)



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


@app.route("/", methods=["GET"])
def index():
    return jsonify({"message": "Order microservice is running"}), 200


@app.route("/orders", methods=["POST"])
def get_orders():
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


@app.route("/order", methods=["POST"])
def add_order():
    order_data = request.json
    return jsonify({"message": "Order received", "order_data": order_data}), 201


# "check" defines which kind of policy it is
# Call this function if the any matching policies fail
def did_not_pass(check):
    if check == "instrument":
        return jsonify({"message": "Instrument policy failed"}), 400
    elif check == "currency":
        return jsonify({"message": "Currency check failed"}), 400
    elif check == "lot_size":
        return jsonify({"message": "Lot size check failed"}), 400
    elif check == "position":
        return jsonify({"message": "Position check failed"}), 400


# Init the example-set csv, creating the orders from input_orders.csv


if __name__ == "__main__":
    app.run(debug=True, port=5000)
