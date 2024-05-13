from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("/", methods=["GET"])
def index():
    return jsonify({"message": "Order microservice is running"}), 200


@app.route("/orders", methods=["GET"])
def get_orders():
    orders = [
        {
            "OrderID": "ORDER_10",
            "Instrument": "INST_000",
            "Quantity": 17500.0,
            "Client": "CLIENT_107",
            "Price": "Market",
            "Side": "Sell",
        },
        {
            "OrderID": "ORDER_20",
            "Instrument": "INST_001",
            "Quantity": 4350.0,
            "Client": "CLIENT_106",
            "Price": "Market",
            "Side": "Sell",
        },
        {
            "OrderID": "ORDER_30",
            "Instrument": "INST_002",
            "Quantity": 30.5,
            "Client": "CLIENT_102",
            "Price": "Market",
            "Side": "Sell",
        },
    ]
    return jsonify(orders)


@app.route("/order", methods=["POST"])
def add_order():
    order_data = request.json
    return jsonify({"message": "Order received", "order_data": order_data}), 201





# Init the example-set csv, creating the orders from input_orders.csv


if __name__ == "__main__":
    app.run(debug=True, port=5000)
