from flask import Flask, request, jsonify
from redis import Redis
import json
import heapq
import pandas as pd
from flask import send_file
import pika

from match import (
    ORDERS,
    CLIENTS,
    INSTRUMENTS,
    init_orders,
    init_clients,
    init_instruments,
)

app = Flask(__name__)

# Initialize Redis client
# redis_client_orders = Redis(host="localhost", port=6379, db=0)
# redis_client_client = Redis(host="localhost", port=6379, db=1)
# redis_client_instrument = Redis(host="localhost", port=6379, db=2)

# Reports
# redis_client_xchange_report = Redis(host="localhost", port=6379, db=3)
# redis_client_client_report = Redis(host="localhost", port=6379, db=4)
# redis_client_instrument_report = Redis(host="localhost", port=6379, db=5)


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
error_queue = []
INSTRUMENT_TRACK = {}


class OrderBook:
    def __init__(self):
        self.buy_heap = []
        self.sell_heap = []

    def add_order(
        self, order_type, price, client_rating, quantity, client_id, arrival_time
    ):
        market_priority = 0 if price == "Market" else 1
        actual_price = (
            float("inf")
            if price == "Market" and order_type == "Buy"
            else float("-inf") if price == "Market" and order_type == "Sell" else price
        )

        # this is built according to the heapq compulsory comparison of tuples element-wise
        # first it will sort by market priority because MARKET == 0, then by price, then by client rating, where
        order = (
            market_priority,
            -actual_price if order_type == "Buy" else actual_price,
            client_rating,
            arrival_time,
            client_id,
            quantity,
        )

        if order_type == "Buy":
            heapq.heappush(self.buy_heap, order)
        else:
            heapq.heappush(self.sell_heap, order)

    def match_order(self):
        while self.buy_heap and self.sell_heap:
            # this will pop according to MARKET FIRST
            # THEN this will pop according to HIGHEST BUYER PRICE FIRST, AND pop according to LOWEST SELLER PRICE FIRST
            buy_order = heapq.heappop(self.buy_heap)
            sell_order = heapq.heappop(self.sell_heap)

            # Prevent Market-to-Market matching
            if buy_order[0] == 0 and sell_order[0] == 0:
                heapq.heappush(self.buy_heap, buy_order)
                heapq.heappush(self.sell_heap, sell_order)
                break  # Exit matching, no

            # Match logic: compare prices considering buy uses -price
            if -buy_order[1] >= sell_order[1]:
                min_qty = min(buy_order[5], sell_order[5])
                print(
                    f"Matched {min_qty} units between {buy_order[4]} (Buy) and {sell_order[4]} (Sell)"
                )

                # Adjust quantities
                if buy_order[5] > min_qty:
                    new_buy_order = (
                        buy_order[0],
                        buy_order[1],
                        buy_order[2],
                        buy_order[3],
                        buy_order[4],
                        buy_order[5] - min_qty,
                    )
                    heapq.heappush(self.buy_heap, new_buy_order)

                if sell_order[5] > min_qty:
                    new_sell_order = (
                        sell_order[0],
                        sell_order[1],
                        sell_order[2],
                        sell_order[3],
                        sell_order[4],
                        sell_order[5] - min_qty,
                    )
                    heapq.heappush(self.sell_heap, new_sell_order)

                # If quantity is perfectly eliminated (bought and sold equally)
                if buy_order[5] == sell_order[5]:
                    pass

            else:
                # No match, push back to heaps
                heapq.heappush(self.buy_heap, buy_order)
                heapq.heappush(self.sell_heap, sell_order)
                break  # Exit matching if top orders can't match

    # edit this functions according to the problem statement
    # def print_book(self):
    #     print("Buy Orders:")
    #     for order in sorted(self.buy_heap, reverse=True):  # Min-heap, so reverse to show max at top
    #         print(order)
    #     print("Sell Orders:")
    #     for order in sorted(self.sell_heap):
    #         print(order)


# Example usage
book = OrderBook()
# init_orders()
# init_instruments()
# init_clients()


print(ORDERS)
print(CLIENTS)
print(INSTRUMENTS)

# book.add_order("buy", "Market", 95, 100, "client1")
# book.add_order("buy", 300, 90, 50, "client2")
# book.add_order("sell", 305, 85, 30, "client3")
# book.add_order("sell", "Market", 92, 70, "client4")

# book.match_order()  # Perform matching
# book.print_book()  # Print remaining orders in the book


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
    # if instrument_id not in redis_client_instrument.keys():
    #     return False
    # if client_id not in redis_client_client.keys():
    #     return False
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
        # if pull_from_redis(client_id) == 0:
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


# @app.route("/", methods=["GET"])
# def index():
#     return jsonify({"message": "healthy"}), 200


# if __name__ == "__main__":
#     app.run(debug=True, port=5000)
