from flask import Flask, request, jsonify
from redis import Redis
import json
import heapq
import pandas as pd
from flask import send_file
import pika
import requests

from datetime import datetime, time

app = Flask(__name__)

# Initialize Redis client
# redis_client_orders = Redis(host="localhost", port=6379, db=0)
# redis_client_client = Redis(host="localhost", port=6379, db=1)
# redis_client_instrument = Redis(host="localhost", port=6379, db=2)

# Reports
# redis_client_xchange_report = Redis(host="localhost", port=6379, db=3)
# redis_client_client_report = Redis(host="localhost", port=6379, db=4)
# redis_client_instrument_report = Redis(host="localhost", port=6379, db=5)

INSTRUMENTS = []
ORDERS = []
CLIENTS = []
STOCKS = []

INSTRUMENTS.extend(pd.read_csv("data-set/example-set/input_instruments.csv").to_dict(orient='records'))
ORDERS.extend(pd.read_csv("data-set/example-set/input_orders.csv").to_dict(orient='records'))
CLIENTS.extend(pd.read_csv("data-set/example-set/input_clients.csv").to_dict(orient='records'))
STOCKS.extend(pd.read_csv("data-set/example-set/stocks.csv").to_dict(orient='records'))


print("finish init")
# print(ORDERS)
# print(CLIENTS)
# print(INSTRUMENTS)

error_queue = []
INSTRUMENT_TRACK = {}


class OrderBook:
    def __init__(self):
        self.buy_heap = []
        self.sell_heap = []

    def add_order(
        self, order_type, price, client_rating, quantity, client_id, arrival_time, order_id
    ):
        market_priority = 0
        if price == "Market":
            market_priority = 0
        else:
            market_priority = 1

        if price == "Market" and order_type == "Buy":
            actual_price = float("inf")
        elif price == "Market" and order_type == "Sell":
            actual_price = float("-inf")
        else:
            actual_price = price

        # this is built according to the heapq compulsory comparison of tuples element-wise
        # first it will sort by market priority because MARKET == 0, then by price, then by client rating, where
        if order_type == "Buy":
            actual_price = float(actual_price)
            actual_price = -actual_price
        order = (
            market_priority,
            actual_price,
            client_rating,
            arrival_time,
            client_id,
            quantity,
            order_id
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
                        buy_order[6],
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
                        buy_order[6],
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


# "check" defines which kind of policy it is
# Call this function if the any matching policies fail

endpoint_url = f"http://localhost:5000/update_exchange_report" 
def did_not_pass(check, order_id):
    if check == "currency":
        requests.post(endpoint_url, json={"order_id": order_id, "reason": "REJECTED - MISMATCH CURRENCY"})  
        error_queue.append("REJECTED - MISMATCH CURRENCY")
    elif check == "lot_size":
        requests.post(endpoint_url, json={"order_id": order_id, "reason": "REJECTED - INVALID LOT SIZE"})  
        error_queue.append("REJECTED - INVALID LOT SIZE")
    elif check == "position":
        requests.post(endpoint_url, json={"order_id": order_id, "reason": "REJECTED - POSITION CHECK FAILED"})  
        error_queue.append("REJECTED - POSITION CHECK FAILED")


book = OrderBook()


def validate_instrument(instrument_curr, client_curr, quantity, lot_size, order_id):
    if instrument_curr not in client_curr:
        did_not_pass("currency", order_id)
        return False
    if quantity % lot_size != 0:
        did_not_pass("lot_size", order_id)
        return False
    return True


def validate_client(client_id, instrument_id):
    # Check if sufficient balance
    if position_check == "Y" or "y":
        # Check if client has sufficient balance
        if instrument_id in STOCKS:
            return False
        return True
    return True


# validation checks
def validate_order(
    client_id,
    instrument_curr,
    client_curr,
    quantity,
    lot_size,
    position_check,
    order_id
):
    # if instrument_id not in redis_client_instrument.keys():
    #     return False
    # if client_id not in redis_client_client.keys():
    #     return False
    if not validate_instrument(instrument_curr, client_curr, quantity, lot_size,order_id):
        # log error and reason
        return False
    if not validate_client(client_id, position_check):
        # log error and reason
        return False
    return True


# print(ORDERS)
# print(CLIENTS)
# print(INSTRUMENTS)

book = OrderBook()

open_period = False
continuous_period = False
closed_period = False

start_time = datetime.strptime("09:00:00", "%H:%M:%S").time()
open_time = datetime.strptime("09:30:00", "%H:%M:%S").time()
continuous_time = datetime.strptime("16:00:00", "%H:%M:%S").time()
closed_time = datetime.strptime("16:10:00", "%H:%M:%S").time()

for order in ORDERS:
    # print("performing per order")
    data_time = datetime.strptime(order.get("Time"), "%H:%M:%S").time()
    if not open_period and not continuous_period and not closed_period:
        if start_time < data_time < open_time:
            open_period = True
        elif open_time <= data_time < continuous_time:
            continuous_period = True
        elif continuous_time <= data_time < closed_time:
            closed_period = True

    # Convert target time string to a time object
    instrument_curr = ""
    client_curr = ""
    lot_size = ""
    position_check = ""

    for json_obj in INSTRUMENTS:
        if json_obj.get("InstrumentID") == order.get("Instrument"):
            instrument_curr = json_obj.get("Currency")
            lot_size = json_obj.get("LotSize")
            break
    for json_obj in CLIENTS:
        if json_obj.get("ClientID") == order.get("Client"):
            client_curr = json_obj.get("Currencies")
            break
    validate_order(
        order.get("Client"),
        order.get("Instrument"),
        instrument_curr,
        client_curr,
        order.get("Quantity"),
        lot_size,
        position_check,
    )

    if open_period:
        if data_time >= open_time:
            open_period = False
            continuous_period = True
            book.match_order()

        book.add_order(
            order.get("Side"),
            order.get("Price"),
            order.get("ClientRating"),
            order.get("Quantity"),
            order.get("Client"),
            order.get("Time"),
            order.get("OrderID"),
        )

    elif continuous_period:
        if data_time >= continuous_time:
            continuous_period = False
            closed_period = True

        book.add_order(
            order.get("Side"),
            order.get("Price"),
            order.get("ClientRating"),
            order.get("Quantity"),
            order.get("Client"),
            order.get("Time"),
            order.get("OrderID"),
        )
        book.match_order()
    elif closed_period:
        if data_time >= open_time:
            open_period = False
            continuous_period = True
            book.match_order()

        book.add_order(
            order.get("Side"),
            order.get("Price"),
            order.get("ClientRating"),
            order.get("Quantity"),
            order.get("Client"),
            order.get("Time"),
            order.get("OrderID"),
        )

print("-------------------")
print("buy heap")
print(book.buy_heap)
print("-------------------")

print("-------------------")
print("sell heap")
print(book.sell_heap)
print("-------------------")


# book.add_order("buy", "Market", 95, 100, "client1")
# book.add_order("buy", 300, 90, 50, "client2")
# book.add_order("sell", 305, 85, 30, "client3")
# book.add_order("sell", "Market", 92, 70, "client4")

# book.match_order()  # Perform matching
# book.print_book()  # Print remaining orders in the book


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


# ### Initialize endpoints ##################################################################
# # INIT ORDERS
# @app.route("/orders", methods=["POST"])
# def init_orders():
#     # the post request contains a .csv attached, read it and display the output.
#     # eg:
#     # Time,OrderID,Instrument,Quantity,Client,Price,Side
#     # 9:00:01,A1,SIA,1500,A,Market,Buy
#     # 9:02:00,B1,SIA,4500,B,32.1,Sell
#     # 9:05:00,C1,SIA,100,C,32,Buy
#     # Check if the file exists
#     if "file" not in request.files:
#         return jsonify({"error": "No file part"}), 400
#     file = request.files["file"]
#     if file.filename == "":
#         return jsonify({"error": "No selected file"}), 400
#     if file:
#         orders_df = pd.read_csv(file)

#     orders_html = orders_df.to_html()

#     # Store all the orders as key value in redis order, then an array of their attributes
#     for index, row in orders_df.iterrows():
#         order_id = row["OrderID"]
#         attributes = row.to_dict()
#         for key, value in attributes.items():
#             redis_client_orders.hset(order_id, key, value)

#     # Return HTML string
#     return orders_html, 200


# # INIT CLIENTS
# @app.route("/clients", methods=["POST"])
# def init_clients():
#     if "file" not in request.files:
#         return jsonify({"error": "No file part"}), 400
#     file = request.files["file"]
#     if file.filename == "":
#         return jsonify({"error": "No selected file"}), 400
#     if file:
#         clients_df = pd.read_csv(file)

#     clients_html = clients_df.to_html()

#     # store the clients in redis
#     # for index, row in clients_df.iterrows():
#     #     client_id = row["ClientID"]
#     #     attributes = row.to_dict()
#     #     for key, value in attributes.items():
#     #         redis_client_client.hset(client_id, key, value)

#     return clients_html, 200


# # INIT INSTRUMENTS
# @app.route("/instruments", methods=["POST"])
# def init_instruments():
#     if "file" not in request.files:
#         return jsonify({"error": "No file part"}), 400
#     file = request.files["file"]
#     if file.filename == "":
#         return jsonify({"error": "No selected file"}), 400
#     if file:
#         instruments_df = pd.read_csv(file)

#     instruments_html = instruments_df.to_html()

#     # store the instruments in redis!
#     # for index, row in instruments_df.iterrows():
#     #     instrument_id = row["InstrumentID"]
#     #     attributes = row.to_dict()
#     #     for key, value in attributes.items():
#     #         redis_client_instrument.hset(instrument_id, key, value)

#     return instruments_html, 200


# #### INSTRUMENTS ###################################################################


# ##### REPORT ###############################################################


# @app.route("/exchange_report", methods=["GET"])
# def get_exchange_report():
#     # sample: need to call store_failed_orders each time a policy check fails
#     # store_failed_orders("A1", "instrument")
#     return generate_xchange_report()


# @app.route("/client_report", methods=["GET"])
# def get_client_report():
#     # sample: need to call update client pos each time a trade is made
#     # update_client_position("A1", "SIA", 100)
#     return generate_client_report()


# @app.route("/instrument_report", methods=["POST"])
# def instrument_report():
#     # sample : need to call update_instrument_track each time a trade is made
#     update_instrument_track("SIA", "OpenPrice", 100)
#     update_instrument_track("SIA", "ClosePrice", 100)
#     update_instrument_track("SIA", "TotalVolume", 100)
#     return generate_instrument_report()


# #### HELPER FUNCTIONS FOR REPORTS #####
# # Matching Policies failed? storing in redis exchange_report hash with order id as key
# def store_failed_orders(order_id, reason):
#     redis_client_xchange_report.hset("exchange_report", order_id, reason)


# # Updating client position into redis directly, so it can be taken out later on for report...
# def update_client_position(client_id, instrument_id, quantity):
#     redis_client_client_report.hset(client_id, instrument_id, quantity)


# # Updating the instrument track, for each item and value
# def update_instrument_track(instrument, item, value):
#     # instrument = decoded id from redis
#     # item = the item to update eg: open price, closed price, tvol, day high, day low, vwap
#     # value = the value to update to
#     validate = [
#         "OpenPrice",
#         "ClosePrice",
#         "TotalVolume",
#         "DayHigh",
#         "DayLow",
#         "VWAP",
#     ]
#     if item in validate:
#         redis_client_instrument_report.hset(instrument, item, value)


# # Calculate VWAP (Volume Weighted Average Price)
# # VWAP = (Typical Price * Volume) / Total Volume
# def calculate_vwap(instrument):
#     success = False
#     vwap = 0
#     instrument_data = redis_client_instrument.hgetall(instrument)
#     if instrument_data:
#         day_high = float(instrument_data.get("DayHigh", 0))
#         day_low = float(instrument_data.get("DayLow", 0))
#         closed_price = float(instrument_data.get("ClosedPrice", 0))
#         total_volume_traded = float(instrument_data.get("TotalVolume", 0))

#         if total_volume_traded != 0:
#             typical_price = (day_high + day_low + closed_price) / 3
#             vwap = (typical_price * total_volume_traded) / total_volume_traded
#             redis_client_instrument.hset(instrument, "VWAP", vwap)
#             success = True
#             return vwap, success
#         else:
#             print("Total volume traded is 0!!, cannot calculate VWAP.")
#             return vwap, success
#     else:
#         print(f"No data found for instrument {instrument}.")
#         return vwap, success


# ### REPORT GENERATION FUNCS
# def generate_xchange_report():
#     exchange_report = redis_client_xchange_report.hgetall("exchange_report")
#     # Convert the exchange report dictionary to a DataFrame for better CSV display
#     report_df = pd.DataFrame(
#         list(exchange_report.items()), columns=["Order Id", "Rejection Reason"]
#     )
#     # Convert DataFrame to CSV format for display
#     exchange_report_csv = report_df.to_csv(index=False)
#     print(exchange_report_csv)
#     return jsonify({"exchange_report": exchange_report_csv}), 200


# def generate_client_report():
#     client_ids = redis_client_client_report.keys()
#     print(client_ids)

#     client_positions = []
#     for client_id in client_ids:
#         client_data = redis_client_client_report.hgetall(client_id)
#         for instrument_id, quantity in client_data.items():
#             client_positions.append(
#                 [
#                     client_id.decode("utf-8"),
#                     instrument_id.decode("utf-8"),
#                     int(quantity),
#                 ]
#             )

#     client_report = pd.DataFrame(
#         client_positions, columns=["Client ID", "Instrument ID", "Net Position"]
#     )
#     client_report_csv = client_report.to_csv(index=False)
#     print(client_report_csv)
#     # this client report consits a position of each client at the end of the trading day for each instrument.

#     return jsonify({"client_report": client_report_csv}), 200


# def generate_instrument_report():
#     instrument_ids = redis_client_instrument.keys()
#     print(instrument_ids)

#     instrument_report = []
#     for instrument_id in instrument_ids:
#         instrument_data = redis_client_instrument.hgetall(instrument_id)
#         print(
#             "All instrument data:",
#             {k.decode("utf-8"): v.decode("utf-8") for k, v in instrument_data.items()},
#         )

#         instrument_report.append(
#             [
#                 instrument_id.decode("utf-8"),
#                 float(instrument_data.get(b"OpenPrice", b"0")),
#                 float(instrument_data.get(b"ClosePrice", b"0")),
#                 int(instrument_data.get(b"TotalVolume", b"0")),
#             ]
#         )

#     instrument_report_df = pd.DataFrame(
#         instrument_report,
#         columns=["Instrument ID", "Open Price", "Close Price", "Total Volume"],
#     )

#     instrument_report_csv = instrument_report_df.to_csv(index=False)
#     print("instrument report here: ")
#     print(instrument_report_csv)
#     return jsonify({"instrument_report": instrument_report_csv}), 200


# if __name__ == "__main__":
#     app.run(debug=True, port=5000)
