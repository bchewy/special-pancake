import unittest 
from unittest.mock import patch
from matching import app, OrderBook
from unittest import mock

class TestOrderBook(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch("matching.OrderBook")
    def test_add_order(self, mock_order_book):
        order_book_instance = mock_order_book.return_value

        order_type = "Buy"
        price = 1
        client_rating = 10
        quantity = 1
        client_id = "client2"
        arrival_time = "09:00:05"

        self.app.add_order(
            order_type, price, client_rating, quantity, client_id, arrival_time
        )
        order_book_instance.add_order.assert_called_with(
            order_type, price, client_rating, quantity, client_id, arrival_time
        )


    @patch("matching.OrderBook")
    def test_match_order(self, mock_order_book):
        order_book_instance = mock_order_book.return_value
        self.app.match_order()

        order_book_instance.match_order.assert_called_once()

    @patch("matching.OrderBook")
    def test_order_flow(self, mock_order_book):
        order_book_instance = mock_order_book.return_value

        orders = [
            ("Buy", "Market", 95, 100, "client1", "09:00:01"),
            ("Buy", 300, 90, 50, "client2", "09:02:00"),
            ("Sell", 305, 85, 30, "client3", "09:05:00"),
            ("Sell", "Market", 92, 70, "client4", "09:10:00"),
        ]

        for order in orders:
            self.app.add_order(*order)

        self.app.match_order()

        expected_calls = [mock.call(*order) for order in orders]
        order_book_instance.add_order.assert_has_calls(expected_calls, any_order=True)

        order_book_instance.match_order.assert_called_once()
