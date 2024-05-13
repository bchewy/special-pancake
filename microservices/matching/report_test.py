import unittest
from unittest.mock import patch
from report import app


class TestReportService(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    @patch("report.REPORTS", [])
    def test_get_report_empty(self):
        response = self.app.get("/report")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Reports displayed and exported to CSV.", str(response.data))

    @patch(
        "report.REPORTS",
        [{"exchange_report": [{"order_id": 1, "reason": "insufficient_balance"}]}],
    )
    def test_get_report_with_data(self):
        response = self.app.get("/report")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Reports displayed and exported to CSV.", str(response.data))

    @patch("report.REPORTS", [])
    def test_update_exchange_report(self):
        response = self.app.post(
            "/update_exchange_report",
            json={"order_id": 1, "reason": "insufficient_balance"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual("ok", response.data.decode())

    @patch("report.REPORTS", [])
    def test_update_client_report(self):
        response = self.app.post(
            "/update_client_report",
            json={"client_id": 1, "order_id": 1, "reason": "invalid policy"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual("ok", response.data.decode())

    @patch("report.REPORTS", [])
    def test_update_instrument_report(self):
        response = self.app.post(
            "/update_instrument_report",
            json={
                "instrument_id": "SIA",
                "openprice": 30,
                "closed_price": 43,
                "total_traded_vol": 400,
                "day_high": 43,
                "day_low": 29,
                "vwap": 142,
                "timestamp": 1715582657,
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual("ok", response.data.decode())


if __name__ == "__main__":
    unittest.main()
