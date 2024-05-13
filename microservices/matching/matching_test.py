import unittest
from unittest.mock import patch
from report import app


class TestReportService(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    