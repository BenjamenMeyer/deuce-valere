"""
Deuce Valere - Tests - Client - Valere
"""
from deucevalere.client.valere import ValereClient
from deucevalere.tests.client_base import TestValereClientBase


class TestValereClientBasics(TestValereClientBase):

    def setUp(self):
        super().setUp()

        self.secondary_setup(manager_start=None,
                             manager_end=None)

    def tearDown(self):
        super().tearDown()

    def test_valere_client_creation(self):
        client = ValereClient(self.deuce_client, self.vault, self.manager)
