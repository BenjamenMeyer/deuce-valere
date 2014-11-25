"""
Deuce Valere - Tests - Client - Valere
"""
import unittest

from deuceclient.api import Vault
from deuceclient.client.deuce import DeuceClient
from deuceclient.tests import *
import httpretty

from deucevalere.api.system import Manager
from deucevalere.client.valere import ValereClient
from deucevalere.tests import *


class TestValereClientBasics(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.manager = Manager()
        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.vault = Vault(self.project_id, self.vault_id)
        self.authengine = FakeAuthEngine(userid='blue',
                                         usertype='pill',
                                         credentials='morpheus',
                                         auth_method='matrix')
        self.deuce_client = DeuceClient(self.authengine,
                                        'neo.the.one',
                                        True)

    def tearDown(self):
        super().tearDown()

    def test_valere_client_creation(self):
        client = ValereClient(self.deuce_client, self.vault, self.manager)
