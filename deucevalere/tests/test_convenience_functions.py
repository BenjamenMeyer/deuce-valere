"""
Deuce Valere - Tests - Convenience Functions
"""
import unittest

from deuceclient.api import Vault
from deuceclient.client.deuce import DeuceClient
from deuceclient.tests import *

from deucevalere import vault_validate, vault_cleanup
from deucevalere.api.system import Manager
from deucevalere.tests import *


class TestConvenienceFunctions(unittest.TestCase):

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
        self.client = DeuceClient(self.authengine,
                                  'neo.the.one',
                                  True)

    def tearDown(self):
        super().tearDown()

    # def test_cleanup(self):
        # self.assertEqual(vault_cleanup(self.client,
        #                               self.vault,
        #                               self.manager),
        #                 0)
