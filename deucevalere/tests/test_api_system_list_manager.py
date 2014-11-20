"""
Deuce Valere - Tests - API - System - List Manager
"""
import unittest

from deucevalere.api.system import ListManager


class DeuceValereApiSystemListManagerTest(unittest.TestCase):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_create_list_manager(self):
        name = 'testing'

        listmanager = ListManager(name)

        self.assertEqual(listmanager.name, name)
        self.assertIsNone(listmanager.current)
        self.assertIsNone(listmanager.expired)

    def test_set_current_list(self):
        name = 'testing'

        listmanager = ListManager(name)

        self.assertEqual(listmanager.name, name)
        self.assertIsNone(listmanager.current)
        self.assertIsNone(listmanager.expired)

        dictum = dict()

        listmanager.current = dictum

        self.assertEqual(listmanager.current, dictum)

        listmanager.current = None

        self.assertIsNone(listmanager.current)

    def test_set_expired_list(self):
        name = 'testing'

        listmanager = ListManager(name)

        self.assertEqual(listmanager.name, name)
        self.assertIsNone(listmanager.current)
        self.assertIsNone(listmanager.expired)

        dictum = dict()

        listmanager.expired = dictum

        self.assertEqual(listmanager.expired, dictum)

        listmanager.expired = None

        self.assertIsNone(listmanager.expired)
