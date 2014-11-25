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
        self.assertIsNone(listmanager.deleted)

    def test_set_current_list(self):
        name = 'testing'

        listmanager = ListManager(name)

        self.assertEqual(listmanager.name, name)
        self.assertIsNone(listmanager.current)
        self.assertIsNone(listmanager.expired)
        self.assertIsNone(listmanager.deleted)

        dictum = dict()

        listmanager.current = dictum

        self.assertEqual(listmanager.current, dictum)
        self.assertIsNone(listmanager.expired)
        self.assertIsNone(listmanager.deleted)

        listmanager.current = None

        self.assertIsNone(listmanager.current)
        self.assertIsNone(listmanager.expired)
        self.assertIsNone(listmanager.deleted)

    def test_set_expired_list(self):
        name = 'testing'

        listmanager = ListManager(name)

        self.assertEqual(listmanager.name, name)
        self.assertIsNone(listmanager.current)
        self.assertIsNone(listmanager.expired)
        self.assertIsNone(listmanager.deleted)

        dictum = dict()

        listmanager.expired = dictum

        self.assertIsNone(listmanager.current)
        self.assertEqual(listmanager.expired, dictum)
        self.assertIsNone(listmanager.deleted)

        listmanager.expired = None

        self.assertIsNone(listmanager.current)
        self.assertIsNone(listmanager.expired)
        self.assertIsNone(listmanager.deleted)

    def test_set_deleted_list(self):
        name = 'testing'

        listmanager = ListManager(name)

        self.assertEqual(listmanager.name, name)
        self.assertIsNone(listmanager.current)
        self.assertIsNone(listmanager.expired)
        self.assertIsNone(listmanager.deleted)

        dictum = dict()

        listmanager.deleted = dictum

        self.assertIsNone(listmanager.current)
        self.assertIsNone(listmanager.expired)
        self.assertEqual(listmanager.deleted, dictum)

        listmanager.deleted = None

        self.assertIsNone(listmanager.current)
        self.assertIsNone(listmanager.expired)
        self.assertIsNone(listmanager.deleted)
