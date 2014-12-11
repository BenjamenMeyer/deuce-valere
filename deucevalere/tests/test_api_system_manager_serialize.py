"""
Deuce Valere - Tests - API - System - Manager - Serialization
"""
import datetime
import unittest

from deuceclient.tests import *

from deucevalere.api.system import *


class DeuceValereApiSystemManagerSerializationTest(unittest.TestCase):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def __check_time_manager(self, a, b):
        self.assertEqual(a.name, b.name)
        self.assertEqual(a.start, b.start)
        self.assertEqual(a.end, b.end)

    def __check_counter_manager(self, a, b):
        self.assertEqual(a.name, b.name)
        self.assertEqual(a.count, b.count)
        self.assertEqual(a.size, b.size)

    def __check_list_manager(self, a, b):
        def __check_list(c, d):
            if c is None:
                self.assertIsNone(d)
            else:
                self.assertEqual(c, d)

        self.assertEqual(a.name, b.name)
        __check_list(a.current, b.current)
        __check_list(a.expired, b.expired)
        __check_list(a.deleted, b.deleted)
        __check_list(a.orphaned, b.orphaned)

    def __check_manager(self, a, b):
        def __check_marker(c, d):
            if c is None:
                self.assertIsNone(d)
            else:
                self.assertEqual(c, d)

        self.assertEqual(a.expire_age, b.expire_age)
        __check_marker(a.start_block, b.start_block)
        __check_marker(a.end_block, b.end_block)
        self.__check_time_manager(a.validation_timer,
                                  b.validation_timer)
        self.__check_time_manager(a.cleanup_timer,
                                  b.cleanup_timer)
        self.__check_counter_manager(a.expired_counter,
                                     b.expired_counter)
        self.__check_counter_manager(a.missing_counter,
                                     b.missing_counter)
        self.__check_counter_manager(a.orphaned_counter,
                                     b.orphaned_counter)
        self.__check_list_manager(a.metadata,
                                  b.metadata)
        self.__check_list_manager(a.storage,
                                  b.storage)
        self.assertEqual(a.cross_reference,
                         b.cross_reference)

    def test_manager_serialization_default(self):
        manager = Manager()
        json_data = manager.to_json()
        deserialized_manager = Manager.from_json(json_data)
        self.__check_manager(manager,
                             deserialized_manager)

    def test_manager_serialization_with_data(self):
        manager = Manager()
        with manager.validation_timer:
            with manager.cleanup_timer:
                manager.expired_counter.add(50, 100)
                manager.missing_counter.add(100, 50)
                manager.orphaned_counter.add(200, 200)
                manager.metadata.current = [50, 100]
                manager.metadata.expired = [100, 50]
                manager.metadata.deleted = [200, 200]
                manager.metadata.orphaned = [50, 100, 100, 50, 200, 200]
                manager.storage.current = [200, 200, 50, 100, 100, 50]
                manager.storage.expired = [200, 200]
                manager.storage.deleted = [100, 50]
                manager.storage.orphaned = [50, 100]
        json_data = manager.to_json()
        deserialized_manager = Manager.from_json(json_data)
        self.__check_manager(manager,
                             deserialized_manager)

    def test_time_manager_deserialization_value_error(self):
        serialization_data = {
            'name': 'fake',
            'start': 'bad',
            'end': 'value'
        }
        with self.assertRaises(ValueError):
            TimeManager.deserialize(serialization_data)

    def test_time_manager_deserialization_type_error(self):
        serialization_data = {
            'name': 'fake',
            'start': list(),
            'end': set()
        }
        with self.assertRaises(TypeError):
            TimeManager.deserialize(serialization_data)

    def test_list_manager_deserialization_value_error(self):
        serialization_data = {
            'name': 'fake',
            'current': 'alice',
            'expired': 'in',
            'deleted': 'wonder',
            'orphaned': 'land'
        }
        with self.assertRaises(ValueError):
            ListManager.deserialize(serialization_data)

    def test_list_manager_deserialization_typee_error(self):
        serialization_data = {
            'name': 'fake',
            'current': datetime.datetime.max,
            'expired': datetime.date.max,
            'deleted': datetime.time.max,
            'orphaned': datetime.datetime.utcnow()
        }
        with self.assertRaises(TypeError):
            ListManager.deserialize(serialization_data)
