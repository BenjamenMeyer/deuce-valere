"""
Deuce Valere - Tests - API - System - Manager - Serialization
"""
import datetime
import unittest

from deuceclient.tests import *

from deucevalere.api.system import *
from deucevalere.tests.client_base import TestValereClientBase


class DeuceValereApiSystemManagerSerializationTest(TestValereClientBase):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_manager_serialization_default(self):
        manager = Manager()
        json_data = manager.to_json()
        deserialized_manager = Manager.from_json(json_data)
        self.check_manager(manager,
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
        self.check_manager(manager,
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

    def test_list_manager_deserialization_type_error(self):
        serialization_data = {
            'name': 'fake',
            'current': datetime.datetime.max,
            'expired': datetime.date.max,
            'deleted': datetime.time.max,
            'orphaned': datetime.datetime.utcnow()
        }
        with self.assertRaises(TypeError):
            ListManager.deserialize(serialization_data)
