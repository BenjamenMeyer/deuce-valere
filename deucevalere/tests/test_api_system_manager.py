"""
Deuce Valere - Tests - API - System - Manager
"""
import datetime
import unittest

from deuceclient.tests import *

from deucevalere.api.system import *


class DeuceValereApiSystemManagerTest(unittest.TestCase):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_create_default(self):
        manager = Manager()
        self.assertIsNone(manager.start_block)
        self.assertIsNone(manager.end_block)
        self.assertIsInstance(manager.validation_timer, TimeManager)
        self.assertIsInstance(manager.cleanup_timer, TimeManager)
        self.assertIsInstance(manager.expired_counter, CounterManager)
        self.assertIsInstance(manager.missing_counter, CounterManager)
        self.assertIsInstance(manager.orphaned_counter, CounterManager)
        self.assertIsInstance(manager.delete_expired_counter, CounterManager)
        self.assertIsInstance(manager.delete_orphaned_counter, CounterManager)
        self.assertIsInstance(manager.metadata, ListManager)
        self.assertIsInstance(manager.storage, ListManager)
        self.assertIsInstance(manager.cross_reference, dict)
        self.assertIsInstance(manager.expire_age, datetime.timedelta)

    def test_create_cases(self):

        cases = [
            # Both Start and End Markers
            (create_block()[0], create_block()[0], None),
            # Start Marker, No End Marker
            (create_block()[0], None, None),
            # No Start Marker, End Marker
            (None, create_block()[0], None),
            # Neither Starr nor End Markers
            (None, None, None),
            (None, None, datetime.timedelta(seconds=5)),
        ]

        for start, end, expire_age in cases:
            x = Manager(marker_start=start,
                        marker_end=end,
                        expire_age=expire_age)
            self.assertEqual(x.start_block, start)
            self.assertEqual(x.end_block, end)
            if expire_age is None:
                self.assertIsInstance(x.expire_age, datetime.timedelta)
            else:
                self.assertEqual(x.expire_age, expire_age)

    def test_expire_age_set(self):

        manager = Manager()

        old_age = manager.expire_age

        new_age = datetime.timedelta(seconds=3600)

        manager.expire_age = new_age
        self.assertNotEqual(manager.expire_age, old_age)
        self.assertEqual(manager.expire_age, new_age)
