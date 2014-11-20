"""
Deuce Valere - Tests - API - System - Manager
"""
import unittest

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
        self.assertIsInstance(manager.metadata, ListManager)
        self.assertIsInstance(manager.storage, ListManager)

    def test_create_cases(self):

        cases = [
            ('scott', 'jean'),
            ('wolverine', None),
            (None, None)
        ]

        for start, end in cases:
            x = Manager(marker_start=start, marker_end=end)
            self.assertEqual(x.start_block, start)
            self.assertEqual(x.end_block, end)
