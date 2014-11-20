"""
Deuce Valere - Tests - API - System - Time Manager
"""
import datetime
import time
import unittest

from deucevalere.api.system import TimeManager


class DeuceValereApiSystemTimeManagerTest(unittest.TestCase):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_create_time_manager(self):
        name = 'testing'
        timemanager = TimeManager(name)

        self.assertEqual(timemanager.name, name)
        self.assertIsNone(timemanager.start)
        self.assertIsNone(timemanager.end)
        self.assertEqual(timemanager.elapsed, 0)

    def test_start_stop(self):
        name = 'testing'
        timemanager = TimeManager(name)

        self.assertIsNone(timemanager.start)
        self.assertIsNone(timemanager.end)
        self.assertEqual(timemanager.elapsed, 0)

        delta = datetime.timedelta(seconds=0.1)

        with timemanager:
            self.assertIsNotNone(timemanager.start)
            self.assertIsNone(timemanager.end)
            time.sleep(delta.total_seconds())
            self.assertNotEqual(timemanager.elapsed, 0)
            self.assertGreaterEqual(timemanager.elapsed,
                                    delta)

        self.assertIsNotNone(timemanager.start)
        self.assertIsNotNone(timemanager.end)
        self.assertNotEqual(timemanager.elapsed, 0)
        self.assertGreaterEqual(timemanager.elapsed,
                                delta)

    def test_reset(self):
        name = 'testing'
        timemanager = TimeManager(name)

        self.assertIsNone(timemanager.start)
        self.assertIsNone(timemanager.end)
        self.assertEqual(timemanager.elapsed, 0)

        delta = datetime.timedelta(seconds=0.1)

        with timemanager:
            self.assertIsNotNone(timemanager.start)
            self.assertIsNone(timemanager.end)
            time.sleep(delta.total_seconds())

        self.assertIsNotNone(timemanager.start)
        self.assertIsNotNone(timemanager.end)
        self.assertNotEqual(timemanager.elapsed, 0)
        self.assertGreaterEqual(timemanager.elapsed,
                                delta)

        timemanager.reset()

        self.assertIsNone(timemanager.start)
        self.assertIsNone(timemanager.end)
        self.assertEqual(timemanager.elapsed, 0)
