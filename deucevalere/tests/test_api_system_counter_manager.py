"""
Deuce Valere - Tests - API - System - Counter Manager
"""
import unittest

from deucevalere.api.system import CounterManager


class DeuceValereApiSystemCounterManagerTest(unittest.TestCase):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_create_counter_manager(self):
        name = 'testing'

        countermanager = CounterManager(name)

        self.assertEqual(countermanager.name, name)
        self.assertEqual(countermanager.count, 0)
        self.assertEqual(countermanager.size, 0)

    def test_counter_manager_add(self):
        name = 'testing'

        countermanager = CounterManager(name)

        self.assertEqual(countermanager.name, name)
        self.assertEqual(countermanager.count, 0)
        self.assertEqual(countermanager.size, 0)

        running_counter = 0
        running_size = 0

        for c, b in [(x, x * 1024) for x in range(100)]:
            countermanager.add(c, b)
            running_counter = running_counter + c
            running_size = running_size + b
            self.assertEqual(countermanager.count, running_counter)
            self.assertEqual(countermanager.size, running_size)

    def test_counter_manager_reset(self):
        name = 'testing'

        countermanager = CounterManager(name)

        self.assertEqual(countermanager.name, name)
        self.assertEqual(countermanager.count, 0)
        self.assertEqual(countermanager.size, 0)

        running_counter = 0
        running_size = 0

        for c, b in [(x, x * 1024) for x in range(100)]:
            countermanager.add(c, b)
            running_counter = running_counter + c
            running_size = running_size + b
            self.assertEqual(countermanager.count, running_counter)
            self.assertEqual(countermanager.size, running_size)

        countermanager.reset()
        self.assertEqual(countermanager.count, 0)
        self.assertEqual(countermanager.size, 0)
