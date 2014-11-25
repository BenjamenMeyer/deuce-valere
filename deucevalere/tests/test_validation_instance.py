"""
Deuce Valere - Tests - Common - Validation
"""
import unittest

from stoplight import validate

from deucevalere.api.system import *
import deucevalere.common.validation_instance as v
from deucevalere.tests import *


class TestTimeManagerRule(TestRulesBase):

    positive_cases = [
        TimeManager('Monty Python'),
    ]

    negative_cases = [
        None, 0, 'K.I.T.T', b'Knight', u'Rider',
        CounterManager('Defender'),
        ListManager('Cole'),
        Manager()
    ]

    @validate(value=v.TimeManagerRule)
    def check_time_manager(self, value):
        return True

    def test_time_manager_instance(self):

        for p_case in TestTimeManagerRule.positive_cases:
            v.val_time_manager()(p_case)

        for case in TestTimeManagerRule.negative_cases:
            with self.assertRaises(v.ValidationFailed):
                v.val_time_manager()(case)

    def test_time_manager_rule(self):

        for p_case in TestTimeManagerRule.positive_cases:
            self.assertTrue(self.check_time_manager(p_case))

        for case in TestTimeManagerRule.negative_cases:
            with self.assertRaises(TypeError):
                self.check_time_manager(case)


class TestCounterManagerRule(TestRulesBase):

    positive_cases = [
        CounterManager('Black Adder'),
    ]

    negative_cases = [
        None, 0, 'Homer', b'Marge', u'Maggie',
        TimeManager('Bart'),
        ListManager('Krusty'),
        Manager()
    ]

    @validate(value=v.CounterManagerRule)
    def check_counter_manager(self, value):
        return True

    def test_counter_manager_instance(self):

        for p_case in TestCounterManagerRule.positive_cases:
            v.val_counter_manager()(p_case)

        for case in TestCounterManagerRule.negative_cases:
            with self.assertRaises(v.ValidationFailed):
                v.val_counter_manager()(case)

    def test_counter_manager_rule(self):

        for p_case in TestCounterManagerRule.positive_cases:
            self.assertTrue(self.check_counter_manager(p_case))

        for case in TestCounterManagerRule.negative_cases:
            with self.assertRaises(TypeError):
                self.check_counter_manager(case)


class TestListRuleManager(TestRulesBase):

    positive_cases = [
        ListManager('Red Dwarf'),
    ]

    negative_cases = [
        None, 0, 'Robin', b'Batman', u'Penguin',
        TimeManager('Joker'),
        CounterManager('Riddler'),
        Manager()
    ]

    @validate(value=v.ListManagerRule)
    def check_list_manager(self, value):
        return True

    def test_list_manager_instance(self):

        for p_case in TestListRuleManager.positive_cases:
            v.val_list_manager()(p_case)

        for case in TestListRuleManager.negative_cases:
            with self.assertRaises(v.ValidationFailed):
                v.val_list_manager()(case)

    def test_list_manager_rule(self):

        for p_case in TestListRuleManager.positive_cases:
            self.assertTrue(self.check_list_manager(p_case))

        for case in TestListRuleManager.negative_cases:
            with self.assertRaises(TypeError):
                self.check_list_manager(case)


class TestValereRuleManager(TestRulesBase):

    positive_cases = [
        Manager()
    ]

    negative_cases = [
        None, 0, 'Dread', b'Pirate', 'Roberts',
        TimeManager('Buttercup'),
        CounterManager('Max'),
        ListManager('Humperdinck')
    ]

    @validate(value=v.ValereManagerRule)
    def check_valere_manager(self, value):
        return True

    def test_valere_manager_instance(self):

        for p_case in TestValereRuleManager.positive_cases:
            v.val_valere_manager()(p_case)

        for case in TestValereRuleManager.negative_cases:
            with self.assertRaises(v.ValidationFailed):
                v.val_valere_manager()(case)

    def test_valere_manager_rule(self):

        for p_case in TestValereRuleManager.positive_cases:
            self.assertTrue(self.check_valere_manager(p_case))

        for case in TestValereRuleManager.negative_cases:
            with self.assertRaises(TypeError):
                self.check_valere_manager(case)
