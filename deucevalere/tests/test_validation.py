"""
Deuce Valere - Tests - Common - Validation
"""
import unittest

import deuceclient.client.deuce as client
from stoplight import validate

import deucevalere.common.validation as v
from deucevalere.tests import *


class TestRulesBase(unittest.TestCase):

    def cases_with_none_okay(self):

        positive_cases = self.__class__.positive_cases[:]
        positive_cases.append(None)

        negative_cases = self.__class__.negative_cases[:]
        while negative_cases.count(None):
            negative_cases.remove(None)
        while negative_cases.count(''):
            negative_cases.remove('')

        return (positive_cases, negative_cases)

    def iterable_cases(self):

        positive_cases = self.__class__.positive_cases[:]
        while positive_cases.count(None):
            positive_cases.remove(None)
        while positive_cases.count(''):
            positive_cases.remove('')

        negative_cases = self.__class__.negative_cases[:]
        while negative_cases.count(None):
            negative_cases.remove(None)
        while negative_cases.count(''):
            negative_cases.remove('')

        return (positive_cases, negative_cases)


class TestAuthEngineRule(TestRulesBase):

    positive_cases = [
        FakeAuthEngine(userid='onethousandandone', usertype='prisoner',
                       credentials='arabian nights', auth_method='genie')
    ]

    negative_cases = [
        None, 0, 'Aladdin', b'Sinbad', u'Jinni'
    ]

    @validate(auth_engine=v.AuthEngineRule)
    def check_auth(self, auth_engine):
        return True

    def test_auth_instance(self):

        for p_case in TestAuthEngineRule.positive_cases:
            v.val_authenticator_instance()(p_case)

        for case in TestAuthEngineRule.negative_cases:
            with self.assertRaises(v.ValidationFailed):
                v.val_authenticator_instance()(case)

    def test_auth_instance_rule(self):

        for p_case in TestAuthEngineRule.positive_cases:
            self.assertTrue(self.check_auth(p_case))

        for case in TestAuthEngineRule.negative_cases:
            with self.assertRaises(TypeError):
                self.check_auth(case)


class TestDeuceClientRule(TestRulesBase):

    positive_cases = [
        client.DeuceClient(
            FakeAuthEngine(userid='livefreeordie', usertype='republic',
                           credentials='wethepeople', auth_method='liberty'),
            'constitutions.r.us',
            True)
    ]

    negative_cases = [
        None, 0, 'Monarchy', b'Feudal', u'Communism'
    ]

    @validate(client=v.ClientRule)
    def check_client(self, client):
        return True

    def test_client_instance(self):

        for p_case in TestDeuceClientRule.positive_cases:
            v.val_deuceclient_instance()(p_case)

        for case in TestDeuceClientRule.negative_cases:
            with self.assertRaises(v.ValidationFailed):
                v.val_deuceclient_instance()(case)

    def test_client_instance_rule(self):

        for p_case in TestDeuceClientRule.positive_cases:
            self.assertTrue(self.check_client(p_case))

        for case in TestDeuceClientRule.negative_cases:
            with self.assertRaises(TypeError):
                self.check_client(case)
