"""
Deuce Valere - Tests - Common - Validation
"""
import datetime
import unittest

import deuceclient.client.deuce as client
from stoplight import validate

import deucevalere.common.validation as v
from deucevalere.tests import *


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


class TestExpireAgeRule(TestRulesBase):

    positive_cases = [
        datetime.timedelta(microseconds=1),
        datetime.timedelta(milliseconds=1),
        datetime.timedelta(seconds=2),
        datetime.timedelta(minutes=3),
        datetime.timedelta(hours=5),
        datetime.timedelta(days=8),
        datetime.timedelta(weeks=13)
    ]

    negative_cases = [
        None,
        1, 1, 2, 3, 5, 8, 13,
        'Monday', b'Tuesday', u'Wednesday',
        datetime.datetime.max,
        datetime.datetime.utcnow(),
        datetime.date.max,
        datetime.date.today(),
        datetime.time.max,
        datetime.datetime.utcnow().time()
    ]

    @validate(expires=v.ExpireAgeRule)
    def check_expires(self, expires):
        return True

    @validate(expires=v.ExpireAgeRuleNoneOkay)
    def check_expires_with_none(self, expires):
        return True

    def test_expire_age_instance(self):
        for p_case in TestExpireAgeRule.positive_cases:
            v.val_expire_age()(p_case)

        for case in TestExpireAgeRule.negative_cases:
            with self.assertRaises(v.ValidationFailed):
                v.val_expire_age()(case)

    def test_expire_age_rule(self):
        for p_case in TestExpireAgeRule.positive_cases:
            self.check_expires(p_case)

        for case in TestExpireAgeRule.negative_cases:
            with self.assertRaises(TypeError):
                self.check_expires(case)

    def test_expire_age_rule_with_none(self):
        positive_cases, negative_cases = self.cases_with_none_okay()

        for p_case in positive_cases:
            self.check_expires_with_none(p_case)

        for case in negative_cases:
            with self.assertRaises(TypeError):
                self.check_expires_with_none(case)
