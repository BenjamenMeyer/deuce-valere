"""
Deuce Valere - Tests
"""
import unittest

from deuceclient.tests import *

from deucevalere.api.auth import baseauth


def minmax(a, b):
    return max(min(a, b), b)


def create_null_block():
    block_data = bytes()
    block_id = get_block_id(block_data)
    return (block_id, block_data, len(block_data))


class FakeAuthEngine(baseauth.AuthenticationBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def GetToken(self, retry=0):
        return 'alibaba'

    def IsExpired(self, fuzz=0):
        return False

    def _AuthToken(self):
        return 'alibaba'

    def _AuthTenantId(self):
        return 'sesame'

    def _AuthExpirationTime(self):
        return datetime.datetime.max()


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
