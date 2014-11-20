"""
Deuce Valere - Tests
"""
from deucevalere.api.auth import baseauth


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
