"""
Deuce Valere - Common - Validation
"""
from deuceclient.api import *
from deuceclient.auth.base import AuthenticationBase
from deuceclient.client.deuce import DeuceClient
from deuceclient.common.validation import *
from deuceclient.common.validation_instance import *
from stoplight import Rule, ValidationFailed, validation_function


@validation_function
def val_authenticator_instance(value):
    if not isinstance(value, AuthenticationBase):
        raise ValidationFailed('authenticator must be derived from '
                               'deuceclient.auth.base.AuthenticationBase')


@validation_function
def val_deuceclient_instance(value):
    if not isinstance(value, DeuceClient):
        raise ValidationFailed('invalid Deuce Client instance')


def _abort(error_code):
    abort_errors = {
        100: TypeError
    }
    raise abort_errors[error_code]

AuthEngineRule = Rule(val_authenticator_instance(), lambda: _abort(100))
ClientRule = Rule(val_deuceclient_instance(), lambda: _abort(100))
