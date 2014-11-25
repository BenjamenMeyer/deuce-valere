"""
Deuce Valere - Common - Validation
"""
import datetime

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


@validation_function
def val_expire_age(value):
    if not isinstance(value, datetime.timedelta):
        raise ValidationFailed('must be type datetime.timedelta')


def _abort(error_code):
    abort_errors = {
        100: TypeError
    }
    raise abort_errors[error_code]

AuthEngineRule = Rule(val_authenticator_instance(), lambda: _abort(100))
ClientRule = Rule(val_deuceclient_instance(), lambda: _abort(100))

ExpireAgeRule = Rule(val_expire_age(), lambda: _abort(100))
ExpireAgeRuleNoneOkay = Rule(val_expire_age(none_ok=True), lambda: _abort(100))
