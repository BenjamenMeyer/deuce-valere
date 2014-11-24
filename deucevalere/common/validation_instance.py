"""
Deuce Valere - Common - Validation - Instance
"""
from stoplight import Rule, ValidationFailed, validation_function

from deucevalere.api.system import *


@validation_function
def val_time_manager(value):
    if not isinstance(value, TimeManager):
        raise ValidationFailed('must be of type '
                               'deucevalere.api.system.TimeManager')


@validation_function
def val_counter_manager(value):
    if not isinstance(value, CounterManager):
        raise ValidationFailed('must be of type '
                               'deucevalere.api.system.CounterManager')


@validation_function
def val_list_manager(value):
    if not isinstance(value, ListManager):
        raise ValidationFailed('must be of type '
                               'deucevalere.api.system.ListManager')


@validation_function
def val_valere_manager(value):
    if not isinstance(value, Manager):
        raise ValidationFailed('must be of type '
                               'deucevalere.api.system.Manager')


def _abort(error_code):
    abort_errors = {
        101: TypeError
    }
    raise abort_errors[error_code]


TimeManagerRule = Rule(val_time_manager(), lambda: _abort(101))
CounterManagerRule = Rule(val_counter_manager(), lambda: _abort(101))
ListManagerRule = Rule(val_list_manager(), lambda: _abort(101))
ValereManagerRule = Rule(val_valere_manager(), lambda: _abort(101))
