"""
Deuce Valere
"""
from stoplight import validate

from deucevalere.api.auth import *
from deucevalere.common.validation import *


__DEUCE_VALERE_VERSION__ = {
    'major': 0,
    'minor': 1
}


def version():
    """Return the Deuce Valere Version"""
    return '{0:}.{1:}'.format(__DEUCE_VALERE_VERSION__['major'],
                              __DEUCE_VALERE_VERSION__['minor'])


@validate(authengine=AuthEngineRule,
          client=ClientRule,
          vault=VaultInstanceRule,
          start_marker=MetadataBlockIdRuleNoneOkay,
          end_marker=MetadataBlockIdRuleNoneOkay)
def vault_validate(authengine, client, vault, start_marker, end_marker):
    """Validate the Deuce Vault by checking all blocks exist

    :param authengine: instance of deuceclient.auth.Authentication
                       to use for retrieving tokens
    :param client: instance of deuceclient.client.deuce
                   to use for interacting with the Deuce Service
    :param vault: instance of deuceclient.api.Vault for the
                  vault to be inspected and cleaned
    :param start_marker: the start of the range to use, inclusive, may be None
    :param end_marker: the end of the range to use, inclusive, may be None

    :returns: zero on success
    :raises: RuntimeError on error
    """
    markers = {
        'start': start_marker if start_marker else None,
        'end': end_marker if end_marker else None
    }

    return 0


@validate(authengine=AuthEngineRule,
          client=ClientRule,
          vault=VaultInstanceRule,
          start_marker=StorageBlockIdRuleNoneOkay,
          end_marker=StorageBlockIdRuleNoneOkay)
def vault_cleanup(authengine, client, vault, start_marker, end_marker):
    """Cleanup the Deuce Vault by removing orphaned data

    :param authengine: instance of deuceclient.auth.Authentication
                       to use for retrieving tokens
    :param client: instance of deuceclient.client.deuce
                   to use for interacting with the Deuce Service
    :param vault: instance of deuceclient.api.Vault for the
                  vault to be inspected and cleaned
    :param start_marker: the start of the range to use, inclusive, may be None
    :param end_marker: the end of the range to use, inclusive, may be None

    :returns: zero on success
    :raises: RuntimeError on error
    """
    markers = {
        'start': start_marker if start_marker else None,
        'end': end_marker if end_marker else None
    }

    return 0
