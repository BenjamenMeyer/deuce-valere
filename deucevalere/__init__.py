"""
Deuce Valere
"""
from stoplight import validate

from deucevalere.api.auth import *
from deucevalere.api.system import Manager
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
          vault=VaultInstanceRule)
def vault_validate(authengine, client, vault, manager):
    """Validate the Deuce Vault by checking all blocks exist

    :param authengine: instance of deuceclient.auth.Authentication
                       to use for retrieving tokens
    :param client: instance of deuceclient.client.deuce
                   to use for interacting with the Deuce Service
    :param vault: instance of deuceclient.api.Vault for the
                  vault to be inspected and cleaned
    :param manager: deucevalere.api.system.Manager instance that will track
                    the state of the system and all statistics

    :returns: zero on success
    :raises: RuntimeError on error
    """
    return 0


@validate(authengine=AuthEngineRule,
          client=ClientRule,
          vault=VaultInstanceRule)
def vault_cleanup(authengine, client, vault, manager):
    """Cleanup the Deuce Vault by removing orphaned data

    :param authengine: instance of deuceclient.auth.Authentication
                       to use for retrieving tokens
    :param client: instance of deuceclient.client.deuce
                   to use for interacting with the Deuce Service
    :param vault: instance of deuceclient.api.Vault for the
                  vault to be inspected and cleaned
    :param manager: deucevalere.api.system.Manager instance that will track
                    the state of the system and all statistics

    :returns: zero on success
    :raises: RuntimeError on error
    """
    return 0
