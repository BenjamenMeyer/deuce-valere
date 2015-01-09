#!/usr/bin/python3
"""
Deuce Valere - Shell
"""
from __future__ import print_function
import argparse
import datetime
import json
import logging
import os
import os.path
import pprint
import sys

from deuceclient import api as deuce_api
import deuceclient.client.deuce as client
import prettytable

from deucevalere import vault_cleanup as valere_cleanup
from deucevalere import vault_reload as valere_reload_vault
from deucevalere import vault_validate as valere_validate
from deucevalere.api.auth import *
from deucevalere.api.shell_cache import ShellCache
from deucevalere.api.system import Manager


class ProgramArgumentError(ValueError):
    pass


def __api_operation_prep(log, arguments):
    """
    API Operation Common Functionality
    """
    # Parse the user data
    example_user_config_json = """
    {
        'user': <username>,
        'username': <username>,
        'user_name': <username>,
        'user_id': <userid>
        'tenant_name': <tenantname>,
        'tenant_id': <tenantid>,
        'apikey': <apikey>,
        'password': <password>,
        'token': <token>
    }

    Note: Only one of user, username, user_name, user_id, tenant_name,
          or tenant_id must be specified.

    Note: Only one of apikey, password, token must be specified.
        Token preferred over apikey or password.
        Apikey preferred over password.
    """
    auth_url = arguments.auth_service_url
    auth_provider = arguments.auth_service

    auth_data = {
        'user': {
            'value': None,
            'type': None
        },
        'credentials': {
            'value': None,
            'type': None
        }
    }

    def find_user(data):
        user_list = [
            ('user', 'user_name'),
            ('username', 'user_name'),
            ('user_name', 'user_name'),
            ('user_id', 'user_id'),
            ('tenant_name', 'tenant_name'),
            ('tenant_id', 'tenant_id'),
        ]

        for u in user_list:
            try:
                auth_data['user']['value'] = user_data[u[0]]
                auth_data['user']['type'] = u[1]
                return True
            except LookupError:
                pass

        return False

    def find_credentials(data):
        credential_list = ['token', 'password', 'apikey']
        for credential_type in credential_list:
            try:
                auth_data['credentials']['value'] = user_data[credential_type]
                auth_data['credentials']['type'] = credential_type
                return True
            except LookupError:
                pass

        return False

    user_data = json.load(arguments.user_config)
    if not find_user(user_data):
        sys.stderr.write('Unknown User Type.\n Example Config: {0:}'.format(
            example_user_config_json))
        sys.exit(-2)

    if not find_credentials(user_data):
        sys.stderr.write('Unknown Auth Type.\n Example Config: {0:}'.format(
            example_user_config_json))
        sys.exit(-3)

    # Setup the Authentication
    datacenter = arguments.datacenter

    asp = None
    if auth_provider == 'openstack':
        asp = openstackauth.OpenStackAuthentication

    elif auth_provider == 'rackspace':
        asp = rackspaceauth.RackspaceAuthentication

    elif auth_provider == 'none':
        asp = noauth.NonAuthAuthentication

    else:
        sys.stderr.write('Unknown Authentication Service Provider'
                         ': {0:}'.format(auth_provider))
        sys.exit(-4)

    auth_engine = asp(userid=auth_data['user']['value'],
                      usertype=auth_data['user']['type'],
                      credentials=auth_data['credentials']['value'],
                      auth_method=auth_data['credentials']['type'],
                      datacenter=datacenter,
                      auth_url=auth_url)

    # Deuce URL
    uri = arguments.url

    # Setup Agent Access
    deuce = client.DeuceClient(auth_engine, uri)

    try:
        vault = deuce.GetVault(arguments.vault_name)
    except Exception as ex:
        print('Unable to access Vault {0}. Error: {1}'
              .format(arguments.vault_name, str(ex)))
        sys.exit(-1)

    valere_shell_cache = ShellCache()

    return (auth_engine, deuce, vault, valere_shell_cache)


def print_report(manager, title=None, show_missing=True,
                 show_expired=True, show_orphaned=True,
                 show_deleted=True):
    table_columns = [
        'counter',
        'count',
        'gigabytes',
        'megabytes',
        'kilobytes',
        'bytes'
    ]
    display_table = prettytable.PrettyTable(table_columns)

    def calc_kilobytes(b):
        return b / 1024.0

    def calc_megabytes(b):
        return calc_kilobytes(b) / 1024.0

    def calc_gigabytes(b):
        return calc_megabytes(b) / 1024.0

    display_table.add_row(['Current',
                           manager.current_counter.count,
                           calc_gigabytes(manager.current_counter.size),
                           calc_megabytes(manager.current_counter.size),
                           calc_kilobytes(manager.current_counter.size),
                           manager.current_counter.size])

    if show_missing is True:
        display_table.add_row(['Missing',
                               manager.missing_counter.count,
                               calc_gigabytes(manager.missing_counter.size),
                               calc_megabytes(manager.missing_counter.size),
                               calc_kilobytes(manager.missing_counter.size),
                               manager.missing_counter.size])

    if show_expired is True:
        display_table.add_row(['Expired',
                               manager.expired_counter.count,
                               calc_gigabytes(manager.expired_counter.size),
                               calc_megabytes(manager.expired_counter.size),
                               calc_kilobytes(manager.expired_counter.size),
                               manager.expired_counter.size])

    if show_orphaned is True:
        display_table.add_row(['Orphaned',
                               manager.orphaned_counter.count,
                               calc_gigabytes(manager.orphaned_counter.size),
                               calc_megabytes(manager.orphaned_counter.size),
                               calc_kilobytes(manager.orphaned_counter.size),
                               manager.orphaned_counter.size])

    if show_deleted is True:
        display_table.add_row(['Deleted',
                               manager.deleted_counter.count,
                               calc_gigabytes(manager.deleted_counter.size),
                               calc_megabytes(manager.deleted_counter.size),
                               calc_kilobytes(manager.deleted_counter.size),
                               manager.deleted_counter.size])

    if title:
        print(title)
    print(display_table)


def vault_validate(log, arguments):
    """Vault Validate

    Validate the data in a vault
    """
    auth_engine, deuceclient, vault, cache = __api_operation_prep(log,
                                                                  arguments)

    vault_data_expiration = datetime.timedelta(
        seconds=arguments.data_expiration) if arguments.data_expiration\
        else None

    manager = Manager(marker_start=arguments.start,
                      marker_end=arguments.end,
                      expire_age=vault_data_expiration)

    returnValue = valere_validate(deuceclient,
                           vault,
                           manager)
    # On success we want to save the data before terminating
    if returnValue == 0:
        cache.add_manager(vault, manager)
        cache.add_vault(vault, manager)
        # hide deleted b/c that is not covered here
        # hide missing b/c it cannot be detected yet
        print_report(manager,
                     title='Validation Report',
                     show_missing=False,
                     show_deleted=False)
    else:
        print('Validation Failed with error code {0}'.format(returnValue))

    sys.exit(returnValue)


def vault_cleanup(log, arguments):
    """Vault Cleanup

    Cleanup orphaned data in a vault
    """
    auth_engine, deuceclient, vault, cache = __api_operation_prep(log,
                                                                  arguments)

    cache_expiration = datetime.timedelta(seconds=arguments.cache_expiration)

    try:
        manager = cache.get_manager(vault,
                                    arguments.start,
                                    arguments.end,
                                    cache_expiration)
    except Exception as ex:
        print('Cache error while loading manager: {0}'.format(ex))
        manager = Manager(marker_start=arguments.start,
                          marker_end=arguments.end)
        print('Manager data was not located in the cache. Re-validating.')
        returnValue = valere_validate(deuceclient,
                                      vault,
                                      manager)
        if returnValue != 0:
            print('Re-Validation Failed with error code {0}'
                  .format(returnValue))
            return returnValue

    manager.expire_age = datetime.timedelta(
        seconds=arguments.data_expiration) if arguments.data_expiration\
        else None

    try:
        vault = cache.load_vault(vault,
                                 arguments.start,
                                 arguments.end,
                                 cache_expiration)
    except Exception as ex:
        print('Error loading vault from cache: {0}'.format(ex))
        print('Vault data was not located in the cache. Reloading from Deuce.')
        valere_reload_vault(deuceclient,
                            vault,
                            manager)

    returnValue = valere_cleanup(deuceclient,
                                 vault,
                                 manager)
    if returnValue == 0:
        # Cache values are not longer valid so remove them
        cache.clear_manager(vault, manager)
        cache.clear_vault(vault, manager)
    else:
        print('Clean up Failed with error code {0}'.format(returnValue))

    # hide missing b/c that cannot be detected yet
    print_report(manager, title='Cleanup Report', show_missing=False)

    sys.exit(returnValue)


def main():
    arg_parser = argparse.ArgumentParser(
        description="Deuce Cleanup and Validation Client")

    arg_parser.add_argument('--user-config',
                            default=None,
                            type=argparse.FileType('r'),
                            required=True,
                            help='JSON file containing username and API Key')
    arg_parser.add_argument('--url',
                            default='127.0.0.1:8080',
                            type=str,
                            required=False,
                            help="Network Address for the Deuce Server."
                                 " Default: 127.0.0.1:8080")
    arg_parser.add_argument('-lg', '--log-config',
                            default=None,
                            type=str,
                            dest='logconfig',
                            help='log configuration file')
    arg_parser.add_argument('-dc', '--datacenter',
                            default='ord',
                            type=str,
                            dest='datacenter',
                            required=True,
                            help='Datacenter the system is in',
                            choices=['lon', 'syd', 'hkg', 'ord', 'iad', 'dfw'])
    arg_parser.add_argument('--auth-service',
                            default='rackspace',
                            type=str,
                            required=False,
                            help='Authentication Service Provider',
                            choices=['openstack', 'rackspace', 'none'])
    arg_parser.add_argument('--auth-service-url',
                            default=None,
                            type=str,
                            required=False,
                            help='Authentication Service Provider URL')
    arg_parser.add_argument('--vault-name',
                            default=None,
                            required=True,
                            help="Vault Name")
    arg_parser.add_argument('--start',
                            default=None,
                            required=False,
                            type=str,
                            help='Marking denoting the starting'
                                 'block name in the vault')
    arg_parser.add_argument('--end',
                            default=None,
                            required=False,
                            type=str,
                            help='Marking denoting the ending'
                                 'block name in the vault')
    arg_parser.add_argument('--cache-expiration',
                            default=360,
                            required=False,
                            type=int,
                            help='Expiration of the cache data in seconds')
    arg_parser.add_argument('--data-expiration',
                            default=3600,
                            required=False,
                            type=int,
                            help='Expiration of the Vault data in seconds')
    sub_argument_parser = arg_parser.add_subparsers(title='subcommands')

    vault_validation_parser = sub_argument_parser.add_parser('validate')
    vault_validation_parser.set_defaults(func=vault_validate)

    vault_cleanup_parser = sub_argument_parser.add_parser('cleanup')
    vault_cleanup_parser.set_defaults(func=vault_cleanup)

    arguments = arg_parser.parse_args()

    # If the caller provides a log configuration then use it
    # Otherwise we'll add our own little configuration as a default
    # That captures stdout and outputs to .deuce_valere-py.log
    if arguments.logconfig is not None:
        logging.config.fileConfig(arguments.logconfig)
    else:
        lf = logging.FileHandler('.deuce_valere-py.log')
        lf.setLevel(logging.DEBUG)

        log = logging.getLogger()
        log.addHandler(lf)
        log.setLevel(logging.DEBUG)

    logging.captureWarnings(True)
    # Build the logger
    log = logging.getLogger()

    return arguments.func(log, arguments)

if __name__ == "__main__":
    sys.exit(main())
