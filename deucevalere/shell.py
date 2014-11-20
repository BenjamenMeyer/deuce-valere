#!/usr/bin/python3
"""
Deuce Valere - Shell
"""
from __future__ import print_function
import argparse
import logging
import pprint
import sys

from deuceclient import api as deuce_api
import deuceclient.client.deuce as client

from deucevalere import vault_cleanup as valere_cleanup
from deucevalere import vault_validate as valere_validate
from deucevalere.api.auth import *
from deucevalare.api.system import Manager


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

    return (auth_engine, deuce, vault)


def vault_validate(log, arguments):
    """Vault Validate

    Validate the data in a vault
    """
    auth_engine, deuceclient, vault = __api_operation_prep(log, arguments)

    manager = Manager(marker_start=arguments.start,
                      marker_end=arguments.end)

    return valere_valiate(auth_engine,
                          deuceclient,
                          vault,
                          manager)


def vault_cleanup(log, arguments):
    """Vault Cleanup

    Cleanup orphaned data in a vault
    """
    auth_engine, deuceclient, vault = __api_operation_prep(log, arguments)

    manager = Manager(marker_start=arguments.start,
                      marker_end=arguments.end)

    return valere_cleanup(auth_engine,
                          deuceclient,
                          vault,
                          manager)


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
    sub_argument_parser = arg_parser.add_subparsers(title='subcommands')

    vault_validation_parser = sub_argument_parser.add_parser('validate')
    vault_validation_parser.set_defaults(func=vault_validate)

    vault_cleanup_parser = sub_argument_parser.add_parser('cleanup')
    vault_cleanup_parser.set_defaults(func=vault_cleanup)

    arguments = arg_parser.parse_args()

    # If the caller provides a log configuration then use it
    # Otherwise we'll add our own little configuration as a default
    # That captures stdout and outputs to output/integration-slave-server.out
    if arguments.logconfig is not None:
        logging.config.fileConfig(arguments.logconfig)
    else:
        lf = logging.FileHandler('.deuce_client-py.log')
        lf.setLevel(logging.DEBUG)

        log = logging.getLogger()
        log.addHandler(lf)
        log.setLevel(logging.DEBUG)

    # Build the logger
    log = logging.getLogger()

    return arguments.func(log, arguments)

if __name__ == "__main__":
    sys.exit(main())
