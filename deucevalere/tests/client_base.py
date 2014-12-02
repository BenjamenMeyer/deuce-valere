"""
Deuce Valere - Client - Base Test Functionality
"""
import json
import unittest
import urllib.parse
import uuid

from deuceclient.api import Vault
from deuceclient.client.deuce import DeuceClient
from deuceclient.tests import *
import httpretty

from deucevalere.api.system import Manager
from deucevalere.client.valere import ValereClient
from deucevalere.tests import *


class TestValereClientBase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.meta_data = None
        self.storage_data = None

    def tearDown(self):
        super().tearDown()

    def generate_blocks(self, count):
        # Generate a list of metadata block ids - 100 blocks
        self.meta_data = sorted(
            [block[0] for block in create_blocks(block_count=count)]
        )

        # Generate a list of equivalent storage blocks
        self.storage_data = sorted([
            '{0}_{1}'.format(block_id, uuid.uuid4())
            for block_id in self.meta_data
        ])

    def secondary_setup(self, manager_start, manager_end):
        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.vault = Vault(self.project_id, self.vault_id)
        self.apihost = 'neo.the.one'
        self.authengine = FakeAuthEngine(userid='blue',
                                         usertype='pill',
                                         credentials='morpheus',
                                         auth_method='matrix')
        self.deuce_client = DeuceClient(self.authengine,
                                        self.apihost,
                                        True)
        self.manager = Manager(marker_start=manager_start,
                               marker_end=manager_end)
        self.client = ValereClient(self.deuce_client, self.vault, self.manager)

    def metadata_body_generator(self, uri):
        def get_group(gg_start, gg_end):
            url_base = get_blocks_url(self.apihost,
                                      self.vault.vault_id)

            url = None
            block_set = None
            if gg_start is not None:
                block_set = self.meta_data[gg_start:gg_end]
            else:
                block_set = self.meta_data[:gg_end]

            if gg_end is not None:
                block_next = self.meta_data[gg_end]

                url_params = urllib.parse.urlencode({'marker': block_next})
                next_batch = '{0}?{1}'.format(url_base, url_params)

                return (block_set, next_batch)

            else:
                return (block_set, None)

        parsed_url = urllib.parse.urlparse(uri)
        qs = urllib.parse.parse_qs(parsed_url[4])

        start = 0
        end = len(self.meta_data)
        if 'marker' in qs:
            marker = qs['marker'][0]

            new_start = 0
            for check_index in range(len(self.meta_data)):
                if self.meta_data[check_index] >= marker:
                    new_start = check_index
                    break

            if new_start > start and new_start <= len(self.meta_data):
                start = new_start
                end = start + int(len(self.meta_data) / 3)

            if end >= len(self.meta_data):
                end = None

        else:
            start = None
            end = int(len(self.meta_data) / 3)

        return get_group(start, end)

    def storage_body_generator(self, uri):
        def get_group(gg_start, gg_end):
            url_base = get_storage_blocks_url(self.apihost,
                                              self.vault.vault_id)

            url = None
            block_set = None
            if gg_start is not None:
                block_set = self.storage_data[gg_start:gg_end]
            else:
                block_set = self.storage_data[:gg_end]

            if gg_end is not None:
                block_next = self.storage_data[gg_end]

                url_params = urllib.parse.urlencode({'marker': block_next})
                next_batch = '{0}?{1}'.format(url_base, url_params)

                return (block_set, next_batch)

            else:
                return (block_set, None)

        parsed_url = urllib.parse.urlparse(uri)
        qs = urllib.parse.parse_qs(parsed_url[4])

        start = 0
        end = len(self.storage_data)
        if 'marker' in qs:
            marker = qs['marker'][0]

            new_start = 0
            for check_index in range(len(self.storage_data)):
                if self.storage_data[check_index] >= marker:
                    new_start = check_index
                    break

            if new_start > start and new_start <= len(self.storage_data):
                start = new_start
                end = start + int(len(self.storage_data) / 3)

            if end >= len(self.storage_data):
                end = None

        else:
            start = None
            end = int(len(self.storage_data) / 3)

        return get_group(start, end)
