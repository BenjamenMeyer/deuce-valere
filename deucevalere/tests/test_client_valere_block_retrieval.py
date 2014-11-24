"""
Deuce Valere - Tests - Client - Valere
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


class TestValereClientBlockRetrieval(unittest.TestCase):

    def setUp(self):
        super().setUp()

        # Generate a list of metadata block ids - 100 blocks
        self.meta_data = sorted(
            [block[0] for block in create_blocks(block_count=100)]
        )

        # Generate a list of equivalent storage blocks
        self.storage_data = sorted([
            '{0}_{1}'.format(block_id, uuid.uuid4())
            for block_id in self.meta_data
        ])

    def tearDown(self):
        super().tearDown()

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
                block_start = self.meta_data[gg_start]
                url_params = urllib.parse.urlencode({'marker': block_start})
                url = '{0}?{1}'.format(url_base, url_params)
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
                block_start = self.storage_data[gg_start]
                url_params = urllib.parse.urlencode({'marker': block_start})
                url = '{0}?{1}'.format(url_base, url_params)
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

    @httpretty.activate
    def test_get_metadata_block_list(self):

        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def metadata_callback(request, uri, headers):
            body, next_batch = self.metadata_body_generator(uri)
            if next_batch is not None:
                headers.update({'x-next-batch': next_batch})

            return (200, headers, json.dumps(body))

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_callback)

        self.client.get_block_list()

        self.assertEqual(len(self.manager.metadata.current),
                         len(self.meta_data))

        for block_id in self.meta_data:
            self.assertIn(block_id, self.manager.metadata.current)

    @httpretty.activate
    def test_get_metadata_block_list_with_end(self):

        length = int(len(self.meta_data) / 3)

        self.secondary_setup(manager_start=None,
                             manager_end=self.meta_data[length])

        def metadata_callback(request, uri, headers):
            body, next_batch = self.metadata_body_generator(uri)
            if next_batch is not None:
                headers.update({'x-next-batch': next_batch})

            return (200, headers, json.dumps(body))

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_callback)

        self.client.get_block_list()

        self.assertEqual(len(self.manager.metadata.current),
                         length)

        for block_id in self.meta_data:
            if block_id < self.meta_data[length]:
                self.assertIn(block_id, self.manager.metadata.current)
            else:
                self.assertNotIn(block_id, self.manager.metadata.current)

    @httpretty.activate
    def test_get_storage_block_list(self):

        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def storage_callback(request, uri, headers):
            body, next_batch = self.storage_body_generator(uri)
            if next_batch is not None:
                headers.update({'x-next-batch': next_batch})

            return (200, headers, json.dumps(body))

        url = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=storage_callback)

        self.client.get_storage_list()

        self.assertEqual(len(self.manager.storage.current),
                         len(self.storage_data))

        for block_id in self.storage_data:
            self.assertIn(block_id, self.manager.storage.current)

    @httpretty.activate
    def test_get_storage_block_list_with_end(self):

        length = int(len(self.meta_data) / 3)

        self.secondary_setup(manager_start=None,
                             manager_end=self.meta_data[length])

        def storage_callback(request, uri, headers):
            body, next_batch = self.storage_body_generator(uri)
            if next_batch is not None:
                headers.update({'x-next-batch': next_batch})

            return (200, headers, json.dumps(body))

        url = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=storage_callback)

        self.client.get_storage_list()

        self.assertEqual(len(self.manager.storage.current),
                         length)

        for block_id in self.storage_data:
            if block_id < self.meta_data[length]:
                self.assertIn(block_id, self.manager.storage.current)
            else:
                self.assertNotIn(block_id, self.manager.storage.current)
