"""
Deuce Valere - Tests - Client - Valere
"""
import json

from deuceclient.tests import *
import httpretty

from deucevalere.tests import *
from deucevalere.tests.client_base import TestValereClientBase


class TestValereClientBlockRetrieval(TestValereClientBase):

    def setUp(self):
        super().setUp()
        self.generate_blocks(count=100)

    def tearDown(self):
        super().tearDown()

    @httpretty.activate
    def test_get_metadata_block_list(self):

        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def metadata_callback(request, uri, headers):
            return self.metadata_block_listing_success(request,
                                                       uri,
                                                       headers)

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

        end_position = self.metadata_calculate_position()

        self.secondary_setup(manager_start=None,
                             manager_end=self.meta_data[end_position])

        def metadata_callback(request, uri, headers):
            return self.metadata_block_listing_success(request,
                                                       uri,
                                                       headers)

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_callback)

        self.client.get_block_list()

        self.assertEqual(len(self.manager.metadata.current),
                         end_position)

        for block_id in self.meta_data:
            if block_id < self.meta_data[end_position]:
                self.assertIn(block_id, self.manager.metadata.current)
            else:
                self.assertNotIn(block_id, self.manager.metadata.current)

    @httpretty.activate
    def test_get_storage_block_list(self):

        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def storage_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

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

        end_position = self.metadata_calculate_position()

        self.secondary_setup(manager_start=None,
                             manager_end=self.meta_data[end_position])

        def storage_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        url = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=storage_callback)

        self.client.get_storage_list()

        self.assertEqual(len(self.manager.storage.current),
                         end_position)

        for block_id in self.storage_data:
            if block_id < self.meta_data[end_position]:
                self.assertIn(block_id, self.manager.storage.current)
            else:
                self.assertNotIn(block_id, self.manager.storage.current)
