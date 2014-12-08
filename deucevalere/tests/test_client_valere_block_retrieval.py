"""
Deuce Valere - Tests - Client - Valere - Block Retrieval
"""
import json

from deuceclient.tests import *
import httpretty

from deucevalere.tests import *
from deucevalere.tests.client_base import TestValereClientBase


@httpretty.activate
class TestValereClientBlockRetrieval(TestValereClientBase):

    def setUp(self):
        super().setUp()
        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.generate_blocks(count=100)

    def tearDown(self):
        super().tearDown()

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

        for block_id in self.meta_data.keys():
            self.assertIn(block_id, self.manager.metadata.current)

    def test_get_metadata_block_list_with_end(self):
        sorted_metadata_info = sorted(self.meta_data.keys())

        end_position = self.metadata_calculate_position()

        self.secondary_setup(manager_start=None,
                             manager_end=sorted_metadata_info[end_position])

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

        for block_id in self.meta_data.keys():
            if block_id < sorted_metadata_info[end_position]:
                self.assertIn(block_id, self.manager.metadata.current)
            else:
                self.assertNotIn(block_id, self.manager.metadata.current)

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

        for block_id in self.storage_data.keys():
            self.assertIn(block_id, self.manager.storage.current)

    def test_get_storage_block_list_with_end(self):
        sorted_metadata_info = sorted(self.meta_data.keys())

        end_position = self.metadata_calculate_position()

        self.secondary_setup(manager_start=None,
                             manager_end=sorted_metadata_info[end_position])

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

        for block_id in self.storage_data.keys():
            if block_id < sorted_metadata_info[end_position]:
                self.assertIn(block_id, self.manager.storage.current)
            else:
                self.assertNotIn(block_id, self.manager.storage.current)
