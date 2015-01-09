"""
Deuce Valere - Tests - Client - Valere - Validate Storage With HEAD
"""
import functools
import json
import mock

from deuceclient.tests import *
import httpretty

from deucevalere.tests import *
from deucevalere.tests.client_base import TestValereClientBase


@httpretty.activate
class TestValereClientValidateStorageWithHEAD(TestValereClientBase):

    def setUp(self):
        super().setUp()
        self.count = 20
        self.orphaned_count = 15

        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.generate_blocks(count=self.count)

    def tearDown(self):
        super().tearDown()

    def test_validate_storage_with_head_failure(self):
        """Basic Validate Storage Test

            Note: "orphaned" data is only what was deleted
                  this is just due to how the test is structured.
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        def storage_head_callback(request, uri, headers):
            return (404, headers, 'mock failure')

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(0,
                         len(self.manager.storage.orphaned))

    def test_validate_storage_with_head_no_orphaned_blocks(self):
        """Basic Validate Storage Test

            Note: "orphaned" data is only what was deleted
                  this is just due to how the test is structured.
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        def storage_head_callback(request, uri, headers):
            return self.storage_block_head_success(request,
                                                   uri,
                                                   headers)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(0,
                         len(self.manager.storage.orphaned))

    def test_validate_storage_with_head_with_orphaned_blocks(self):
        """Basic Validate Storage Test

            Note: "orphaned" data is only what was deleted
                  this is just due to how the test is structured.
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        def storage_head_callback(request, uri, headers):
            return self.storage_block_head_success(request,
                                                   uri,
                                                   headers)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        self.generate_orphaned_blocks(self.orphaned_count)

        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(self.orphaned_count,
                         len(self.manager.storage.orphaned))

    def test_validate_storage_with_head_existing_orphaned_list(self):
        """Basic Validate Storage Test

            Note: "orphaned" data is only what was deleted
                  this is just due to how the test is structured.
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        def storage_head_callback(request, uri, headers):
            return self.storage_block_head_success(request,
                                                   uri,
                                                   headers)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        self.assertIsNone(self.manager.storage.orphaned)
        self.manager.storage.orphaned = []
        self.assertIsNotNone(self.manager.storage.orphaned)
        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(0,
                         len(self.manager.storage.orphaned))

    def test_validate_storage_with_head_existing_storage_list(self):
        """Basic Validate Storage Test

            Note: "orphaned" data is only what was deleted
                  this is just due to how the test is structured.
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        def storage_head_callback(request, uri, headers):
            return self.storage_block_head_success(request,
                                                   uri,
                                                   headers)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        self.client.get_storage_list()
        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(0,
                         len(self.manager.storage.orphaned))

    def test_validate_storage_with_head_no_storage_blocks(self):
        """Basic Validate Storage Test

            Note: "orphaned" data is only what was deleted
                  this is just due to how the test is structured.
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def storage_listing_callback(request, uri, headers):
            return (200, headers, json.dumps([]))

        def storage_head_callback(request, uri, headers):
            return self.storage_block_head_success(request,
                                                   uri,
                                                   headers)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(0,
                         len(self.manager.storage.orphaned))

    def test_validate_storage_with_head_with_orphaned_blocks(self):
        """Basic Validate Storage Test

            Note: "orphaned" data is only what was deleted
                  this is just due to how the test is structured.
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        def storage_head_callback(request, uri, headers):
            return self.storage_block_head_success(request,
                                                   uri,
                                                   headers)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        self.generate_orphaned_blocks(self.orphaned_count)

        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(self.orphaned_count,
                         len(self.manager.storage.orphaned))

    def test_validate_storage_with_head_with_zero_block_length(self):
        """Basic Validate Storage Test

            Note: "orphaned" data is only what was deleted
                  this is just due to how the test is structured.
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def metadata_listing_callback(request, uri, headers):
            return self.metadata_block_listing_success(request,
                                                       uri,
                                                       headers)

        def metadata_head_callback(request, uri, headers):
            return self.metadata_block_head_success(request,
                                                    uri,
                                                    headers)

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        def storage_head_callback(request, uri, headers):
            return self.storage_block_head_success(request,
                                                   uri,
                                                   headers)

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        self.generate_orphaned_blocks(self.orphaned_count)

        self.generate_null_block(block_in_metadata=True,
                                 orphaned_count=self.orphaned_count)

        self.client.get_block_list()
        for block_id in self.vault.blocks:
            self.client.deuceclient.HeadBlock(self.vault,
                                              self.vault.blocks[block_id])
        self.client.build_cross_references()

        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual((self.orphaned_count * 2),
                         len(self.manager.storage.orphaned))

    def test_validate_storage_with_head_with_zero_block_length_second(self):
        """Basic Validate Storage Test

            Note: "orphaned" data is only what was deleted
                  this is just due to how the test is structured.
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def metadata_listing_callback(request, uri, headers):
            return self.metadata_block_listing_success(request,
                                                       uri,
                                                       headers)

        def metadata_head_callback(request, uri, headers):
            return self.metadata_block_head_success(request,
                                                    uri,
                                                    headers)

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        def storage_head_callback(request, uri, headers):
            return self.storage_block_head_success(request,
                                                   uri,
                                                   headers)

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        self.generate_orphaned_blocks(self.orphaned_count)
        self.generate_null_block(block_in_metadata=False,
                                 orphaned_count=self.orphaned_count)

        self.client.get_block_list()
        for block_id in self.vault.blocks:
            self.client.deuceclient.HeadBlock(self.vault,
                                              self.vault.blocks[block_id])
        self.client.build_cross_references()

        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual((self.orphaned_count * 2),
                         len(self.manager.storage.orphaned))
