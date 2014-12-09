"""
Deuce Valere - Tests - Client - Valere - Validate Storage With HEAD
"""
import functools
import json
import mock

from deuceclient.tests import *
import httpretty
import httpretty.compat

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
