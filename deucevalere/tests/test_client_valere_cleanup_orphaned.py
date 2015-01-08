"""
Deuce Valere - Tests - Client - Valere - Cleanup Orphaned
"""
import functools
import json
import mock

from deuceclient.tests import *
import httpretty

from deucevalere.tests import *
from deucevalere.tests.client_base import TestValereClientBase
from deucevalere.tests.client_base import calculate_ref_modified


@httpretty.activate
class TestValereClientCleanupOrphaned(TestValereClientBase):

    def setUp(self):
        super().setUp()
        self.count = 20
        self.orphaned_count = 15

        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.generate_blocks(count=20)

    def tearDown(self):
        super().tearDown()

    def test_cleanup_failure(self):
        """Most basic failure case - no data
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)
        with self.assertRaises(RuntimeError):
            self.client.cleanup_storage()

    def test_cleanup_orphaned(self):
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

        def storage_delete_callback(request, uri, headers):
            return (204, headers, '')

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_delete_callback)

        self.generate_orphaned_blocks(self.orphaned_count)

        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)
        self.assertIsNone(self.manager.storage.deleted)

        self.client.cleanup_storage()
        self.assertIsNotNone(self.manager.storage.deleted)
        self.assertEqual(self.orphaned_count,
                         len(self.manager.storage.deleted))

    def test_cleanup_orphaned_with_some_not_in_metadata(self):
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

        def storage_delete_callback(request, uri, headers):
            return (204, headers, '')

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_delete_callback)

        self.generate_orphaned_blocks(self.orphaned_count)
        self.generate_null_block(block_in_metadata=False,
                                 orphaned_count=self.orphaned_count)

        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)
        self.assertIsNone(self.manager.storage.deleted)

        self.client.cleanup_storage()
        self.assertIsNotNone(self.manager.storage.deleted)
        self.assertEqual((self.orphaned_count * 2),
                         len(self.manager.storage.deleted))

    def test_cleanup_orphaned_delete_failed(self):
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

        def storage_delete_callback(request, uri, headers):
            return (404, headers, 'mock failure')

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_delete_callback)

        self.generate_orphaned_blocks(self.orphaned_count)

        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)
        self.assertIsNone(self.manager.storage.deleted)

        self.client.cleanup_storage()
        self.assertIsNotNone(self.manager.storage.deleted)
        self.assertEqual(0,
                         len(self.manager.storage.deleted))

    def test_cleanup_orphaned_with_existing_deleted(self):
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

        def storage_delete_callback(request, uri, headers):
            return (204, headers, '')

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_delete_callback)

        self.generate_orphaned_blocks(self.orphaned_count)

        self.client.validate_storage_with_head()
        self.assertIsInstance(self.manager.storage.orphaned, list)
        self.assertIsNone(self.manager.storage.deleted)
        self.assertNotEqual(0, self.manager.storage.orphaned)
        self.manager.storage.deleted = [block_id
            for block_id in self.manager.storage.orphaned]

        self.assertEqual(len(self.manager.storage.orphaned),
                         self.orphaned_count)

        self.client.cleanup_storage()
        self.assertIsNotNone(self.manager.storage.deleted)
        # All blocks were
        self.assertEqual(self.orphaned_count,
                         len(self.manager.storage.deleted))
