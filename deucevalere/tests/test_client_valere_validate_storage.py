"""
Deuce Valere - Tests - Client - Valere - Validate Storage
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
class TestValereClientValidateStorage(TestValereClientBase):

    def setUp(self):
        super().setUp()
        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.generate_blocks(count=20)

    def tearDown(self):
        super().tearDown()

    def test_validate_storage(self):
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

        def metadata_delete_callback(request, uri, headers):
            return (204, headers, '')

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_delete_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        self.guarantee_expired(expired_count=10,
                               expired_age=datetime.timedelta(minutes=1))

        self.client.validate_metadata()
        self.client.cleanup_expired_blocks()
        self.client.build_cross_references()
        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(len(self.manager.metadata.deleted),
                         len(self.manager.storage.orphaned))

    def test_validate_storage_existing_storage_list(self):
        """Test with an existing list of storage blocks

            Note: "orphaned" data is only what was deleted
                  this is just due to how the test is structured
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

        def metadata_delete_callback(request, uri, headers):
            return (204, headers, '')

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_delete_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        self.guarantee_expired(expired_count=10,
                               expired_age=datetime.timedelta(minutes=1))

        self.client.validate_metadata()
        self.client.cleanup_expired_blocks()
        self.client.build_cross_references()
        self.client.get_storage_list()

        self.assertIsNotNone(self.manager.storage.current)
        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(len(self.manager.metadata.deleted),
                         len(self.manager.storage.orphaned))

    def test_validate_storage_no_orphans_no_storage_data(self):
        """Test with no storage blocks available
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

        def metadata_delete_callback(request, uri, headers):
            return (204, headers, '')

        def storage_listing_callback(request, uri, headers):
            return (200, headers, json.dumps([]))

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_delete_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        self.guarantee_expired(expired_count=10,
                               expired_age=datetime.timedelta(minutes=1))

        self.client.validate_metadata()
        self.client.cleanup_expired_blocks()
        self.client.build_cross_references()
        self.client.get_storage_list()

        # This is the point of this test:
        self.assertIsNotNone(self.manager.storage.current)
        self.assertIsNone(self.manager.storage.orphaned)
        self.manager.storage.orphaned = []
        self.assertIsNotNone(self.manager.storage.orphaned)

        self.client.validate_storage()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(0,
                         len(self.manager.storage.orphaned))

    def test_validate_storage_no_cross_references(self):
        """Test with no metadata blocks available

            Note: This test essentially makes all blocks in
                  storage be detected as orphaned blocks
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def metadata_listing_callback(request, uri, headers):
            return (200, headers, json.dumps([]))

        def metadata_head_callback(request, uri, headers):
            return self.metadata_block_head_success(request,
                                                    uri,
                                                    headers)

        def metadata_delete_callback(request, uri, headers):
            return (204, headers, '')

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_delete_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        self.guarantee_expired(expired_count=10,
                               expired_age=datetime.timedelta(minutes=1))

        # Note: this will have zero cross references because there are no
        # blocks
        self.client.get_block_list()
        self.client.build_cross_references()
        self.client.get_storage_list()

        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(len(self.meta_data),
                         len(self.manager.storage.orphaned))

    def test_validate_storage_no_overlap(self):
        """Test with no metadata blocks available

            Note: This test essentially makes all blocks in
                  storage be detected as orphaned blocks
        """
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def metadata_listing_callback(request, uri, headers):
            return (200, headers, json.dumps([]))

        def metadata_head_callback(request, uri, headers):
            return self.metadata_block_head_success(request,
                                                    uri,
                                                    headers)

        def metadata_delete_callback(request, uri, headers):
            return (204, headers, '')

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_delete_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        self.guarantee_expired(expired_count=10,
                               expired_age=datetime.timedelta(minutes=1))

        fully_orphaned_blocks = self.generate_orphaned_blocks_no_metadata(
            count=10)

        # Note: this will have zero cross references because there are no
        # blocks
        self.client.get_block_list()
        self.client.build_cross_references()
        self.client.get_storage_list()

        for storage_id in fully_orphaned_blocks:
            self.vault.storageblocks[storage_id].set_block_size(
                len(self.storage_data[storage_id]))

        self.assertIsNone(self.manager.storage.orphaned)
        self.client.validate_storage()
        self.assertIsInstance(self.manager.storage.orphaned, list)

        self.assertEqual(len(self.storage_data),
                         len(self.manager.storage.orphaned))
