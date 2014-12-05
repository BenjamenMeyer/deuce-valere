"""
Deuce Valere - Tests - Client - Valere - Cleanup Expired
"""
import functools
import json
import mock

from deuceclient.tests import *
import httpretty
import httpretty.compat

from deucevalere.tests import *
from deucevalere.tests.client_base import TestValereClientBase
from deucevalere.tests.client_base import calculate_ref_modified


@httpretty.activate
class TestValereClientCleanupExpired(TestValereClientBase):

    def setUp(self):
        super().setUp()
        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.generate_blocks(count=20)

    def tearDown(self):
        super().tearDown()

    def test_cleanup_expired_no_expired_list(self):
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        with self.assertRaises(RuntimeError):
            self.client.cleanup_expired_blocks()

    def test_cleanup_expired_empty_list_provided_deleted(self):
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        self.manager.metadata.expired = []
        self.manager.metadata.deleted = []

        self.client.cleanup_expired_blocks()

    def test_cleanup_expired_operation(self):
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

        base_age_date = datetime.datetime.utcnow()

        key_set = sorted(
            list(self.meta_data.keys()))[0:minmax(len(self.meta_data), 10)]
        for key in key_set:
            self.meta_data[key].ref_count = 0
            self.meta_data[key].ref_modified = \
                calculate_ref_modified(base=base_age_date,
                                       days=0, hours=0, mins=1, secs=0)

        self.manager.metadata.expired = []
        for key in key_set[:int(len(key_set) / 2)]:
            self.manager.metadata.expired.append(key)

        self.manager.expire_age = datetime.timedelta(minutes=1)

        check_count = 0
        for key, block in self.meta_data.items():
            check_delta = base_age_date - datetime.datetime.utcfromtimestamp(
                block.ref_modified)
            if check_delta > self.manager.expire_age and block.ref_count == 0:
                check_count = check_count + 1

        self.client.validate_metadata()
        self.assertEqual(len(self.manager.metadata.expired), check_count)
        self.assertIsNone(self.manager.metadata.deleted, None)

        self.client.cleanup_expired_blocks()
        self.assertEqual(len(self.manager.metadata.deleted), check_count)

    def test_cleanup_expired_operation_with_existing_deleted(self):
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

        base_age_date = datetime.datetime.utcnow()

        self.manager.metadata.deleted = []

        key_set = sorted(
            list(self.meta_data.keys()))[0:minmax(len(self.meta_data), 10)]
        for key in key_set:
            self.meta_data[key].ref_count = 0
            self.meta_data[key].ref_modified = \
                calculate_ref_modified(base=base_age_date,
                                       days=0, hours=0, mins=1, secs=0)
            self.manager.metadata.deleted.append(key)

        self.manager.metadata.expired = []
        for key in key_set[:int(len(key_set) / 2)]:
            self.manager.metadata.expired.append(key)

        self.manager.expire_age = datetime.timedelta(minutes=1)

        check_count = 0
        for key, block in self.meta_data.items():
            check_delta = base_age_date - datetime.datetime.utcfromtimestamp(
                block.ref_modified)
            if check_delta > self.manager.expire_age and block.ref_count == 0:
                check_count = check_count + 1

        self.client.validate_metadata()
        self.assertEqual(len(self.manager.metadata.expired), check_count)

        self.client.cleanup_expired_blocks()
        self.assertEqual(len(self.manager.metadata.deleted), check_count)

    def test_cleanup_expired_operation_deletion_failed(self):
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
            return (404, headers, 'mock failure')

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

        base_age_date = datetime.datetime.utcnow()

        key_set = sorted(
            list(self.meta_data.keys()))[0:minmax(len(self.meta_data), 10)]
        for key in key_set:
            self.meta_data[key].ref_count = 0
            self.meta_data[key].ref_modified = \
                calculate_ref_modified(base=base_age_date,
                                       days=0, hours=0, mins=1, secs=0)

        self.manager.metadata.expired = []
        for key in key_set[:int(len(key_set) / 2)]:
            self.manager.metadata.expired.append(key)

        self.manager.expire_age = datetime.timedelta(minutes=1)

        check_count = 0
        for key, block in self.meta_data.items():
            check_delta = base_age_date - datetime.datetime.utcfromtimestamp(
                block.ref_modified)
            if check_delta > self.manager.expire_age and block.ref_count == 0:
                check_count = check_count + 1

        self.client.validate_metadata()
        self.assertEqual(len(self.manager.metadata.expired), check_count)
        self.assertIsNone(self.manager.metadata.deleted, None)

        self.client.cleanup_expired_blocks()
