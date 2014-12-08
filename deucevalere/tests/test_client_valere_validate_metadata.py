"""
Deuce Valere - Tests - Client - Valere - Validate Metadata
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
class TestValereClientValidateMetadata(TestValereClientBase):

    def setUp(self):
        super().setUp()
        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.generate_blocks(count=20)

    def tearDown(self):
        super().tearDown()

    def test_validate_metadata_no_blocks(self):
        self.secondary_setup(manager_start=None,
                             manager_end=None)

        def metadata_listing_callback(request, uri, headers):
            return (200, headers, json.dumps([]))

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        self.client.validate_metadata()
        self.assertEqual(len(self.manager.metadata.current), 0)
        self.assertEqual(len(self.manager.metadata.expired), 0)
        self.assertIsNone(self.manager.metadata.deleted, None)

    def test_validate_metadata_no_expiration_existing_current(self):
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

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        self.client.get_block_list()

        self.client.validate_metadata()
        self.assertEqual(len(self.manager.metadata.current),
                         len(self.meta_data))
        self.assertEqual(len(self.manager.metadata.expired), 0)
        self.assertIsNone(self.manager.metadata.deleted, None)

    def test_validate_metadata_no_expiration_existing_expired(self):
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

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        self.manager.metadata.expired = []
        self.client.validate_metadata()
        self.assertEqual(len(self.manager.metadata.current),
                         len(self.meta_data))
        self.assertEqual(len(self.manager.metadata.expired), 0)
        self.assertIsNone(self.manager.metadata.deleted, None)

    def test_validate_metadata_no_expiration_exception_on_block_heading(self):
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

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        with mock.patch(
                'deuceclient.client.deuce.DeuceClient.HeadBlock') as mock_dc:
            mock_dc.side_effect = RuntimeError('mock exception')

            self.client.validate_metadata()

    def test_validate_metadata_no_expiration(self):
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

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        self.client.validate_metadata()
        self.assertEqual(len(self.manager.metadata.current),
                         len(self.meta_data))
        self.assertEqual(len(self.manager.metadata.expired), 0)
        self.assertIsNone(self.manager.metadata.deleted, None)

    def test_validate_metadata_no_expiration_some_zero_refs(self):
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

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        def __min_max(a, b):
            return max(min(a, b), b)

        key_set = sorted(
            list(self.meta_data.keys()))[0:__min_max(len(self.meta_data), 10)]
        for key in key_set:
            print('Setting Block {0:} to have ZERO (0) Ref Count'.format(key))
            self.meta_data[key].ref_count = 0

        self.client.validate_metadata()
        self.assertEqual(len(self.manager.metadata.current),
                         len(self.meta_data))
        self.assertEqual(len(self.manager.metadata.expired), 0)
        self.assertIsNone(self.manager.metadata.deleted, None)

    def test_validate_metadata_expiration_1min_some_zero_refs(self):
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

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        def __min_max(a, b):
            return max(min(a, b), b)

        base_age_date = datetime.datetime.utcnow()

        key_set = sorted(
            list(self.meta_data.keys()))[0:__min_max(len(self.meta_data), 10)]
        for key in key_set:
            self.meta_data[key].ref_count = 0
            self.meta_data[key].ref_modified = TestValereClientBase.\
                calculate_ref_modified(base=base_age_date,
                                       days=0, hours=0, mins=1, secs=0)

        self.manager.expire_age = datetime.timedelta(minutes=1)

        check_count = 0
        for key, block in self.meta_data.items():
            check_delta = base_age_date - datetime.datetime.utcfromtimestamp(
                block.ref_modified)
            if check_delta > self.manager.expire_age and block.ref_count == 0:
                check_count = check_count + 1
                print('Block {0:} has expired age of {1}'
                      .format(block.block_id,
                              check_delta))

        self.client.validate_metadata()
        self.assertEqual(len(self.manager.metadata.current),
                         len(self.meta_data))
        self.assertEqual(len(self.manager.metadata.expired), check_count)
        self.assertIsNone(self.manager.metadata.deleted, None)

    def test_validate_metadata_expiration_existing_in_expirations(self):
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

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        def __min_max(a, b):
            return max(min(a, b), b)

        base_age_date = datetime.datetime.utcnow()

        key_set = sorted(
            list(self.meta_data.keys()))[0:__min_max(len(self.meta_data), 10)]
        for key in key_set:
            self.meta_data[key].ref_count = 0
            self.meta_data[key].ref_modified = TestValereClientBase.\
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
                print('Block {0:} has expired age of {1}'
                      .format(block.block_id,
                              check_delta))

        self.client.validate_metadata()
        self.assertEqual(len(self.manager.metadata.current),
                         len(self.meta_data))
        self.assertEqual(len(self.manager.metadata.expired), check_count)
        self.assertIsNone(self.manager.metadata.deleted, None)
