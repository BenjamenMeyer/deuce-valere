"""
Deuce Valere - Tests - Functions - Vault Cleanup
"""
import ddt
from deuceclient.tests import *
import httpretty

from deucevalere import vault_validate, vault_cleanup
from deucevalere.tests import *
from deucevalere.tests.client_base import TestValereClientBase
from deucevalere.tests.client_base import calculate_ref_modified


@httpretty.activate
class TestConvenienceFunctions(TestValereClientBase):

    def setUp(self):
        super().setUp()
        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.generate_blocks(count=20)
        self.generate_orphaned_blocks(count=10)
        self.secondary_setup(manager_start=None,
                             manager_end=None)

    def tearDown(self):
        super().tearDown()

    def test_cleanup_no_expired_blocks(self):
        with self.assertRaises(RuntimeError):
            vault_cleanup(self.deuce_client,
                          self.vault,
                          self.manager)

    def test_cleanup_no_orphaned_blocks(self):
        self.manager.metadata.expired = []
        with self.assertRaises(RuntimeError):
            vault_cleanup(self.deuce_client,
                          self.vault,
                          self.manager)

    def test_cleanup(self):

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

        def storage_delete_callback(request, uri, headers):
            return (204, headers, '')

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               url,
                               body=metadata_delete_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_delete_callback)

        self.assertEqual(vault_validate(self.deuce_client,
                                        self.vault,
                                        self.manager),
                         0)

        self.assertEqual(vault_cleanup(self.deuce_client,
                                       self.vault,
                                       self.manager),
                         0)

    def test_cleanup_metadata_cleanup_error(self):

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

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        def storage_delete_callback(request, uri, headers):
            return (204, headers, '')

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               url,
                               body=metadata_delete_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_delete_callback)

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

        self.assertEqual(vault_validate(self.deuce_client,
                                        self.vault,
                                        self.manager),
                         0)

        self.assertEqual(vault_cleanup(self.deuce_client,
                                       self.vault,
                                       self.manager),
                         1)

    def test_cleanup_storage_cleanup_error(self):

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

        def storage_delete_callback(request, uri, headers):
            return (404, headers, 'mock failure')

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               url,
                               body=metadata_delete_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_delete_callback)

        self.assertEqual(vault_validate(self.deuce_client,
                                        self.vault,
                                        self.manager),
                         0)

        self.assertEqual(vault_cleanup(self.deuce_client,
                                       self.vault,
                                       self.manager),
                         2)

    def test_cleanup_metadata_and_storage_cleanup_errors(self):

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

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        def storage_delete_callback(request, uri, headers):
            return (404, headers, 'mock failure')

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.HEAD,
                               self.get_metadata_block_pattern_matcher(),
                               body=metadata_head_callback)

        httpretty.register_uri(httpretty.DELETE,
                               url,
                               body=metadata_delete_callback)

        surl = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               surl,
                               body=storage_listing_callback)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_storage_block_pattern_matcher(),
                               body=storage_delete_callback)

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

        self.assertEqual(vault_validate(self.deuce_client,
                                        self.vault,
                                        self.manager),
                         0)

        self.assertEqual(vault_cleanup(self.deuce_client,
                                       self.vault,
                                       self.manager),
                         3)
