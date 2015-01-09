"""
Deuce Valere - Tests - Function - Vault Reload
"""
from deuceclient.api import Vault
from deuceclient.client.deuce import DeuceClient
from deuceclient.tests import *
import httpretty

from deucevalere import vault_reload
from deucevalere.api.system import Manager
from deucevalere.tests import *
from deucevalere.tests.client_base import TestValereClientBase


@httpretty.activate
class TestConvenienceFunctionReload(TestValereClientBase):

    def setUp(self):
        super().setUp()
        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.generate_blocks(count=20)
        self.generate_orphaned_blocks(count=10)
        self.secondary_setup(manager_start=None,
                             manager_end=None)
        self.vault = Vault(self.project_id,
                           self.vault_id)
        self.deuce_client = DeuceClient(self.authenticator,
                                        self.apihost,
                                        True)
        self.manager = Manager()

    def tearDown(self):
        super().tearDown()

    def test_vault_reload(self):

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

        vault_reload(self.deuce_client,
                     self.vault,
                     self.manager)

    def test_vault_reload_fail_get_metadata_block_list(self):

        def metadata_listing_callback(request, uri, headers):
            return (404, headers, 'mock failure')

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

        with self.assertRaises(RuntimeError):
            vault_reload(self.deuce_client,
                         self.vault,
                     self.manager)

    def test_vault_reload_fail_get_storage_block_list(self):

        def metadata_listing_callback(request, uri, headers):
            return self.metadata_block_listing_success(request,
                                                       uri,
                                                       headers)

        def metadata_head_callback(request, uri, headers):
            return self.metadata_block_head_success(request,
                                                    uri,
                                                    headers)

        def storage_listing_callback(request, uri, headers):
            return (404, headers, 'mock failure')

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

        with self.assertRaises(RuntimeError):
            vault_reload(self.deuce_client,
                         self.vault,
                     self.manager)
