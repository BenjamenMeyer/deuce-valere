"""
Deuce Valere - Tests - Splitter - Valere
"""
import ddt
import json
import random

from deucevalere.tests.client_base import TestValereClientBase
from deucevalere.splitter.meta_splitter import ValereSplitter
from deucevalere.splitter.meta_splitter import MetaChunkError
from deucevalere.splitter.meta_splitter import StorageChunkError
from deuceclient.tests import *

import httpretty


@ddt.ddt
@httpretty.activate
class TestValereSplitter(TestValereClientBase):

    def setUp(self):
        super().setUp()

        self.secondary_setup(manager_start=None,
                             manager_end=None)

    def tearDown(self):
        super().tearDown()

    def test_valere_meta_splitter(self):

        self.num_blocks = random.randrange(10, 100)
        self.limit = random.randrange(1, 10)
        self.generate_blocks(self.num_blocks)

        def metadata_listing_callback(request, uri, headers):
            return self.metadata_block_listing_success(request,
                                                       uri,
                                                       headers)

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        splitter_client = ValereSplitter(self.deuce_client, self.vault)
        chunks = splitter_client.meta_chunker(limit=self.limit)

        sorted_metadata = sorted(self.meta_data.keys())
        index = 0

        for chunk in chunks:

            try:
                project_id, vaultid, start_marker, end_marker = chunk
            except ValueError:
                # NOTE(TheSriram): The last batch will not have an end marker,
                # since there was no x-next-batch from the listing of metadata
                # blocks, which in turn will cause a ValueError when unpacking
                # tuples
                project_id, vaultid, start_marker = chunk
                end_marker = None

            self.assertEqual(self.vault.project_id, project_id)
            self.assertEqual(self.vault.vault_id, vaultid)
            self.assertEqual(start_marker, sorted_metadata[index])

            if end_marker:
                index += self.limit
                self.assertEqual(end_marker, sorted_metadata[index])

    def test_valere_meta_splitter_empty(self):

        self.limit = random.randrange(1, 10)
        self.generate_blocks(0)

        def metadata_listing_callback(request, uri, headers):
            return self.metadata_block_listing_success(request,
                                                       uri,
                                                       headers)

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        meta_blocks_url = get_blocks_url(self.apihost, self.vault.vault_id)
        storage_blocks_url = get_storage_blocks_url(self.apihost,
                                             self.vault.vault_id)

        httpretty.register_uri(httpretty.GET,
                               meta_blocks_url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.GET,
                               storage_blocks_url,
                               body=storage_listing_callback)

        splitter_client = ValereSplitter(self.deuce_client, self.vault)

        chunks = splitter_client.meta_chunker(limit=self.limit)

        for chunk in chunks:

            project_id, vaultid, start_marker, end_marker = chunk

            self.assertEqual(self.vault.project_id, project_id)
            self.assertEqual(self.vault.vault_id, vaultid)
            self.assertIsNone(start_marker)
            self.assertIsNone(end_marker)

    def test_valere_meta_splitter_meta_chunker_exception(self):

        self.limit = random.randrange(1, 10)

        def metadata_listing_callback(request, uri, headers):
            return (404, headers, 'mock failure')

        url = get_blocks_url(self.apihost, self.vault.vault_id)

        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        splitter_client = ValereSplitter(self.deuce_client, self.vault)
        with self.assertRaises(MetaChunkError):
            splitter_client.meta_chunker(limit=self.limit)

    def test_valere_meta_splitter_storage_chunker_exception(self):

        self.limit = random.randrange(1, 10)

        def metadata_listing_callback(request, uri, headers):
            return (200, headers, json.dumps([]))

        def storage_listing_callback(request, uri, headers):
            return (404, headers, 'mock failure')

        meta_blocks_url = get_blocks_url(self.apihost, self.vault.vault_id)
        storage_blocks_url = get_storage_blocks_url(self.apihost,
                                             self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               meta_blocks_url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.GET,
                               storage_blocks_url,
                               body=storage_listing_callback)

        splitter_client = ValereSplitter(self.deuce_client, self.vault)
        with self.assertRaises(StorageChunkError):
            splitter_client.meta_chunker(limit=self.limit)

    @ddt.data(True, False)
    def test_valere_meta_splitter_orphaned_storage(self, exactly_divisible):

        if exactly_divisible:
            self.limit = random.randrange(1, 10)
            self.num_blocks = self.limit * random.randrange(10, 100)

        else:
            self.limit = random.randrange(10, 100)
            self.num_blocks = (self.limit * random.randrange(10, 100)) + \
                random.randrange(1, self.limit)

        self.generate_blocks(self.num_blocks)
        self.generate_orphaned_blocks(self.num_blocks +
                                      random.randrange(10, 100))

        def metadata_listing_callback(request, uri, headers):
            return (200, headers, json.dumps([]))

        def storage_listing_callback(request, uri, headers):
            return self.storage_block_listing_success(request,
                                                      uri,
                                                      headers)

        meta_blocks_url = get_blocks_url(self.apihost, self.vault.vault_id)
        storage_blocks_url = get_storage_blocks_url(self.apihost,
                                             self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               meta_blocks_url,
                               body=metadata_listing_callback)

        httpretty.register_uri(httpretty.GET,
                               storage_blocks_url,
                               body=storage_listing_callback)

        splitter_client = ValereSplitter(self.deuce_client, self.vault)

        un_orphaned = []

        # NOTE(TheSriram): Let's make sure that we are dealing only with
        # orphaned blocks

        for key in self.storage_data.keys():
            if not self.storage_data[key].block_orphaned:
                un_orphaned.append(key)

        for key in un_orphaned:
            del self.storage_data[key]

        for key in self.storage_data.keys():
            self.assertEqual(self.storage_data[key].block_orphaned, True)

        sorted_markers = sorted(list(set([marker.split('_')[0]
                                   for marker in self.storage_data.keys()])))

        chunks = splitter_client.meta_chunker(limit=self.limit)

        index = 0
        for chunk in chunks:
            try:
                project_id, vaultid, start_marker, end_marker = chunk
            except ValueError:
                # NOTE(TheSriram): The last batch will not have an end marker,
                # since there was no x-next-batch from the listing of storage
                # blocks, which in turn will cause a ValueError when unpacking
                # tuples
                project_id, vaultid, start_marker = chunk
                end_marker = None

            self.assertEqual(self.vault.project_id, project_id)
            self.assertEqual(self.vault.vault_id, vaultid)
            try:
                self.assertEqual(start_marker, sorted_markers[index])
            except IndexError:
                self.assertEqual(start_marker, sorted_markers[-1:][0])

            if end_marker:
                index += self.limit
                try:
                    self.assertEqual(end_marker, sorted_markers[index])
                except IndexError:
                    self.assertEqual(end_marker, sorted_markers[-1:][0])
