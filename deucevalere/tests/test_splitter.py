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
import mock


@ddt.ddt
@httpretty.activate
class TestValereSplitter(TestValereClientBase):

    def setUp(self):
        super().setUp()

        self.secondary_setup(manager_start=None,
                             manager_end=None)

    def tearDown(self):
        super().tearDown()

    def findmaxindex(self, markers, value):
        # NOTE(TheSriram): This will help us find the last index
        # of any element in a list, even if the element is repeated.
        # As is the case with orphaned blocks in storage
        return max([index for index, element in enumerate(markers)
                    if element == value])

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

        with mock.patch.object(splitter_client.deuceclient, 'GetBlockList',
                               side_effect=RuntimeError):
            with self.assertRaises(MetaChunkError):
                for _ in splitter_client.meta_chunker(limit=self.limit):
                    pass

    def test_valere_meta_splitter_storage_chunker_exception(self):

        self.limit = random.randrange(1, 10)

        def metadata_listing_callback(request, uri, headers):
            print(uri)
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

        with mock.patch.object(splitter_client.deuceclient,
                               'GetBlockStorageList',
                               side_effect=RuntimeError):
            with self.assertRaises(StorageChunkError):
                for _ in splitter_client.meta_chunker(limit=self.limit):
                    pass

    @ddt.data(True, False)
    def test_valere_meta_splitter_orphaned_storage(self, exactly_divisible):

        if exactly_divisible:
            self.limit = random.randrange(1, 10)
            self.num_blocks = self.limit * random.randrange(10, 100)

        else:
            self.limit = random.randrange(2, 10)
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

        sorted_markers = sorted(list([marker.split('_')[0]
                  for marker in self.storage_data.keys()]))

        chunks = splitter_client.meta_chunker(limit=self.limit)

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

            # NOTE(TheSriram): The tests cannot check for exact matches since
            # the specified limit might not be same as the number of orphaned
            # blocks found. Therefore what can be verified is that the marker
            # indeed progresses though the list.

            self.assertEqual(self.vault.project_id, project_id)
            self.assertEqual(self.vault.vault_id, vaultid)
            if end_marker:
                self.assertGreater(sorted_markers.index(end_marker),
                                   sorted_markers.index(start_marker))

            if not end_marker:
                end_index = self.findmaxindex(sorted_markers, start_marker)
                self.assertGreaterEqual(end_index + self.limit,
                                        len(sorted_markers))
