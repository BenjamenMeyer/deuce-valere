"""
Deuce Valere - Tests - Splitter - Valere
"""
import random

from deucevalere.tests.client_base import TestValereClientBase
from deucevalere.splitter.meta_splitter import ValereSplitter
from deucevalere.splitter.meta_splitter import ChunkError
from deuceclient.tests import *

import httpretty


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

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        splitter_client = ValereSplitter(self.deuce_client, self.vault)

        chunks = splitter_client.meta_chunker(limit=self.limit)


        for chunk in chunks:

            project_id, vaultid, start_marker, end_marker = chunk

            self.assertEqual(self.vault.project_id, project_id)
            self.assertEqual(self.vault.vault_id, vaultid)
            self.assertEqual(start_marker, None)
            self.assertEqual(end_marker, None)


    def test_valere_meta_splitter_exception(self):

        self.limit = random.randrange(1, 10)

        def metadata_listing_callback(request, uri, headers):
            return (404, headers, 'mock failure')

        url = get_blocks_url(self.apihost, self.vault.vault_id)

        httpretty.register_uri(httpretty.GET,
                               url,
                               body=metadata_listing_callback)

        splitter_client = ValereSplitter(self.deuce_client, self.vault)
        with self.assertRaises(ChunkError):
            splitter_client.meta_chunker(limit=self.limit)
