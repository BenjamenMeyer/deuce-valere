"""
Deuce Valere - Tests - Splitter - Valere
"""
import logging

from deucevalere.tests.client_base import TestValereClientBase
from deucevalere.splitter.meta_splitter import ValereSplitter
from deucevalere.splitter.meta_splitter import ChunkError
from deuceclient.tests import *

import mock
import httpretty


@httpretty.activate
class TestValereSplitter(TestValereClientBase):

    def setUp(self):
        super().setUp()

        self.secondary_setup(manager_start=None,
                             manager_end=None)
        self.num_blocks = 25
        self.limit = 4

    def tearDown(self):
        super().tearDown()

    def test_valere_meta_splitter(self):

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

        for chunk in chunks:
            self.assertIn(self.vault.vault_id, chunk)
            self.assertIn(self.vault.project_id, chunk)
            self.assertIn(chunk[2], self.meta_data.keys())
            try:
                self.assertIn(chunk[3], self.meta_data.keys())
            except IndexError:
                # NOTE(TheSriram): The last batch will not have an end marker,
                # since there was no x-next-batch from the listing of metadata
                # blocks
                pass

    def test_valere_meta_splitter_exception(self):
        splitter_client = ValereSplitter(self.deuce_client, self.vault)
        with mock.patch.object(self.deuce_client, 'GetBlockList',
                               side_effect=RuntimeError):
            with self.assertRaises(ChunkError):
                splitter_client.meta_chunker(limit=self.limit)
