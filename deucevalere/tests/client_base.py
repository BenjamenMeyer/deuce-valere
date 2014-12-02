"""
Deuce Valere - Client - Base Test Functionality
"""
import json
import re
import unittest
import urllib.parse
import uuid

from deuceclient.api import Vault, Block
from deuceclient.client.deuce import DeuceClient
from deuceclient.common.validation import METADATA_BLOCK_ID_REGEX
from deuceclient.common.validation import STORAGE_BLOCK_ID_REGEX
from deuceclient.tests import *
import httpretty

from deucevalere.api.system import Manager
from deucevalere.client.valere import ValereClient
from deucevalere.tests import *


class TestValereClientBase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.meta_data = None
        self.storage_data = None
        self.data_splitter = 3

    def tearDown(self):
        super().tearDown()

    def get_metadata_block_pattern_matcher(self):
        base_url = get_blocks_url(self.apihost, self.vault.vault_id)
        regex = '^{0:}/{1:}'.format(base_url,
                                    METADATA_BLOCK_ID_REGEX.pattern[2:-2])
        return re.compile(regex)

    def get_storage_block_pattern_matcher(self):
        base_url = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        regex = '^{0:}/{1:}'.format(base_url,
                                    STORAGE_BLOCK_ID_REGEX.pattern[2:-2])
        return re.compile(regex)

    def metadata_calculate_position(self, splitter=None):
        if splitter is None:
            splitter = self.data_splitter

        return int(len(self.meta_data) / splitter)

    def storage_calculate_position(self, splitter=None):
        if splitter is None:
            splitter = self.data_splitter

        return int(len(self.storage_data) / splitter)

    def generate_blocks(self, count):
        # Generate a list of metadata block ids - 100 blocks
        # self.meta_data = sorted(
        #     [block[0] for block in create_blocks(block_count=count)]
        # )

        def generate_ref_modified():
            pass

        self.meta_data = {block[0]: Block(self.project_id,
                                          self.vault_id,
                                          block_id=block[0],
                                          data=block[1],
                                          block_size=block[2],
                                          ref_count=random.randint(0, 4),
                                          block_orphaned=False,
                                          ref_modified=generate_ref_modified()
                                          )
            for block in create_blocks(block_count=count)}

        # Generate a list of equivalent storage blocks
        self.storage_data = {}
        for block_id in self.meta_data.keys():
            sbid = '{0}_{1}'.format(block_id, uuid.uuid4())
            bd = self.meta_data[block_id].data
            bs = self.meta_data[block_id].block_size
            rc = random.randint(0, 4)
            rmod = generate_ref_modified()

            self.storage_data[sbid] = Block(self.project_id,
                                            self.vault_id,
                                            storage_id=sbid,
                                            block_id=block_id,
                                            data=bd,
                                            block_size=bs,
                                            ref_count=rc,
                                            block_orphaned=False,
                                            ref_modified=rmod)

    def generate_orphaned_blocks(self, count):
        raise NotImplemented('TBD - add orphaned storage blocks')

    def secondary_setup(self, manager_start, manager_end):
        if not hasattr(self, 'project_id'):
            self.project_id = None

        if not hasattr(self, 'vault_id'):
            self.vault_id = None

        if self.project_id is None:
            self.project_id = create_project_name()

        if self.vault_id is None:
            self.vault_id = create_vault_name()

        self.vault = Vault(self.project_id, self.vault_id)
        self.apihost = 'neo.the.one'
        self.authengine = FakeAuthEngine(userid='blue',
                                         usertype='pill',
                                         credentials='morpheus',
                                         auth_method='matrix')
        self.deuce_client = DeuceClient(self.authengine,
                                        self.apihost,
                                        True)
        self.manager = Manager(marker_start=manager_start,
                               marker_end=manager_end)
        self.client = ValereClient(self.deuce_client, self.vault, self.manager)

    def metadata_listing_generator(self, uri):
        sorted_metadata_info = sorted(self.meta_data.keys())

        def get_group(gg_start, gg_end):
            url_base = get_blocks_url(self.apihost,
                                      self.vault.vault_id)

            url = None
            block_set = None
            if gg_start is not None:
                block_set = sorted_metadata_info[gg_start:gg_end]
            else:
                block_set = sorted_metadata_info[:gg_end]

            if gg_end is not None:
                block_next = sorted_metadata_info[gg_end]

                url_params = urllib.parse.urlencode({'marker': block_next})
                next_batch = '{0}?{1}'.format(url_base, url_params)

                return (block_set, next_batch)

            else:
                return (block_set, None)

        parsed_url = urllib.parse.urlparse(uri)
        qs = urllib.parse.parse_qs(parsed_url[4])

        start = 0
        end = len(self.meta_data)
        if 'marker' in qs:
            marker = qs['marker'][0]

            new_start = 0

            for check_index in range(len(self.meta_data)):
                if sorted_metadata_info[check_index] >= marker:
                    new_start = check_index
                    break

            if new_start > start and new_start <= len(self.meta_data):
                start = new_start
                end = start + self.metadata_calculate_position()

            if end >= len(self.meta_data):
                end = None

        else:
            start = None
            end = self.metadata_calculate_position()

        return get_group(start, end)

    def metadata_block_listing_success(self, request, uri, headers):
        body, next_batch = self.metadata_listing_generator(uri)
        if next_batch is not None:
            headers.update({'x-next-batch': next_batch})

        return (200, headers, json.dumps(body))

    def metadata_block_head_success(self, request, uri, headers):
        parsed_url = urllib.parse.urlparse(uri)
        url_path_parts = parsed_url.path.split('/')
        requested_vault_id = url_path_parts[3]
        requested_block_id = url_path_parts[-1]

        if requested_vault_id != self.vault.vault_id:
            return (404, headers, 'invalid vault id')

        if requested_block_id in self.meta_data:
            pass

        else:
            return (404, headers, 'invalid block id')

    def storage_listing_generator(self, uri):
        sorted_storage_data_info = sorted(self.storage_data.keys())

        def get_group(gg_start, gg_end):
            url_base = get_storage_blocks_url(self.apihost,
                                              self.vault.vault_id)

            url = None
            block_set = None
            if gg_start is not None:
                block_set = sorted_storage_data_info[gg_start:gg_end]
            else:
                block_set = sorted_storage_data_info[:gg_end]

            if gg_end is not None:
                block_next = sorted_storage_data_info[gg_end]

                url_params = urllib.parse.urlencode({'marker': block_next})
                next_batch = '{0}?{1}'.format(url_base, url_params)

                return (block_set, next_batch)

            else:
                return (block_set, None)

        parsed_url = urllib.parse.urlparse(uri)
        qs = urllib.parse.parse_qs(parsed_url[4])

        start = 0
        end = len(self.storage_data)
        if 'marker' in qs:
            marker = qs['marker'][0]

            new_start = 0
            for check_index in range(len(self.storage_data)):
                if sorted_storage_data_info[check_index] >= marker:
                    new_start = check_index
                    break

            if new_start > start and new_start <= len(self.storage_data):
                start = new_start
                end = start + self.storage_calculate_position()

            if end >= len(self.storage_data):
                end = None

        else:
            start = None
            end = self.storage_calculate_position()

        return get_group(start, end)

    def storage_block_listing_success(self, request, uri, headers):
        body, next_batch = self.storage_listing_generator(uri)
        if next_batch is not None:
            headers.update({'x-next-batch': next_batch})

        return (200, headers, json.dumps(body))
