"""
Deuce Valere - Client - Base Test Functionality
"""
import datetime
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


def calculate_ref_modified(base=None, days=0, hours=0, mins=0, secs=0):
    date_base = datetime.datetime.utcnow() if base is None else base
    modified = (date_base - datetime.timedelta(days=days,
                                               hours=hours,
                                               minutes=mins,
                                               seconds=secs))
    return int(modified.timestamp())


def generate_ref_modified():
    return calculate_ref_modified(
        days=random.randint(0, 60),
        hours=random.randint(0, 23),
        mins=random.randint(0, 59),
        secs=random.randint(0, 59)
    )


def generate_ref_modified_now():
    return calculate_ref_modified()


class TestValereClientBase(VaultTestBase):

    def setUp(self):
        super().setUp()
        self.meta_data = None
        self.storage_data = None
        self.data_splitter = 3

    def tearDown(self):
        super().tearDown()

    def check_time_manager(self, a, b):
        self.assertEqual(a.name, b.name)
        self.assertEqual(a.start, b.start)
        self.assertEqual(a.end, b.end)

    def check_counter_manager(self, a, b):
        self.assertEqual(a.name, b.name)
        self.assertEqual(a.count, b.count)
        self.assertEqual(a.size, b.size)

    def check_list_manager(self, a, b):
        def __check_list(c, d):
            if c is None:
                self.assertIsNone(d)
            else:
                self.assertEqual(c, d)

        self.assertEqual(a.name, b.name)
        __check_list(a.current, b.current)
        __check_list(a.expired, b.expired)
        __check_list(a.deleted, b.deleted)
        __check_list(a.orphaned, b.orphaned)

    def check_manager(self, a, b):
        def __check_marker(c, d):
            if c is None:
                self.assertIsNone(d)
            else:
                self.assertEqual(c, d)

        self.assertEqual(a.expire_age, b.expire_age)
        __check_marker(a.start_block, b.start_block)
        __check_marker(a.end_block, b.end_block)
        self.check_time_manager(a.validation_timer,
                                b.validation_timer)
        self.check_time_manager(a.cleanup_timer,
                                b.cleanup_timer)
        self.check_counter_manager(a.expired_counter,
                                   b.expired_counter)
        self.check_counter_manager(a.missing_counter,
                                   b.missing_counter)
        self.check_counter_manager(a.orphaned_counter,
                                   b.orphaned_counter)
        self.check_list_manager(a.metadata,
                                b.metadata)
        self.check_list_manager(a.storage,
                                b.storage)
        self.assertEqual(a.cross_reference,
                         b.cross_reference)

    def get_metadata_block_pattern_matcher(self):
        base_url = get_blocks_url(self.apihost, self.vault.vault_id)
        regex = '{0:}/{1:}'.format(base_url[8:],
                                   METADATA_BLOCK_ID_REGEX.pattern[2:-2])
        return re.compile(regex)

    def get_storage_block_pattern_matcher(self):
        base_url = get_storage_blocks_url(self.apihost, self.vault.vault_id)
        regex = '{0:}/{1:}'.format(base_url[8:],
                                   STORAGE_BLOCK_ID_REGEX.pattern)
        return re.compile(regex)

    def metadata_calculate_position(self, splitter=None):
        if splitter is None:
            splitter = self.data_splitter

        return int(len(self.meta_data) / splitter)

    def storage_calculate_position(self, splitter=None):
        if splitter is None:
            splitter = self.data_splitter

        return int(len(self.storage_data) / splitter)

    def generate_empty_blocks(self, count):
        for _ in range(count):
            self.meta_data.update({
                block[0]: Block(self.project_id,
                                self.vault_id,
                                block_id=block[0],
                                data=None,
                                block_size=0,
                                ref_count=0,
                                block_orphaned=False,
                                ref_modified=generate_ref_modified_now()
                                )
                for block in create_blocks(block_count=count)})

    def generate_blocks(self, count):

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
            bs = len(self.meta_data[block_id])
            rc = random.randint(0, 4)
            rmod = generate_ref_modified()

            # Update Metadata
            self.meta_data[block_id].storage_id = sbid

            # Insert into Storage
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

        def make_orphaned_storage_block(block_id):
            sbid = '{0}_{1}'.format(block_id, uuid.uuid4())
            bd = self.meta_data[block_id].data
            bs = len(self.meta_data[block_id])
            rc = random.randint(0, 4)
            rmod = generate_ref_modified()

            # Update Metadata
            self.meta_data[block_id].storage_id = sbid

            # Insert into Storage
            self.storage_data[sbid] = Block(self.project_id,
                                            self.vault_id,
                                            storage_id=sbid,
                                            block_id=None,
                                            data=bd,
                                            block_size=bs,
                                            ref_count=rc,
                                            block_orphaned=True,
                                            ref_modified=rmod,
                                            block_type='storage')

        # This creates an equal distribution for
        # what is divisible:
        #   20 blocks + 20 orphaned -> 1 block each
        #   20 blocks + 15 orphaned -> 0 blocks each
        #   20 blocks + 30 orphaned -> 1 block each
        distribution = count // len(self.meta_data)

        # since the number may not be wholly divisible
        # (f.e the 20/30 example above) then we have to
        # add an extra
        extra_block = count % len(self.meta_data)

        total_orphaned = 0
        for block_id in self.meta_data.keys():
            for _ in range(distribution):
                if total_orphaned < count:
                    make_orphaned_storage_block(block_id)
                total_orphaned = total_orphaned + 1

            if extra_block:
                make_orphaned_storage_block(block_id)
                # so that the extra block gets even
                # distributed as well without affecting
                # the primary distribution of blocks as
                # would happen if this was a simple boolean value
                extra_block = extra_block - 1
                total_orphaned = total_orphaned + 1

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

                try:
                    block_next = sorted_metadata_info[gg_end]
                    url_params = urllib.parse.urlencode({'marker': block_next})
                    next_batch = '{0}?{1}'.format(url_base, url_params)

                except IndexError:
                    return (block_set, None)

                return (block_set, next_batch)

            else:
                return (block_set, None)

        parsed_url = urllib.parse.urlparse(uri)
        qs = urllib.parse.parse_qs(parsed_url[4])

        start = 0
        end = len(self.meta_data)

        if 'marker' in qs and 'limit' in qs:
            limit = int(qs['limit'][0])
            marker = qs['marker'][0]

            new_start = 0

            for check_index in range(len(self.meta_data)):
                if sorted_metadata_info[check_index] >= marker:
                    new_start = check_index
                    break

            if new_start > start and new_start <= len(self.meta_data):
                start = new_start
                end = start + limit
            if end >= len(self.meta_data):
                end = None

        elif 'limit' in qs:
            limit = int(qs['limit'][0])
            end = start + limit

        elif 'marker' in qs:
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

            bid = requested_block_id
            headers['X-Block-Reference-Count'] = self.meta_data[bid].ref_count
            headers['X-Ref-Modified'] = self.meta_data[bid].ref_modified
            headers['X-Storage-ID'] = self.meta_data[bid].storage_id
            headers['X-Block-ID'] = self.meta_data[bid].block_id
            headers['X-Block-Size'] = len(self.meta_data[bid])
            return (204, headers, '')

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

                try:
                    block_next = sorted_storage_data_info[gg_end]
                    url_params = urllib.parse.urlencode({'marker': block_next})
                    next_batch = '{0}?{1}'.format(url_base, url_params)

                except IndexError:
                    return (block_set, None)

                return (block_set, next_batch)

            else:
                return (block_set, None)

        parsed_url = urllib.parse.urlparse(uri)
        qs = urllib.parse.parse_qs(parsed_url[4])

        start = 0
        end = len(self.storage_data)

        if 'marker' in qs and 'limit' in qs:
            limit = int(qs['limit'][0])
            marker = qs['marker'][0]

            new_start = 0

            for check_index in range(len(self.storage_data)):
                if sorted_storage_data_info[check_index] >= marker:
                    new_start = check_index
                    break

            if new_start >= start and new_start <= len(self.storage_data):
                start = new_start
                end = start + limit
            if end >= len(self.storage_data):
                end = None

        elif 'limit' in qs:
            limit = int(qs['limit'][0])
            end = start + limit

        elif 'marker' in qs:
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

    def storage_block_head_success(self, request, uri, headers):
        parsed_url = urllib.parse.urlparse(uri)
        url_path_parts = parsed_url.path.split('/')
        requested_vault_id = url_path_parts[3]
        requested_block_id = url_path_parts[-1]

        if requested_vault_id != self.vault.vault_id:
            return (404, headers, 'invalid vault id')

        if requested_block_id in self.storage_data:

            bid = requested_block_id
            headers['X-Block-Reference-Count'] = \
                self.storage_data[bid].ref_count
            headers['X-Ref-Modified'] = self.storage_data[bid].ref_modified
            headers['X-Storage-ID'] = self.storage_data[bid].storage_id
            headers['X-Block-Size'] = len(self.storage_data[bid])

            headers['X-Block-Orphaned'] = \
                self.storage_data[bid].block_orphaned
            headers['X-Block-ID'] = self.storage_data[bid].block_id \
                if self.storage_data[bid].block_id is not None \
                else 'None'

            return (204, headers, '')

        else:
            return (404, headers, 'invalid block id')

    def guarantee_expired(self, expired_count, expired_age):
        base_age_date = datetime.datetime.utcnow()

        key_set = sorted(
            list(self.meta_data.keys()))[0:minmax(len(self.meta_data),
                                                  expired_count)]
        for key in key_set:
            self.meta_data[key].ref_count = 0
            self.meta_data[key].ref_modified = \
                calculate_ref_modified(base=base_age_date,
                                       days=0, hours=0, mins=1, secs=0)

        self.manager.metadata.expired = []
        for key in key_set[:int(len(key_set) / 2)]:
            self.manager.metadata.expired.append(key)

        self.manager.expire_age = expired_age

        check_count = 0
        for key, block in self.meta_data.items():
            check_delta = base_age_date - datetime.datetime.utcfromtimestamp(
                block.ref_modified)
            if check_delta > self.manager.expire_age and block.ref_count == 0:
                check_count = check_count + 1
                print('Block {0:} has expired age of {1}'
                      .format(block.block_id,
                              check_delta))
        return check_count

    def guarantee_deleted(self, count):
        base_age_date = datetime.datetime.utcnow()
        total_deleted = 0

        if self.manager.storage.deleted is None:
            self.manager.storage.deleted = []

        if self.manager.metadata.deleted is None:
            self.manager.metadata.deleted = []

        # check the list for expired blocks and record some for
        for key, block in self.meta_data.items():
            if block.ref_count == 0:
                check_delta = base_age_date - \
                    datetime.datetime.utcfromtimestamp(block.ref_modified)
                if check_delta > self.manager.expire_age:
                    self.manager.storage.deleted.append(block.storage_id)
                    self.manager.metadata.deleted.append(block.block_id)
                    total_deleted = total_deleted + 1
                    print('Block {0:} is deleted'
                          .format(block.storage_id))

            # only record as many deleted as requested
            if total_deleted >= count:
                break

        self.assertTrue(total_deleted == count,
            'Only {0} of {1} blocks were recorded as deleted by test setup'
            .format(total_deleted, count))

    def calculate_cross_reference_length(self):
        if self.manager.metadata.current is None:
            return 0

        expected_length = len(self.manager.metadata.current)
        for block_id in self.manager.metadata.current:
            if self.manager.metadata.expired is not None:
                if block_id in self.manager.metadata.expired:
                    expected_length = expected_length - 1
                continue

            if self.manager.metadata.deleted is not None:
                if block_id in self.manager.metadata.deleted:
                    expected_length = expected_length - 1
                continue

        return expected_length
