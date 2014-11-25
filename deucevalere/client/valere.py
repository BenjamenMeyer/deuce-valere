"""
Deuce Valere - Client - Valere
"""
import datetime
import logging

from stoplight import validate

from deucevalere.common.validation import *
from deucevalere.common.validation_instance import *


class ValereClient(object):

    @validate(deuce_client=ClientRule,
              vault=VaultInstanceRule,
              manager=ValereManagerRule)
    def __init__(self, deuce_client, vault, manager):
        self.deuceclient = deuce_client
        self.vault = vault
        self.manager = manager

    def get_block_list(self):
        log = logging.getLogger(__name__)

        self.manager.metadata.current = []

        marker = self.manager.start_block

        while True:
            for block_id in self.deuceclient.GetBlockList(self.vault,
                                                          marker=marker):
                if self.manager.end_block is not None:
                    if block_id < self.manager.end_block:
                        self.manager.metadata.current.append(block_id)
                    else:
                        break
                else:
                    self.manager.metadata.current.append(block_id)

            marker = self.vault.blocks.marker
            log.debug('Next Marker: {0}'.format(marker))

            if marker is None:
                break

    @staticmethod
    @validate(metadata_id=MetadataBlockIdRuleNoneOkay)
    def _convert_metadata_id_to_storage_id(metadata_id):
        """Return a 'valid' storage id that is the lowest possible value

        Note: This will need to be updated when the format of the
              storage id changes.

        Note: It is impossible to create a UUID that is guarateed to be the
              lowest possible value using a UUID generator. However, UUIDs
              are alphanumeric values; thus '0' is always the lowest value
              and filling out a formatstring that looks like a UUID string
              but uses all zeros (0) will giveus a string that will compare
              as the lowest possible UUID value.
        """
        if metadata_id is not None:
            return '{0:}_{1:}-{2:}-{2:}-{2:}-{3:}'.format(metadata_id,
                                                          '{:08X}'.format(0),
                                                          '{:04X}'.format(0),
                                                          '{:012X}'.format(0))
        else:
            return None

    def get_storage_list(self):
        log = logging.getLogger(__name__)

        self.manager.storage.current = []

        start_marker = ValereClient._convert_metadata_id_to_storage_id(
            self.manager.start_block)
        end_marker = ValereClient._convert_metadata_id_to_storage_id(
            self.manager.end_block) if self.manager.end_block else None

        while True:
            for block_id in self.deuceclient.GetBlockStorageList(
                    self.vault, marker=start_marker):

                if end_marker is not None:
                    if block_id < end_marker:
                        self.manager.storage.current.append(block_id)
                    else:
                        break
                else:
                    self.manager.storage.current.append(block_id)

            start_marker = self.vault.storageblocks.marker
            log.debug('Next Marker: {0}'.format(start_marker))

            if start_marker is None:
                break

    def validate_metadata(self, delete_older_than=datetime.datetime.max):
        if self.manager.metadata.current is None:
            self.get_block_list()

        if len(self.manager.metadata.current) == 0:
            self.get_block_list()

        for block_id in self.manager.metadata.current:
            block = self.client.HeadBlock(self.vault, block_id)
