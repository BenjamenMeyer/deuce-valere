"""
Deuce Valere - Client - Splitter
"""
import itertools
import logging

from deucevalere.common.validation_instance import *


class MetaChunkError(Exception):
    pass


class StorageChunkError(Exception):
    pass


class ValereSplitter(object):

    @validate(deuce_client=ClientRule,
              vault=VaultInstanceRule)
    def __init__(self, deuce_client, vault):
        self.deuceclient = deuce_client
        self.vault = vault
        self.log = logging.getLogger(__name__)

    @validate(limit=LimitRule)
    def store_chunker(self, limit):
        """
        The store_chunker is called when the listing of metadata blocks
        yielded an empty list. The list of storage blocks would then be
        chunked, by extracting the metadata block_id(sha1) from each of
        the storage blocks.(Since each storageblock is of the form
        sha1_uuid5)

        :param limit: number of elements per chunk
        :return: a list of lists containing projectid, vaultid
                 start marker and end marker
        """
        marker = None

        def storage_list(marker, limit):

            try:
                storage_ids = self.deuceclient.GetBlockStorageList(self.vault,
                    marker=marker,
                    limit=limit)
            except RuntimeError as ex:
                msg = 'Storage Chunking for Projectid: {0},' \
                      'Vault: {1} failed!' \
                      'Error : {2}'.format(self.vault.project_id,
                                           self.vault.vault_id,
                                           str(ex))
                self.log.warn(msg)
                raise StorageChunkError(msg)

            if not len(storage_ids):
                return (None, None, None)

            st_marker = self.vault.storageblocks.marker

            if not st_marker:
                return (storage_ids[0].split('_')[0],
                        None,
                        None)

            return (storage_ids[0].split('_')[0],
                    st_marker.split('_')[0],
                    st_marker)

        while True:

            start, end, st_marker = storage_list(marker, limit)

            if (start, end, st_marker) == (None, None, None):
                break

            gen_expr = [self.vault.project_id,
                        self.vault.vault_id,
                        start,
                        end]
            marker = st_marker

            if start != end:
                yield gen_expr

            if not end:  # pragma: no cover
                break

    @validate(limit=LimitRule)
    def meta_chunker(self, limit):
        """
        The chunker splits the listing of metadata blocks from a vault
        that belongs to a specific projectid into manageable chunks.
        This allows instantiating the Manager object with different
        start and end markers, therefore allowing them to be validated.
        :param limit: number of elements per chunk
        :return: a list of lists containing projectid, vaultid
                 start marker and an end marker.
        """
        marker = None
        storage = None

        def metadata_list(marker, limit):

            try:

                block_ids = self.deuceclient.GetBlockList(self.vault,
                                                          marker=marker,
                                                          limit=limit)

            except RuntimeError as ex:
                msg = 'Storage Chunking for Projectid: {0},' \
                      'Vault: {1} failed!' \
                      'Error : {2}'.format(self.vault.project_id,
                                           self.vault.vault_id,
                                           str(ex))
                self.log.warn(msg)
                raise MetaChunkError(msg)

            if not len(block_ids):
                return (None, None)

            marker = self.vault.storageblocks.marker

            return (block_ids[0],
                    marker)
        while True:

            start, end = metadata_list(marker, limit)

            if (start, end) == (None, None):
                storage = True
                break

            gen_expr = [self.vault.project_id,
                        self.vault.vault_id,
                        start,
                        end]
            marker = end

            yield gen_expr

            if not end:  # pragma: no cover
                break

        if storage:
            yield from self.store_chunker(limit)
