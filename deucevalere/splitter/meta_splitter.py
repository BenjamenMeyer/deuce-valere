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

    def determine_storage_end_marker(self, markers, index, limit):
        # NOTE(TheSriram): Slice the list depending on the iteration
        # and the limit, and return the last element in the list. If there
        # is an IndexError, it means we overshot the length of the list,
        # Let the end_marker be None.
        end_marker = markers[index * limit:((index + 1) * limit) + 1][-1:]
        if len(end_marker) == 0:
            return [None]
        else:
            return end_marker

    def determine_storage_start_marker(self, markers, index, limit):
        # NOTE(TheSriram): Slice the list depending on the iteration
        # and the limit, and return the first element in the list
        # If there's an index marker, it means that we overshot the
        # length of the list, so this is would be the end of the iteration.
        # So Lets return the last element in the list
        try:
            return [markers[index * limit:(index + 1) * limit][0]]
        except IndexError:
            return markers[-1:]

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
        markers = []

        try:
            # NOTE(TheSriram): The first block needs to be gotten separately,
            # as markers are returned in x-next-batch only after the first call

            self.deuceclient.GetBlockStorageList(self.vault, limit=1)

            if not len(self.vault.storageblocks):
                # NOTE(TheSriram): Looks like the listing of blocks from
                # storage turned out to be empty, this possibly means that
                # all the blocks have already been cleaned up, or the vault
                # was empty to begin with. If thats the case then both start
                # and end markers are set to None
                return ([[self.vault.project_id,
                          self.vault.vault_id,
                          None,
                          None]])

            marker = list(self.vault.storageblocks.keys())[0]
            markers.append(marker.split('_')[0])

            marker = None

            while True:

                storage_ids = self.deuceclient.GetBlockStorageList(self.vault,
                    marker=marker,
                    limit=limit)
                marker = self.vault.storageblocks.marker

                meta_markers = (st_marker.split('_')[0]
                                for st_marker in storage_ids)
                for meta_marker in meta_markers:
                    if meta_marker not in markers:
                        # NOTE(TheSriram): There might be more than one
                        # orphaned storage block starting with the same
                        # metadata block id. Lets restrict the markers
                        # to be unique
                        markers.append(meta_marker)

                if marker is None:
                    break

            if len(markers) % limit == 0:
                iterations = (len(markers) // limit) + 1

            else:
                iterations = (len(markers) // limit) + 2

            gen_expr = ([self.vault.project_id,
                         self.vault.vault_id] +
                        self.determine_storage_start_marker(markers,
                                                            i,
                                                            limit) +
                        self.determine_storage_end_marker(markers,
                                                          i,
                                                          limit)
                        for i in range(iterations))

            # NOTE (TheSriram): Remove any cases if start and end marker
            # are the same
            return itertools.filterfalse(lambda chunk: chunk[2] == chunk[3],
                                         gen_expr)
        except RuntimeError as ex:
            msg = 'Storage Chunking for Projectid: {0},' \
                  'Vault: {1} failed!' \
                  'Error : {2}'.format(self.vault.project_id,
                                       self.vault.vault_id,
                                       str(ex))
            self.log.warn(msg)
            raise StorageChunkError(msg)

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
        markers = []
        marker = None
        try:
            # NOTE(TheSriram): The first block needs to be gotten separately,
            # as markers are returned in x-next-batch only after the first call

            self.deuceclient.GetBlockList(self.vault, limit=1)

            if not len(self.vault.blocks):
                # NOTE(TheSriram): Looks like the listing of blocks from
                # metadata turned out to be empty, this possibly means that
                # all the blocks that remain in storage are now orphaned.
                # Let's list them out and chunk them as before after
                # extracting the metadata block_id from the returned
                # storage_ids
                return self.store_chunker(limit)

            markers.append(list(self.vault.blocks.keys())[0])

            while True:

                self.deuceclient.GetBlockList(self.vault,
                                              marker=marker,
                                              limit=limit)
                marker = self.vault.blocks.marker

                if marker is None:
                    break
                else:
                    markers.append(marker)

            return ([self.vault.project_id,
                     self.vault.vault_id] + markers[i:i + 2]
                    for i in range(len(markers)))

        except RuntimeError as ex:
            msg = 'Metadata Chunking for Projectid: {0},' \
                  'Vault: {1} failed!' \
                  'Error : {2}'.format(self.vault.project_id,
                                       self.vault.vault_id,
                                       str(ex))
            self.log.warn(msg)
            raise MetaChunkError(msg)
