"""
Deuce Valere - Client - Splitter
"""
import logging

from deucevalere.common.validation_instance import *


class ChunkError(Exception):
    pass


class ValereSplitter(object):

    @validate(deuce_client=ClientRule,
              vault=VaultInstanceRule)
    def __init__(self, deuce_client, vault):
        self.deuceclient = deuce_client
        self.vault = vault
        self.log = logging.getLogger(__name__)

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
        try:
            # NOTE(TheSriram): The first block needs to be gotten separately,
            # as markers are returned in x-next-batch only after the first call

            self.deuceclient.GetBlockList(self.vault, limit=1)
            markers.append(list(self.vault.blocks.keys())[0])

            marker = None

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
            msg = 'Chunking for Projectid: {0},' \
                  'Vault: {1} failed!' \
                  'Error : {2}'.format(self.vault.project_id,
                                       self.vault.vault_id,
                                       str(ex))
            self.log.warn(msg)
            raise ChunkError(msg)
