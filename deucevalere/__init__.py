"""
Deuce Valere
"""
import logging

from stoplight import validate

# from deucevalere.api.auth import *
# from deucevalere.api.system import Manager
from deucevalere.client.valere import ValereClient
from deucevalere.common.validation_instance import *
from deucevalere.common.validation import *


__DEUCE_VALERE_VERSION__ = {
    'major': 0,
    'minor': 1
}


def version():
    """Return the Deuce Valere Version"""
    return '{0:}.{1:}'.format(__DEUCE_VALERE_VERSION__['major'],
                              __DEUCE_VALERE_VERSION__['minor'])


@validate(deuece_client=ClientRule,
          vault=VaultInstanceRule,
          manager=ValereManagerRule)
def vault_validate(deuece_client, vault, manager,
                   head_storage_blocks=False):
    """Validate the Deuce Vault by checking all blocks exist

    :param deuece_client: instance of deuceclient.client.deuce
                          to use for interacting with the Deuce Service
    :param vault: instance of deuceclient.api.Vault for the
                  vault to be inspected and cleaned
    :param manager: deucevalere.api.system.Manager instance that will track
                    the state of the system and all statistics

    :returns: zero (0) if all
    :raises: RuntimeError on error
    """
    log = logging.getLogger(__name__)

    # create the valiere client
    valere_client = ValereClient(deuce_client=deuece_client,
                                 vault=vault,
                                 manager=manager)

    # While the validate_metadata() will automatically get
    # the block list, do it now so that it can be easily
    # documented if there is a failure as being the specific
    # issue at hand. All blocks are stored in the 'current'
    # list for metadata data.
    try:
        valere_client.get_block_list()
    except Exception as ex:
        msg = 'Failed to retrieve Metadata Blocks. Error: {0}'\
            .format(str(ex))
        log.error(msg)
        raise RuntimeError(msg)

    # validate the metadata
    #   - HEADs each block to verify it
    #   - checks the ref-count and ref-modified of the block to see
    #     if the block has expired; manager hold the minimum expired age
    #   - expired blocks are added to a specific 'expired' list
    #   - expired blocks are NOT removed from the 'current' list
    valere_client.validate_metadata()

    # While validate_storage*() will automatically get
    # the storage block list, do it now so that is can be
    # easily documented if there is a failure here. All blocks
    # are stored in the 'current' list for storage data.
    try:
        valere_client.get_storage_list()
    except Exception as ex:
        msg = 'Failed to retrieve Storage Blocks. Error: {0}'\
            .format(str(ex))
        log.error(msg)
        raise RuntimeError(msg)

    if head_storage_blocks:
        # This version has no issue with the expired metadata blocks
        # not beinged cleaned up already. It will properly detect
        # which ones are orphaned and which are not. However, this
        # comes at the expense of additional resource utilization by
        # the system due to the HEAD operations on each storage block.
        valere_client.validate_storage_with_head()
    else:
        # If the expired blocks are excluded from the cross-reference
        # then this will detect their storage blocks as being orphaned.
        # This is not desirable because we would end up trying to
        # delete the block twice - when cleanup of metadata occurs and
        # when cleanup of storage occurs, the second of which would fail.
        # Therefore allow the expired blocks to be counted as 'current'
        # here to remove this anomaly and reduce the potential erros.
        valere_client.validate_storage(skip_expired=True)

    return 0


@validate(deuece_client=ClientRule,
          vault=VaultInstanceRule,
          manager=ValereManagerRule)
def vault_cleanup(deuece_client, vault, manager):
    """Cleanup the Deuce Vault by removing orphaned data

    :param deuece_client: instance of deuceclient.client.deuce
                          to use for interacting with the Deuce Service
    :param vault: instance of deuceclient.api.Vault for the
                  vault to be inspected and cleaned
    :param manager: deucevalere.api.system.Manager instance that will track
                    the state of the system and all statistics

    :returns: zero on complete success.
              on partial failure a bitmapped value is returned as follows:

              bit 1 - not all expired metadata blocks were deleted
              bit 2 - not all orphaned storage blocks were deleted

    :raises: RuntimeError on error
    """
    log = logging.getLogger(__name__)

    # create the valiere client
    valere_client = ValereClient(deuce_client=deuece_client,
                                 vault=vault,
                                 manager=manager)

    expired_metadata_count = len(manager.metadata.expired)\
        if manager.metadata.expired is not None else 0
    orphaned_storage_count = len(manager.storage.orphaned)\
        if manager.storage.orphaned is not None else 0

    # try to delete any expired blocks
    #   - successfully deleted blocks are moved to a 'deleted' list
    #     the 'current' list is untouched
    try:
        valere_client.cleanup_expired_blocks()
    except Exception as ex:
        msg = 'Failed to cleanup expired Metadata Blocks. Error: {0}'\
            .format(str(ex))
        log.error(msg)
        raise RuntimeError(msg)

    try:
        valere_client.cleanup_storage()
    except Exception as ex:
        msg = 'Failed to cleanup orphaned Storage Blocks. Error: {0}'\
            .format(str(ex))
        log.error(msg)
        raise RuntimeError(msg)

    return_value = 0

    if expired_metadata_count != len(manager.metadata.deleted):
        return_value = return_value | 1

    if orphaned_storage_count != len(manager.storage.deleted):
        return_value = return_value | 2

    return return_value
