"""
Deuce Valere - API - Shell Cache
"""
from builtins import NotADirectoryError
import datetime
import json
import logging
import os
import os.path
import tempfile

from stoplight import validate
from deuceclient.api import Vault
from deuceclient.common.validation import *
from deuceclient.common.validation_instance import *

from deucevalere.api.system import Manager
from deucevalere.common.validation import ExpireAgeRuleNoneOkay
from deucevalere.common.validation_instance import ValereManagerRule


class ExpiredCacheData(Exception):
    pass


class NoCachedData(Exception):
    pass


class ShellCache(object):
    """A simple cache mechanism for caching the manager and related data

        This is primarily for use by the Deuce-Valere Shell so that
        information can be cached between runs without requiring the user
        to provide all the information for each step or do steps multiple
        times just to have the data.

        For instance, if the user validates a vault in one instance there
        is then information they would need to provide back to the shell
        in order to clean it. Either the information must be provided by
        the user (not good) or we need some way to cache it between runs.

        This cache is NOT thread/process safe. Multiple processes/threads
        can use it so long as they are not operating on overlapping ranges
        within a vault. For instance any of the following would not be
        good as start/stop ranges (assuming the single letters would
        qualify as markers):
            [None, None]
            [0, F)
            [9, Z)
            [None, G)
            [5, None]
    """

    defaults = {
        # Because HOME might not be defined in some environments
        # This is mainly to solve the issue of when the project
        # gets compiled and HOME is not defined in the environment
        # the compiler is running under to keep the compiler evaluation
        # from failing
        'base': os.environ['HOME']
        if os.environ['HOME'] else temp.gettempdir(),

        'dirname': '.deuce_valere'
    }

    @classmethod
    def default_directory(cls):
        """Return the default path for the cache
        """
        return os.path.join(ShellCache.defaults['base'],
                            ShellCache.defaults['dirname'])

    def __init__(self):
        self.log = logging.getLogger(__name__)
        # Note: 'base' and 'dirname' are only checked for being valid
        #       and usable if either (i) they are changed or
        #       (ii) something is uses the cache. Thus just creating
        #       an instance of ShellCache will not try to create the
        #       locations the cache uses
        self.__cache_info = {
            'base': self.defaults['base'],
            'dirname': self.defaults['dirname']
        }

    def __check_path_exists(self, path):
        if not os.path.exists(path):
            os.mkdir(path)
            if os.path.exists(path):
                self.log.debug('Path exists - {0}'.format(path))
                return True
            else:
                raise NotADirectoryError(
                    '{0} is not a path on disk'.format(path))
        elif os.path.isdir(path):
            self.log.debug('Path is a directory - {0}'.format(path))
            return True
        else:
            raise NotADirectoryError(
                '{0} is not a path on disk'.format(path))

    def __check_dir_name_exists(self, dirname=None):
        path = self.cache_dir
        if dirname is not None:
            path = os.path.join(self.base,
                                dirname)
        return self.__check_path_exists(path)

    @property
    def base(self):
        """Active Base directory for where the cache will be stored under

        Default: os.environ['HOME'] if defined, otherwise TEMPDIR as
            defined by tempfile.gettempdir()
        """
        return self.__cache_info['base']

    @base.setter
    def base(self, value):
        """Set a new base directory

        Path must exist on disk and be a directory, or be createable by
        the current user.

        :parameter value: str - directory name (path) of the base

        :returns: N/A
        :raises:
            NotADirectoryError if parameter is not a directory or
                               a directory by that specification could not
                               be created
        """
        if value is None:
            value = ShellCache.defaults['base']

        self.__check_path_exists(value)
        self.__cache_info['base'] = value

    @property
    def dirname(self):
        """Directory name under Manager.base that will be used for the cache
        """
        return self.__cache_info['dirname']

    @dirname.setter
    def dirname(self, value):
        """Set a new cache directory under the base

        Path must exist on disk under Manager.base and be a directory,
        or be createable as such by the current user.

        :parameter value: str - directory name to use

        :returns: N/A
        :raises:
            NotADirectoryError if parameter is not a directory or
                               a directory by that specification could not
                               be created
        """
        if value is None:
            value = ShellCache.defaults['dirname']

        self.__check_dir_name_exists(dirname=value)
        self.__cache_info['dirname'] = value

    @property
    def cache_dir(self):
        """The path to the active cache
        """
        return os.path.join(self.base, self.dirname)

    @validate(vault=VaultInstanceRule,
              start_block=MetadataBlockIdRuleNoneOkay,
              end_block=MetadataBlockIdRuleNoneOkay)
    def __get_key(self, vault, start_block, end_block):
        """(Internal) Get the key for given vault and markers

        :parameter vault: instance of deuceclient.api.Vault
        :parameter start_block: str - block_id in Vault for the start block
                                      or None
        :parameter end_block: str - block_id in Vault for the end block
                                    or None

        :returns: str - key to use for the specified parameters
        """

        # Base key name
        key = '{0}__{1}'.format(vault.project_id,
                                vault.vault_id)

        # Add the start marker if it exists
        if start_block is not None:
            key = '{0}_start_{1}'.format(key,
                                         start_block)

        # Add the end marker if it exists
        if end_block is not None:
            key = '{0}_end_{1}'.format(key,
                                       end_block)

        # log the resulting key
        self.log.info('Key: Project {0}, Vault {1}, Manager [{2},{3}) => {4}'
                      .format(vault.project_id,
                              vault.vault_id,
                              start_block,
                              end_block,
                              key))
        return key

    def __get_manager_cache_file(self, key):
        """Get the cache file name for the manager data

        :parameter key: str - key for the data

        :returns: str - filename to use for caching the manager data
        """
        self.__check_dir_name_exists()
        manager_key = 'cached_manager_{0}'.format(key)
        return os.path.join(self.cache_dir, manager_key)

    def __get_vault_cache_file(self, key):
        """Get the cache file name for the vault data

        :parameter key: str - key for the data

        :returns: str - filename to use for caching the vault data
        """
        self.__check_dir_name_exists()
        vault_key = 'cached_vault_{0}'.format(key)
        return os.path.join(self.cache_dir, vault_key)

    @validate(vault=VaultInstanceRule,
              manager=ValereManagerRule)
    def add_manager(self, vault, manager):
        """Add the manager to the cache

        :parameter vault: instance of deuceclient.api.Vault
        :parameter manager: instance of deucevalere.api.system.Manager

        :returns: True on success
        :raises: Passes thru any exceptions while trying to write
                 the data to disk
        """
        now = datetime.datetime.utcnow()
        json_data = json.dumps({
            'timestamp': {
                'year': now.year,
                'month': now.month,
                'day': now.day,
                'hour': now.hour,
                'minute': now.minute,
                'second': now.second,
                'microsecond': now.microsecond
            },
            'manager': manager.serialize()
        })

        key = self.__get_key(vault,
                             manager.start_block,
                             manager.end_block)

        file_name = self.__get_manager_cache_file(key)
        self.log.info('Writing cached data to {0}'.format(file_name))

        try:
            with open(file_name, 'w') as data_store:
                data_store.write(json_data)
                return True
        except Exception as ex:
            self.log.error(
                'Error while writing to cache: {0}'.format(str(ex)))
            raise

    @validate(vault=VaultInstanceRule,
              manager=ValereManagerRule)
    def clear_manager(self, vault, manager):
        """Add the vault to the cache

        :parameter vault: instance of deuceclient.api.Vault
        :parameter manager: instance of deucevalere.api.system.Manager

        :returns: None
        :raises: Passes thru any exceptions while trying to write
                 the data to disk
        """
        key = self.__get_key(vault,
                             manager.start_block,
                             manager.end_block)
        file_name = self.__get_manager_cache_file(key)
        self.log.info('Clearing cache file {0}'.format(file_name))
        if os.path.exists(file_name):
            os.remove(file_name)

    @validate(vault=VaultInstanceRule,
              start_block=MetadataBlockIdRuleNoneOkay,
              end_block=MetadataBlockIdRuleNoneOkay,
              cache_age_max=ExpireAgeRuleNoneOkay)
    def get_manager(self, vault, start_block, end_block, cache_age_max):
        """Retrieve the cached data from disk

        :parameter vault: instance of deuceclient.api.Vault
        :parameter start_block: str - block_id in Vault for the start block
                                      or None
        :parameter end_block: str - block_id in Vault for the end block
                                    or None
        :parameter cache_age_max: instance of datetime.timedeleta specifying
                                  the maximum age of the cached value
                                  or None
        :returns: instance of deucevalere.api.system.Manager if able to
                  load data from the cache
        :raises: Passes thru any exceptions while trying to read the
                 data from disk.
        :raises: ExpiredCacheData if the cached data is too old
        :raises: NoCachedData if no data in the cache
        """
        key = self.__get_key(vault,
                             start_block,
                             end_block)

        file_name = self.__get_manager_cache_file(key)
        self.log.info('Reading cached data from {0}'.format(file_name))

        if os.path.isfile(file_name):
            try:
                with open(file_name, 'r') as data_store:
                    json_data = json.loads(data_store.read())

                    if cache_age_max is None:
                        return Manager.deserialize(json_data['manager'])

                    else:
                        then = datetime.datetime(
                            year=json_data['timestamp']['year'],
                            month=json_data['timestamp']['month'],
                            day=json_data['timestamp']['day'],
                            hour=json_data['timestamp']['hour'],
                            minute=json_data['timestamp']['minute'],
                            second=json_data['timestamp']['second'],
                            microsecond=json_data['timestamp']['microsecond'])
                        cache_data_age = (datetime.datetime.utcnow() -
                                          then)

                        if cache_data_age < cache_age_max:
                            return Manager.deserialize(json_data['manager'])
                        else:
                            raise ExpiredCacheData(
                                'Cached data in {0} from {1} < {2} -> expired '
                                'with age {3}'
                                .format(key,
                                        json_data['timestamp'],
                                        cache_age_max,
                                        cache_data_age))
            except Exception as ex:
                self.log.error(
                    'Error while reading from cache: {0}'.format(str(ex)))
                raise
        else:
            raise NoCachedData('No data in cache')

    @validate(vault=VaultInstanceRule,
              manager=ValereManagerRule)
    def add_vault(self, vault, manager):
        """Add the vault to the cache

        :parameter vault: instance of deuceclient.api.Vault
        :parameter manager: instance of deucevalere.api.system.Manager

        :returns: True on success
        :raises: Passes thru any exceptions while trying to write
                 the data to disk
        """
        now = datetime.datetime.utcnow()
        json_data = json.dumps({
            'timestamp': {
                'year': now.year,
                'month': now.month,
                'day': now.day,
                'hour': now.hour,
                'minute': now.minute,
                'second': now.second,
                'microsecond': now.microsecond
            },
            'vault': vault.serialize()
        })

        key = self.__get_key(vault,
                             manager.start_block,
                             manager.end_block)

        file_name = self.__get_vault_cache_file(key)
        self.log.info('Writing cached data to {0}'.format(file_name))

        try:
            with open(file_name, 'w') as data_store:
                data_store.write(json_data)
                return True
        except Exception as ex:
            self.log.error(
                'Error while writing to cache: {0}'.format(str(ex)))
            raise

    @validate(vault=VaultInstanceRule,
              manager=ValereManagerRule)
    def clear_vault(self, vault, manager):
        """Add the vault to the cache

        :parameter vault: instance of deuceclient.api.Vault
        :parameter manager: instance of deucevalere.api.system.Manager

        :returns: None
        :raises: Passes thru any exceptions while trying to write
                 the data to disk
        """
        key = self.__get_key(vault,
                             manager.start_block,
                             manager.end_block)
        file_name = self.__get_vault_cache_file(key)
        self.log.info('Clearing cache file {0}'.format(file_name))
        if os.path.exists(file_name):
            os.remove(file_name)

    @validate(vault=VaultInstanceRule,
              start_block=MetadataBlockIdRuleNoneOkay,
              end_block=MetadataBlockIdRuleNoneOkay,
              cache_age_max=ExpireAgeRuleNoneOkay)
    def load_vault(self, vault, start_block, end_block, cache_age_max):
        """Retrieve the cached data from disk

        :parameter vault: instance of deuceclient.api.Vault
        :parameter start_block: str - block_id in Vault for the start block
                                      or None
        :parameter end_block: str - block_id in Vault for the end block
                                    or None
        :parameter cache_age_max: instance of datetime.timedeleta specifying
                                  the maximum age of the cached value
                                  or None
        :returns: instance of deucevalere.api.system.Manager if able to
                  load data from the cache
        :raises: Passes thru any exceptions while trying to read the
                 data from disk.
        :raises: ExpiredCacheData if the cached data is too old
        :raises: NoCachedData if no data in the cache
        """
        key = self.__get_key(vault,
                             start_block,
                             end_block)

        file_name = self.__get_vault_cache_file(key)
        self.log.info('Reading cached data from {0}'.format(file_name))

        if os.path.isfile(file_name):
            self.log.info('Found cache file {0}'.format(file_name))
            try:
                with open(file_name, 'r') as data_store:
                    self.log.debug('File opened')
                    json_data = json.loads(data_store.read())
                    self.log.debug('File read and parsed')

                    if cache_age_max is None:
                        return Vault.deserialize(json_data['vault'])

                    else:
                        then = datetime.datetime(
                            year=json_data['timestamp']['year'],
                            month=json_data['timestamp']['month'],
                            day=json_data['timestamp']['day'],
                            hour=json_data['timestamp']['hour'],
                            minute=json_data['timestamp']['minute'],
                            second=json_data['timestamp']['second'],
                            microsecond=json_data['timestamp']['microsecond'])
                        cache_data_age = (datetime.datetime.utcnow() -
                                          then)

                        if cache_data_age < cache_age_max:
                            return Vault.deserialize(json_data['vault'])

                        else:
                            raise ExpiredCacheData(
                                'Cached data in {0} from {1} < {2} -> expired '
                                'with age {3}'
                                .format(key,
                                        json_data['timestamp'],
                                        cache_age_max,
                                        cache_data_age))
            except Exception as ex:
                self.log.error(
                    'Error while reading from cache: {0}'.format(str(ex)))
                raise
        else:
            raise NoCachedData('No data in cache')
