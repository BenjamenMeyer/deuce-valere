"""
Deuce Valere - Tests - API - Shell Cache
"""
import builtins
import os
import os.path
import tempfile
import unittest

from deuceclient.api import Vault
from deuceclient.tests import *
import mock

from deucevalere.api.shell_cache import *
from deucevalere.api.system import Manager
from deucevalere.tests.client_base import TestValereClientBase


class DeuceValereApiShellCacheTest(TestValereClientBase):

    def setUp(self):
        super().setUp()
        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.vault = Vault(self.project_id,
                           self.vault_id)
        self.manager = Manager()

        self.cache_dir_object = tempfile.TemporaryDirectory()
        self.cache_dir = self.cache_dir_object.name

        # Note: Aside from the test_shell_cache_create()
        #       all tests should reset self.case.base to
        #       self.cache_dir so that cleanup happens properly
        #       and automatically after the test is complete
        self.cache = ShellCache()

    def tearDown(self):
        super().tearDown()
        self.cache_dir_object.cleanup()

    def test_shell_cache_default_path(self):
        self.assertEqual(ShellCache.default_directory(),
                         os.path.join(ShellCache.defaults['base'],
                                      ShellCache.defaults['dirname']))

    def test_shell_cache_create(self):
        if os.environ['HOME']:
            self.assertEqual(self.cache.base,
                             os.environ['HOME'])
        else:
            self.assertEqual(self.cache.base,
                             tempfile.gettempdir())
        self.assertEqual(self.cache.base,
                         ShellCache.defaults['base'])

        self.assertEqual(self.cache.dirname,
                         '.deuce_valere')
        self.assertEqual(self.cache.dirname,
                         ShellCache.defaults['dirname'])

    def test_shell_cache_set_base(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.assertNotEqual(self.cache.base,
                            ShellCache.defaults['base'])
        self.assertEqual(self.cache.dirname,
                         ShellCache.defaults['dirname'])

    def test_shell_cache_set_base_reset(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.assertNotEqual(self.cache.base,
                            ShellCache.defaults['base'])
        self.cache.base = None
        self.assertEqual(self.cache.base,
                         ShellCache.defaults['base'])
        self.assertNotEqual(self.cache.base,
                            self.cache_dir)

    def test_shell_cache_set_dirname(self):
        dirname = '.deuce-valere-test'

        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.dirname = dirname
        self.assertEqual(self.cache.dirname,
                         dirname)

    def test_shell_cache_set_dirname_reset(self):
        dirname = '.deuce-valere-test'

        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.dirname = dirname
        self.assertEqual(self.cache.dirname,
                         dirname)
        self.assertNotEqual(self.cache.dirname,
                            ShellCache.defaults['dirname'])
        self.cache.dirname = None
        self.assertEqual(self.cache.dirname,
                         ShellCache.defaults['dirname'])
        self.assertNotEqual(self.cache.dirname,
                            dirname)

    def test_shell_cache_set_dirname_failure_create(self):
        dirname = '.deuce-valere-test'

        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)

        with mock.patch('os.path.exists') as mok_exists:
            with mock.patch('os.mkdir') as mok_mkdir:
                mok_exists.side_effect = [False, False]
                mok_mkdir.return_value = None
                with self.assertRaises(builtins.NotADirectoryError):
                    self.cache.dirname = dirname

    def test_shell_cache_set_dirname_failure_exists(self):
        dirname = '.deuce-valere-test'

        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)

        with mock.patch('os.path.exists') as mok_exists:
            with mock.patch('os.path.isdir') as mok_isdir:
                mok_exists.return_value = True
                mok_isdir.return_value = False
                with self.assertRaises(builtins.NotADirectoryError):
                    self.cache.dirname = dirname

    def test_shell_cache_manager_clear(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.add_manager(self.vault, self.manager)
        self.cache.clear_manager(self.vault, self.manager)

    def test_shell_cache_manager_clear_uncached(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.clear_manager(self.vault, self.manager)

    def test_shell_cache_manager_basic(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.add_manager(self.vault, self.manager)

        new_manager = self.cache.get_manager(self.vault,
                                             None,
                                             None,
                                             None)

        self.check_manager(self.manager,
                           new_manager)

    def test_shell_cache_manager_basic_with_age(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.add_manager(self.vault, self.manager)

        expire_age = datetime.timedelta(weeks=52)
        new_manager = self.cache.get_manager(self.vault,
                                             None,
                                             None,
                                             expire_age)

        self.check_manager(self.manager,
                           new_manager)

    def test_shell_cache_manager_expired(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.add_manager(self.vault, self.manager)

        expire_age = datetime.timedelta(microseconds=5)
        with self.assertRaises(ExpiredCacheData):
            new_manager = self.cache.get_manager(self.vault,
                                                 None,
                                                 None,
                                                 expire_age)

    def test_shell_cache_manager_no_data(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)

        with self.assertRaises(NoCachedData):
            new_manager = self.cache.get_manager(self.vault,
                                                 None,
                                                 None,
                                                 None)

    def test_shell_cache_manager_basic_with_start_end(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)

        start_block = create_block()[0]
        end_block = create_block()[0]
        self.manager = Manager(start_block, end_block)

        self.cache.add_manager(self.vault, self.manager)

        new_manager = self.cache.get_manager(self.vault,
                                             start_block,
                                             end_block,
                                             None)

        self.check_manager(self.manager,
                           new_manager)

    def test_shell_cache_add_manager_failure(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)

        with mock.patch('builtins.open',
                        mock.mock_open(), create=True) as mok_open:
            mok_open.side_effect = RuntimeError('mock failure')
            with self.assertRaises(RuntimeError):
                self.cache.add_manager(self.vault, self.manager)

    def test_shell_cache_vault_clear(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.add_vault(self.vault, self.manager)
        self.cache.clear_vault(self.vault, self.manager)

    def test_shell_cache_vault_clear_uncached(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.clear_vault(self.vault, self.manager)

    def test_shell_cache_vault_basic(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.add_vault(self.vault, self.manager)

        new_vault = self.cache.load_vault(self.vault,
                                          None,
                                          None,
                                          None)
        self.check_vault(self.vault,
                         new_vault)

    def test_shell_cache_vault_basic_with_age(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.add_vault(self.vault, self.manager)

        expire_age = datetime.timedelta(weeks=52)

        new_vault = self.cache.load_vault(self.vault,
                                          None,
                                          None,
                                          expire_age)
        self.check_vault(self.vault,
                         new_vault)

    def test_shell_cache_vault_expired(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)
        self.cache.add_vault(self.vault, self.manager)

        expire_age = datetime.timedelta(microseconds=5)
        with self.assertRaises(ExpiredCacheData):
            self.cache.load_vault(self.vault,
                                  None,
                                  None,
                                  expire_age)

    def test_shell_cache_vault_no_data(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)

        with self.assertRaises(NoCachedData):
            self.cache.load_vault(self.vault,
                                  None,
                                  None,
                                  None)

    def test_shell_cache_vault_basic_with_start_end(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)

        start_block = create_block()[0]
        end_block = create_block()[0]
        self.manager = Manager(start_block, end_block)

        self.cache.add_vault(self.vault, self.manager)

        new_vault = self.cache.load_vault(self.vault,
                                          start_block,
                                          end_block,
                                          None)
        self.check_vault(self.vault,
                         new_vault)

    def test_shell_cache_add_vault_failure(self):
        self.cache.base = self.cache_dir
        self.assertEqual(self.cache.base,
                         self.cache_dir)

        with mock.patch('builtins.open',
                        mock.mock_open(), create=True) as mok_open:
            mok_open.side_effect = RuntimeError('mock failure')
            with self.assertRaises(RuntimeError):
                self.cache.add_vault(self.vault, self.manager)
