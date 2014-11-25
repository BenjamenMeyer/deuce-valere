"""
Tests - Deuce Valere  - Version
"""

from unittest import TestCase

import deucevalere


class VersionTest(TestCase):

    def test_version(self):

        deuce_valere_version = deucevalere.version()

        version_data = deuce_valere_version.split('.')

        self.assertEqual(int(version_data[0]),
                         deucevalere.__DEUCE_VALERE_VERSION__['major'])

        self.assertEqual(int(version_data[1]),
                         deucevalere.__DEUCE_VALERE_VERSION__['minor'])

        self.assertEqual(len(version_data),
                         len(deucevalere.__DEUCE_VALERE_VERSION__))
