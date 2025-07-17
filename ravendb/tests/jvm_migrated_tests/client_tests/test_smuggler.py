import functools

from ravendb.documents.smuggler.backup_utils import BackupUtils
from ravendb.tests.test_base import TestBase

from pathlib import Path


class TestSmuggler(TestBase):
    def setUp(self):
        super(TestSmuggler, self).setUp()

    def test_can_sort_files(self):
        files = [
            "2018-11-08-10-47.ravendb-incremental-backup",
            "2018-11-08-10-46.ravendb-incremental-backup",
            "2018-11-08-10-46.ravendb-full-backup"
        ]

        mapped_files = [Path(file) for file in files]
        sorted_files = sorted(mapped_files, key=functools.cmp_to_key(BackupUtils.file_comparator))

        self.assertEqual("2018-11-08-10-46.ravendb-full-backup", sorted_files[0].name)
        self.assertEqual("2018-11-08-10-46.ravendb-incremental-backup", sorted_files[1].name)
        self.assertEqual("2018-11-08-10-47.ravendb-incremental-backup", sorted_files[2].name)