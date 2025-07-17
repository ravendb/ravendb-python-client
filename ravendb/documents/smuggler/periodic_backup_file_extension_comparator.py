from pathlib import Path

from ravendb.primitives.constants import PeriodicBackup

class PeriodicBackupFileExtensionComparator:

    @staticmethod
    def compare(file1: Path, file2: Path) -> int:
        if file1.resolve() == file2.resolve():
            return 0

        ext1 = file1.suffix[1:].lower() if file1.suffix.startswith('.') else file1.suffix.lower()

        if ext1 == PeriodicBackup.SNAPSHOT_EXTENSION:
            return -1

        if ext1 == PeriodicBackup.FULL_BACKUP_EXTENSION:
            return -1

        return 1