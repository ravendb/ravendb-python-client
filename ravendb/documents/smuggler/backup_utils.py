from pathlib import Path

from ravendb.documents.smuggler.periodic_backup_file_extension_comparator import PeriodicBackupFileExtensionComparator


class BackupUtils:

    @staticmethod
    def file_comparator(file1: Path, file2: Path) -> int:
        base_name1 = file1.stem
        base_name2 = file2.stem

        if base_name1 != base_name2:
            return (base_name1 > base_name2) - (base_name1 < base_name2)

        ext1 = file1.suffix[1:] if file1.suffix.startswith('.') else file1.suffix
        ext2 = file2.suffix[1:] if file2.suffix.startswith('.') else file2.suffix

        if ext1 != ext2:
            return PeriodicBackupFileExtensionComparator.compare(file1, file2)

        last_modified1 = file1.stat().st_mtime
        last_modified2 = file2.stat().st_mtime

        return (last_modified1 > last_modified2) - (last_modified1 < last_modified2)
