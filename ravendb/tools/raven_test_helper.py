import os
from datetime import datetime
from typing import Optional

from ravendb import DocumentStore, GetIndexErrorsOperation


class RavenTestHelper:
    @staticmethod
    def utc_today() -> datetime:
        today = datetime.today()
        return datetime(today.year, today.month, today.day, 0, 0, 0, 0)

    @staticmethod
    def utc_this_month() -> datetime:
        today = datetime.today()
        return datetime(today.year, today.month, 1, 0, 0, 0, 0)

    @staticmethod
    def assert_no_index_errors(store: DocumentStore, database_name: Optional[str] = None) -> None:
        errors = store.maintenance.for_database(database_name).send(GetIndexErrorsOperation())

        sb = []
        for index_errors in errors:
            if not index_errors or not index_errors.errors:
                continue

            sb.append("Index Errors for '")
            sb.append(index_errors.name)
            sb.append("' (")
            sb.append(len(index_errors.errors))
            sb.append(")")
            sb.append(os.linesep)

            for index_error in index_errors.errors:
                sb.append(f"- {index_error}")
                sb.append(os.linesep)

            sb.append(os.linesep)

        if not sb:
            return

        raise RuntimeError("".join(map(str, sb)))
