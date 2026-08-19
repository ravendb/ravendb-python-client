"""Integration test for the cluster-transaction change-vector fix.

After saving a document in a CLUSTER_WIDE transaction, the session's
last_cluster_transaction_index must be advanced to the cluster-transaction etag
of the stored document (the etag read from the last '|'-separated part of the
change vector). Skipped when RAVENDB_LICENSE is not set, following the
AI-agent/CDC integration pattern.
"""

import os
import unittest

from ravendb.documents.session.misc import SessionOptions, TransactionMode
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestClusterTransactionChangeVector(TestBase):
    def test_last_cluster_transaction_index_advanced_after_cluster_transaction(self):
        user = User(name="Karmel")

        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)
        session_options.disable_atomic_document_writes_in_cluster_wide_transaction = True

        with self.store.open_session(session_options=session_options) as session:
            session.store(user, "users/1")
            session.save_changes()

            # The Database-Cluster-Tx-Id header captured on the save response gives the
            # cluster id; the stored change vector carries the cluster-transaction segment
            # as its last '|'-separated part, so the session index must advance.
            self.assertGreater(session.session_info.last_cluster_transaction_index or 0, 0)
            self.assertIsNotNone(session.session_info.cluster_transaction_id)


if __name__ == "__main__":
    unittest.main()
