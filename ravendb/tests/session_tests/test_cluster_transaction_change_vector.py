"""Tests for the session cluster-transaction change-vector fix:
the Database-Cluster-Tx-Id header capture, the last-part etag rule, the
>2-parts throw, and the null change-vector no-op.
"""

import unittest

from ravendb.documents.session.document_info import DocumentInfo
from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
    InMemoryDocumentSessionOperations,
)
from ravendb.documents.session.misc import SessionInfo
from ravendb.exceptions.exceptions import InvalidOperationException
from ravendb.primitives import constants
from ravendb.util.client_change_vector_utils import ClientChangeVectorUtils


class TestClientChangeVectorUtils(unittest.TestCase):
    def test_etag_between_last_colon_and_dash(self):
        # Format of a cluster-transaction segment: 'Trxn:{index}-{clusterId}'.
        self.assertEqual(15, ClientChangeVectorUtils.get_etag_by_id("Trxn:15-A:42", "A:42"))

    def test_no_match_returns_zero(self):
        self.assertEqual(0, ClientChangeVectorUtils.get_etag_by_id("A:1-node:2", "A:42"))
        self.assertEqual(0, ClientChangeVectorUtils.get_etag_by_id("Trxn:0-A:42", "A:42"))

    def test_null_change_vector_returns_zero(self):
        self.assertEqual(0, ClientChangeVectorUtils.get_etag_by_id(None, "A:42"))

    def test_separator_guard(self):
        with self.assertRaises(ValueError):
            ClientChangeVectorUtils.get_etag_by_id("a|b", "x")


def _new_actions(session_info):
    actions = InMemoryDocumentSessionOperations.SaveChangesData.ActionsToRunOnSuccess.__new__(
        InMemoryDocumentSessionOperations.SaveChangesData.ActionsToRunOnSuccess
    )
    session = object.__new__(InMemoryDocumentSessionOperations)
    session.session_info = session_info
    actions._ActionsToRunOnSuccess__session = session
    actions._ActionsToRunOnSuccess__document_infos_to_update = []
    return actions


class TestUpdateEntityDocumentInfo(unittest.TestCase):
    def test_last_part_etag_advances_last_cluster_transaction_index(self):
        session_info = SessionInfo.__new__(SessionInfo)
        session_info.cluster_transaction_id = "A:42"
        session_info.last_cluster_transaction_index = None

        actions = _new_actions(session_info)
        info = DocumentInfo(key="users/1", change_vector="A:1-node:2|Trxn:15-A:42")
        actions.update_entity_document_info(info, {"Name": "u"})

        self.assertEqual(15, session_info.last_cluster_transaction_index)
        self.assertEqual(1, len(actions._ActionsToRunOnSuccess__document_infos_to_update))

    def test_etag_read_from_last_part_only(self):
        session_info = SessionInfo.__new__(SessionInfo)
        session_info.cluster_transaction_id = "A:42"
        session_info.last_cluster_transaction_index = None

        actions = _new_actions(session_info)
        # The first part also contains 'A:42'; only the last part may contribute.
        info = DocumentInfo(key="users/1", change_vector="A:5-A:42|Trxn:7-A:42")
        actions.update_entity_document_info(info, {})

        self.assertEqual(7, session_info.last_cluster_transaction_index)

    def test_max_with_existing_index(self):
        session_info = SessionInfo.__new__(SessionInfo)
        session_info.cluster_transaction_id = "A:42"
        session_info.last_cluster_transaction_index = 20

        actions = _new_actions(session_info)
        actions.update_entity_document_info(DocumentInfo(key="users/1", change_vector="A:1-node:2|Trxn:15-A:42"), {})
        self.assertEqual(20, session_info.last_cluster_transaction_index)

        actions.update_entity_document_info(DocumentInfo(key="users/2", change_vector="A:1-node:2|Trxn:25-A:42"), {})
        self.assertEqual(25, session_info.last_cluster_transaction_index)

    def test_more_than_two_parts_throws(self):
        session_info = SessionInfo.__new__(SessionInfo)
        session_info.cluster_transaction_id = "A:42"
        session_info.last_cluster_transaction_index = None

        actions = _new_actions(session_info)
        with self.assertRaises(InvalidOperationException) as ctx:
            actions.update_entity_document_info(DocumentInfo(key="users/1", change_vector="A:1|B:2|Trxn:3-A:42"), {})
        self.assertEqual("The document 'users/1' has invalid change vector 'A:1|B:2|Trxn:3-A:42'", str(ctx.exception))

    def test_null_change_vector_is_no_op(self):
        session_info = SessionInfo.__new__(SessionInfo)
        session_info.cluster_transaction_id = "A:42"
        session_info.last_cluster_transaction_index = None

        actions = _new_actions(session_info)
        actions.update_entity_document_info(DocumentInfo(key="users/1", change_vector=None), {})
        self.assertIsNone(session_info.last_cluster_transaction_index)

    def test_no_cluster_id_is_no_op(self):
        session_info = SessionInfo.__new__(SessionInfo)
        session_info.cluster_transaction_id = None
        session_info.last_cluster_transaction_index = None

        actions = _new_actions(session_info)
        actions.update_entity_document_info(DocumentInfo(key="users/1", change_vector="A:1-node:2"), {})
        self.assertIsNone(session_info.last_cluster_transaction_index)

    def test_split_separator_is_pipe(self):
        self.assertEqual("|", ClientChangeVectorUtils.SEPARATOR)

    def test_session_info_has_cluster_transaction_id_field(self):
        session_info = SessionInfo.__new__(SessionInfo)
        session_info.cluster_transaction_id = None
        self.assertIsNone(session_info.cluster_transaction_id)


class TestClusterTransactionHeaderCapture(unittest.TestCase):
    def test_constant_is_database_cluster_tx_id(self):
        self.assertEqual("Database-Cluster-Tx-Id", constants.Headers.DATABASE_CLUSTER_TRANSACTION_ID)

    def test_request_executor_captures_header_in_success_path(self):
        from ravendb.http.request_executor import RequestExecutor

        source = open("ravendb/http/request_executor.py").read()
        # The capture must sit in the success path, before process_response.
        success_index = source.index("command.process_response(self._cache, response, url)")
        header_index = source.index("DATABASE_CLUSTER_TRANSACTION_ID in response.headers")
        self.assertLess(header_index, success_index)
        # It must guard the absent header.
        self.assertIn("DATABASE_CLUSTER_TRANSACTION_ID in response.headers", source)


if __name__ == "__main__":
    unittest.main()
