"""Live-server integration tests for AI conversation messages (C1/C6).

Asserts the live-verified fact: a missing conversation answers HTTP 404 with an
empty body, and the operation surfaces result None (never an exception).
Skipped when no server is reachable at localhost:8081.
"""

import os
import unittest

import requests

from ravendb.documents.store.definition import DocumentStore

_LIVE_SERVER_URL = os.environ.get("RAVENDB_LIVE_SERVER_URL", "http://localhost:8081")
_DATABASE = "conversation-messages-live-db"


def _server_reachable() -> bool:
    try:
        response = requests.get(f"{_LIVE_SERVER_URL}/build/version", timeout=3)
        return response.status_code == 200 and '"7.2.5"' in response.text
    except Exception:
        return False


@unittest.skipUnless(_server_reachable(), f"no live 7.2.5 server at {_LIVE_SERVER_URL}")
class TestConversationMessagesLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        requests.put(f"{_LIVE_SERVER_URL}/admin/databases?name={_DATABASE}", json={"DatabaseName": _DATABASE})

    @classmethod
    def tearDownClass(cls):
        requests.delete(f"{_LIVE_SERVER_URL}/admin/databases?name={_DATABASE}&hard-delete=true")

    def setUp(self):
        self.store = DocumentStore(urls=[_LIVE_SERVER_URL], database=_DATABASE)
        self.store.initialize()

    def tearDown(self):
        self.store.close()

    def test_missing_conversation_answers_404_with_empty_body(self):
        response = requests.get(
            f"{_LIVE_SERVER_URL}/databases/{_DATABASE}/ai/agent/conversation/messages"
            "?conversationId=chats/missing&pageSize=10&detailLevel=Simple"
        )
        self.assertEqual(404, response.status_code)
        self.assertEqual(0, len(response.content))

    def test_missing_conversation_result_is_none(self):
        result = self.store.ai.get_conversation_messages("chats/missing")
        self.assertIsNone(result)

    def test_response_carries_database_cluster_tx_id_header(self):
        response = requests.get(
            f"{_LIVE_SERVER_URL}/databases/{_DATABASE}/ai/agent/conversation/messages"
            "?conversationId=chats/missing&pageSize=10&detailLevel=Simple"
        )
        self.assertIsNotNone(response.headers.get("Database-Cluster-Tx-Id"))


if __name__ == "__main__":
    unittest.main()
