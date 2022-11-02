import http
import unittest

from ravendb import MultiGetOperation
from ravendb.documents.commands.multi_get import GetRequest
from ravendb.tests.test_base import TestBase
from ravendb.tools.utils import Utils


class TestObj:
    def __init__(self, Id: str = None, large_content: str = None):
        self.Id = Id
        self.large_content = large_content


class TestUseCachingInLazy(TestBase):
    def setUp(self):
        super(TestUseCachingInLazy, self).setUp()

    @unittest.skip("Aggressive caching in MultiGetCommand")
    def test_lazily_load__when_query_not_found_not_modified__should_use_cache(self):
        not_exists_doc_id = "NotExistDocId"

        with self.store.open_session() as session:
            # Add "NotExistDocId" to cache
            session.advanced.lazily.load(TestObj, not_exists_doc_id).value

        request_executor = self.store.get_request_executor()
        with self.store.open_session() as session:
            multi_get_operation = MultiGetOperation(session)
            get_request = GetRequest()
            get_request.url = "/docs"
            get_request.query = f"?&id={Utils.escape(not_exists_doc_id)}"

            requests = [get_request]

            with multi_get_operation.create_request(requests) as multi_get_command:
                # Should use the cache here and release it in after that
                request_executor.execute_command(multi_get_command)
                self.assertEqual(http.HTTPStatus.NOT_MODIFIED.real, multi_get_command.result[0].status_code)
