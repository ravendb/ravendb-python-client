from pyravendb.tests.test_base import TestBase


class RegexMe:
    def __init__(self, text):
        self.text = text


class TestRegexQuery(TestBase):
    def setUp(self):
        super(TestRegexQuery, self).setUp()

    def test_queries_with_regex_from_document_query(self):
        with self.store.open_session() as session:
            session.store(RegexMe("I love dogs and cats"))
            session.store(RegexMe("I love cats"))
            session.store(RegexMe("I love dogs"))
            session.store(RegexMe("I love bats"))
            session.store(RegexMe("dogs love me"))
            session.store(RegexMe("cats love me"))
            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(RegexMe).where_regex("text", "^[a-z ]{2,4}love")
            iq = query.get_index_query()
            self.assertEqual("FROM RegexMes WHERE regex(text, $p0)", iq.query)
            self.assertEqual("^[a-z ]{2,4}love", iq.query_parameters.get("p0"))
            result = list(query)
            self.assertEqual(4, len(result))
