from datetime import datetime, timedelta
from typing import Optional

from ravendb.documents.session.loaders.include import TimeSeriesIncludeBuilder
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper

document_id = "users/gracjan"
company_id = "companies/1-A"
order_id = "orders/1-A"
base_line = datetime(2023, 8, 20, 21, 30)
ts_name1 = "Heartrate"
ts_name2 = "Speedrate"
tag1 = "watches/fitbit"
tag2 = "watches/apple"
tag3 = "watches/sony"


class Watch:
    def __init__(self, name: Optional[str] = None, accuracy: Optional[float] = None):
        self.name = name
        self.accuracy = accuracy


class TestRavenDB14164(TestBase):
    def setUp(self):
        super(TestRavenDB14164, self).setUp()

    def test_can_get_time_series_with_include_tag_documents(self):
        tags = [tag1, tag2, tag3]
        with self.store.open_session() as session:
            session.store(User(), document_id)

            session.store(Watch("FitBit", 0.855), tags[0])
            session.store(Watch("Apple", 0.9), tags[1])
            session.store(Watch("Sony", 0.78), tags[2])
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name1)
            for i in range(121):
                tsf.append_single(base_line + timedelta(minutes=i), i, tags[i % 3])
            session.save_changes()

        with self.store.open_session() as session:
            get_results = session.time_series_for(document_id, ts_name1).get_with_include(
                base_line, base_line + timedelta(hours=2), lambda i: i.include_tags()
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(121, len(get_results))
            self.assertEqual(base_line, get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(hours=2), get_results[-1].timestamp)

            # should not go to server
            tag_documents = session.load(tags, Watch)
            self.assertEqual(1, session.advanced.number_of_requests)

            # assert tag documents

            self.assertEqual(3, len(tag_documents))

            tag_doc = tag_documents.get("watches/fitbit")
            self.assertEqual("FitBit", tag_doc.name)
            self.assertEqual(0.855, tag_doc.accuracy)

            tag_doc = tag_documents.get("watches/apple")
            self.assertEqual("Apple", tag_doc.name)
            self.assertEqual(0.9, tag_doc.accuracy)

            tag_doc = tag_documents.get("watches/sony")
            self.assertEqual("Sony", tag_doc.name)
            self.assertEqual(0.78, tag_doc.accuracy)

    def test_can_get_time_series_with_include_tags_and_parent_document(self):
        tags = [tag1, tag2, tag3]
        with self.store.open_session() as session:
            session.store(User(name="poisson"), document_id)
            session.store(Watch("FitBit", 0.855), tags[0])
            session.store(Watch("Apple", 0.9), tags[1])
            session.store(Watch("Sony", 0.78), tags[2])
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name1)
            for i in range(121):
                tsf.append_single(base_line + timedelta(minutes=i), i, tags[i % 3])
            session.save_changes()

        with self.store.open_session() as session:
            get_results = session.time_series_for(document_id, ts_name1).get_with_include(
                base_line, base_line + timedelta(hours=2), lambda i: i.include_tags().include_document()
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(121, len(get_results))
            self.assertEqual(base_line, get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(hours=2), get_results[-1].timestamp)

            # should not go to server
            user = session.load(document_id, User)
            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual("poisson", user.name)

            # should not go to server
            tag_documents = session.load(tags, Watch)
            self.assertEqual(1, session.advanced.number_of_requests)

            # assert tag documents

            self.assertEqual(3, len(tag_documents))

            tag_doc = tag_documents.get("watches/fitbit")
            self.assertEqual("FitBit", tag_doc.name)
            self.assertEqual(0.855, tag_doc.accuracy)

            tag_doc = tag_documents.get("watches/apple")
            self.assertEqual("Apple", tag_doc.name)
            self.assertEqual(0.9, tag_doc.accuracy)

            tag_doc = tag_documents.get("watches/sony")
            self.assertEqual("Sony", tag_doc.name)
            self.assertEqual(0.78, tag_doc.accuracy)

    def test_includes_should_affect_time_series_get_command_etag(self):
        tags = [tag1, tag2, tag3]
        with self.store.open_session() as session:
            session.store(User(name="poisson"), document_id)
            session.store(Watch("FitBit", 0.855), tags[0])
            session.store(Watch("Apple", 0.9), tags[1])
            session.store(Watch("Sony", 0.78), tags[2])
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name1)
            for i in range(121):
                tsf.append_single(base_line + timedelta(minutes=i), i, tags[i % 3])
            session.save_changes()

        with self.store.open_session() as session:
            get_results = session.time_series_for(document_id, ts_name1).get_with_include(
                base_line, base_line + timedelta(hours=2), lambda i: i.include_tags()
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(121, len(get_results))
            self.assertEqual(base_line, get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(hours=2), get_results[-1].timestamp)

            # should not go to server
            tag_documents = session.load(tags, Watch)
            self.assertEqual(1, session.advanced.number_of_requests)

            # assert tag documents

            self.assertEqual(3, len(tag_documents))

            tag_doc = tag_documents.get("watches/fitbit")
            self.assertEqual("FitBit", tag_doc.name)
            self.assertEqual(0.855, tag_doc.accuracy)

            tag_doc = tag_documents.get("watches/apple")
            self.assertEqual("Apple", tag_doc.name)
            self.assertEqual(0.9, tag_doc.accuracy)

            tag_doc = tag_documents.get("watches/sony")
            self.assertEqual("Sony", tag_doc.name)
            self.assertEqual(0.78, tag_doc.accuracy)

        with self.store.open_session() as session:
            # update tags[0]
            watch = session.load(tags[0], Watch)
            watch.accuracy += 0.05
            session.save_changes()

        with self.store.open_session() as session:
            get_results = session.time_series_for(document_id, ts_name1).get_with_include(
                base_line, base_line + timedelta(hours=2), lambda i: i.include_tags()
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(121, len(get_results))
            self.assertEqual(base_line, get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(hours=2), get_results[-1].timestamp)
            # should not go to server

            tag_documents = session.load(tags, Watch)
            self.assertEqual(1, session.advanced.number_of_requests)

            # assert tag documents

            self.assertEqual(3, len(tag_documents))

            tag_doc = tag_documents.get("watches/fitbit")
            self.assertEqual("FitBit", tag_doc.name)
            self.assertEqual(0.905, tag_doc.accuracy)

        new_tag = "watches/google"

        with self.store.open_session() as session:
            session.store(Watch("Google Watch", 0.75), new_tag)
            # update a time series entry to have the new tag

            session.time_series_for(document_id, ts_name1).append_single(base_line + timedelta(minutes=45), 90, new_tag)
            session.save_changes()

        with self.store.open_session() as session:
            get_results = session.time_series_for(document_id, ts_name1).get_with_include(
                base_line, base_line + timedelta(hours=2), lambda i: i.include_tags()
            )
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(121, len(get_results))
            self.assertEqual(base_line, get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(hours=2), get_results[-1].timestamp)

            # should not go to server
            tag_documents = session.load(tags, Watch)
            self.assertEqual(1, session.advanced.number_of_requests)

            # assert that new tag is in cache
            watch = session.load(new_tag, Watch)
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("Google Watch", watch.name)
            self.assertEqual(0.75, watch.accuracy)

    def test_can_get_time_series_with_include_cache_not_empty(self):
        tags = [tag1, tag2, tag3]
        with self.store.open_session() as session:
            session.store(User(name="poisson"), document_id)
            session.store(Watch("FitBit", 0.855), tags[0])
            session.store(Watch("Apple", 0.9), tags[1])
            session.store(Watch("Sony", 0.78), tags[2])
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name1)
            for i in range(121):
                tag = tags[0 if i < 60 else 1 if i < 90 else 2]
                tsf.append_single(base_line + timedelta(minutes=i), i, tag)

            session.save_changes()

        with self.store.open_session() as session:
            # get [21:30 - 22:30]
            get_results = session.time_series_for(document_id, ts_name1).get(base_line, base_line + timedelta(hours=1))

            self.assertEqual(61, len(get_results))
            self.assertEqual(base_line, get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(hours=1), get_results[-1].timestamp)

            # get [22:45 - 23:30] with includes
            get_results = session.time_series_for(document_id, ts_name1).get_with_include(
                base_line + timedelta(minutes=75), base_line + timedelta(hours=2), TimeSeriesIncludeBuilder.include_tags
            )

            self.assertEqual(2, session.advanced.number_of_requests)

            self.assertEqual(46, len(get_results))
            self.assertEqual(base_line + timedelta(minutes=75), get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(hours=2), get_results[-1].timestamp)

            # should not go to server

            tags_documents = session.load(tags[1:3], Watch)
            self.assertEqual(2, session.advanced.number_of_requests)

            # assert tag documents
            self.assertEqual(2, len(tags_documents))

            tag_doc = tags_documents.get("watches/apple")
            self.assertEqual("Apple", tag_doc.name)
            self.assertEqual(0.9, tag_doc.accuracy)

            tag_doc = tags_documents.get("watches/sony")
            self.assertEqual("Sony", tag_doc.name)
            self.assertEqual(0.78, tag_doc.accuracy)

            # watches/fitbit should not be in cache
            watch = session.load(tags[0], Watch)
            self.assertEqual(3, session.advanced.number_of_requests)
            self.assertEqual("FitBit", watch.name)
            self.assertEqual(0.855, watch.accuracy)

    def test_can_get_time_series_with_include_tags_when_not_all_entries_have_tags(self):
        tags = [tag1, tag2, tag3]
        with self.store.open_session() as session:
            session.store(User(name="poisson"), document_id)
            session.store(Watch("FitBit", 0.855), tags[0])
            session.store(Watch("Apple", 0.9), tags[1])
            session.store(Watch("Sony", 0.78), tags[2])
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name1)
            for i in range(121):
                tsf.append_single(base_line + timedelta(minutes=i), i, tags[i % 3])
            session.save_changes()

        with self.store.open_session() as session:
            get_results = session.time_series_for(document_id, ts_name1).get_with_include(
                base_line, base_line + timedelta(hours=2), lambda i: i.include_tags()
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(121, len(get_results))
            self.assertEqual(base_line, get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(hours=2), get_results[-1].timestamp)

            # should not go to server
            tag_documents = session.load(tags, Watch)
            self.assertEqual(1, session.advanced.number_of_requests)

            # assert tag documents

            self.assertEqual(3, len(tag_documents))

            tag_doc = tag_documents.get("watches/fitbit")
            self.assertEqual("FitBit", tag_doc.name)
            self.assertEqual(0.855, tag_doc.accuracy)

            tag_doc = tag_documents.get("watches/apple")
            self.assertEqual("Apple", tag_doc.name)
            self.assertEqual(0.9, tag_doc.accuracy)

            tag_doc = tag_documents.get("watches/sony")
            self.assertEqual("Sony", tag_doc.name)
            self.assertEqual(0.78, tag_doc.accuracy)

    def test_can_get_time_series_with_include_cache_not_empty_2(self):
        tags = [tag1, tag2, tag3]
        with self.store.open_session() as session:
            session.store(User(name="poisson"), document_id)
            session.store(Watch("FitBit", 0.855), tags[0])
            session.store(Watch("Apple", 0.9), tags[1])
            session.store(Watch("Sony", 0.78), tags[2])
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name1)
            for i in range(121):
                tag = tags[0 if i < 60 else 1 if i < 90 else 2]
                tsf.append_single(base_line + timedelta(minutes=i), i, tag)

            session.save_changes()

        with self.store.open_session() as session:
            # get [21:30 - 22:30]
            get_results = session.time_series_for(document_id, ts_name1).get(base_line, base_line + timedelta(hours=1))

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(61, len(get_results))
            self.assertEqual(base_line, get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(hours=1), get_results[-1].timestamp)

            # get [23:00 - 23:30] with includes
            get_results = session.time_series_for(document_id, ts_name1).get(
                base_line + timedelta(minutes=90), base_line + timedelta(hours=2)
            )

            self.assertEqual(2, session.advanced.number_of_requests)

            self.assertEqual(31, len(get_results))
            self.assertEqual(base_line + timedelta(minutes=90), get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(hours=2), get_results[-1].timestamp)

            # get [22:30 - 22:45] with includes
            get_results = session.time_series_for(document_id, ts_name1).get_with_include(
                base_line + timedelta(hours=1),
                base_line + timedelta(minutes=75),
                lambda builder: builder.include_tags(),
            )

            self.assertEqual(3, session.advanced.number_of_requests)

            self.assertEqual(16, len(get_results))
            self.assertEqual(base_line + timedelta(hours=1), get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=75), get_results[-1].timestamp)

            # should not go to server
            watch = session.load(tags[1], Watch)
            self.assertEqual(3, session.advanced.number_of_requests)

            self.assertEqual("Apple", watch.name)

            self.assertEqual(0.9, watch.accuracy)

            # tags[0] and tags[2] should not be in cache

            watch = session.load(tags[0], Watch)
            self.assertEqual(4, session.advanced.number_of_requests)

            self.assertEqual("FitBit", watch.name)
            self.assertEqual(0.855, watch.accuracy)

            watch = session.load(tags[2], Watch)
            self.assertEqual(5, session.advanced.number_of_requests)
            self.assertEqual("Sony", watch.name)
            self.assertEqual(0.78, watch.accuracy)

    def test_can_get_multiple_ranges_with_includes(self):
        tags = [tag1, tag2, tag3]
        with self.store.open_session() as session:
            session.store(User(name="poisson"), document_id)
            session.store(Watch("FitBit", 0.855), tags[0])
            session.store(Watch("Apple", 0.9), tags[1])
            session.store(Watch("Sony", 0.78), tags[2])
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name1)
            for i in range(121):
                tsf.append_single(base_line + timedelta(minutes=i), i, tags[i % 3])
            session.save_changes()

        with self.store.open_session() as session:
            # get [21:30 - 22:00]
            get_results = session.time_series_for(document_id, ts_name1).get(
                base_line, base_line + timedelta(minutes=30)
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(31, len(get_results))
            self.assertEqual(base_line, get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=30), get_results[-1].timestamp)

            # get [22:15 - 22:30]
            get_results = session.time_series_for(document_id, ts_name1).get(
                base_line + timedelta(minutes=45), base_line + timedelta(minutes=60)
            )

            self.assertEqual(2, session.advanced.number_of_requests)

            self.assertEqual(16, len(get_results))
            self.assertEqual(base_line + timedelta(minutes=45), get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=60), get_results[-1].timestamp)

            # get [22:15 - 22:30]
            get_results = session.time_series_for(document_id, ts_name1).get(
                base_line + timedelta(minutes=90), base_line + timedelta(minutes=120)
            )

            self.assertEqual(3, session.advanced.number_of_requests)

            self.assertEqual(31, len(get_results))
            self.assertEqual(base_line + timedelta(minutes=90), get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=120), get_results[-1].timestamp)

            # get multiple ranges with includes
            # ask for entire range [00:00 - 02:00] with includes
            # this will go to server to get the "missing parts" - [00:30 - 00:45] and [01:00 - 01:30]

            get_results = session.time_series_for(document_id, ts_name1).get_with_include(
                base_line, base_line + timedelta(minutes=120), lambda x: x.include_tags().include_document()
            )

            self.assertEqual(4, session.advanced.number_of_requests)

            self.assertEqual(121, len(get_results))
            self.assertEqual(base_line + timedelta(minutes=0), get_results[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=120), get_results[-1].timestamp)

            # should not go to server
            user = session.load(document_id, User)
            self.assertEqual(4, session.advanced.number_of_requests)
            self.assertEqual("poisson", user.name)

            # should not go to server
            tag_documents = session.load(tags, Watch)
            self.assertEqual(4, session.advanced.number_of_requests)

            # assert tag documents

            self.assertEqual(3, len(tag_documents))

            tag_doc = tag_documents.get("watches/fitbit")
            self.assertEqual("FitBit", tag_doc.name)
            self.assertEqual(0.855, tag_doc.accuracy)

            tag_doc = tag_documents.get("watches/apple")
            self.assertEqual("Apple", tag_doc.name)
            self.assertEqual(0.9, tag_doc.accuracy)

            tag_doc = tag_documents.get("watches/sony")
            self.assertEqual("Sony", tag_doc.name)
            self.assertEqual(0.78, tag_doc.accuracy)
