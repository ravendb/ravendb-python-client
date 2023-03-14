from ravendb.tests.test_base import TestBase, Company, User


class TestRavenDB15080(TestBase):
    def setUp(self):
        super(TestRavenDB15080, self).setUp()

    def test_can_delete_and_re_insert_counter(self):
        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1")
            session.counters_for_entity(company).increment("Likes", 999)
            session.counters_for_entity(company).increment("Cats", 999)
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            session.counters_for_entity(company).delete("Likes")
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            counters = session.advanced.get_counters_for(company)
            self.assertEqual(1, len(counters))
            self.assertIn("Cats", counters)

            counter = session.counters_for_entity(company).get("Likes")
            self.assertIsNone(counter)

            all = session.counters_for_entity(company).get_all()

            self.assertEqual(1, len(all))

        with self.store.open_session() as session:
            session.counters_for("companies/1").increment("Likes")
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            counters = session.advanced.get_counters_for(company)

            self.assertEqual(2, len(counters))
            self.assertSequenceContainsElements(counters, "Cats", "Likes")

            counter = session.counters_for_entity(company).get("Likes")
            self.assertIsNotNone(counter)
            self.assertEqual(1, counter)

    def test_can_split_lower_cased_and_upper_cased_counter_names(self):
        with self.store.open_session() as session:
            user = User("Aviv1")
            session.store(user, "users/1")

            counters_for = session.counters_for("users/1")

            for i in range(500):
                string = f"abc{i}"
                counters_for.increment(string)

            session.save_changes()

        with self.store.open_session() as session:
            counters_for = session.counters_for("users/1")

            for i in range(500):
                string = f"Xyz{i}"
                counters_for.increment(string)

            session.save_changes()

    def test_counters_should_be_case_insensitive(self):
        # RavenDB-14753

        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1")
            session.counters_for_entity(company).increment("Likes", 999)
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            session.counters_for_entity(company).delete("lIkEs")
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            counters = session.counters_for_entity(company).get_all()
            self.assertEqual(0, len(counters))

    def test_deleted_counter_should_not_be_present_in_metadata_counters(self):
        # RavenDB-14753

        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1")
            session.counters_for_entity(company).increment("Likes", 999)
            session.counters_for_entity(company).increment("Cats", 999)

            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            session.counters_for_entity(company).delete("lIkEs")
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            company.name = "RavenDB"
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)
            counters = session.advanced.get_counters_for(company)
            self.assertEqual(1, len(counters))
            self.assertIn("Cats", counters)

    def test_counter_operations_should_be_case_insensitive_to_counter_name(self):
        with self.store.open_session() as session:
            user = User("Aviv")
            session.store(user, "users/1")

            session.counters_for("users/1").increment("abc")
            session.save_changes()

        with self.store.open_session() as session:
            # should NOT create a new counter
            session.counters_for("users/1").increment("ABc")
            session.save_changes()

        with self.store.open_session() as session:
            self.assertEqual(1, len(session.counters_for("users/1").get_all()))

        with self.store.open_session() as session:
            # get should be case-insensitive to counter name

            val = session.counters_for("users/1").get("AbC")
            self.assertEqual(2, val)

            doc = session.load("users/1", User)
            counter_names = session.advanced.get_counters_for(doc)
            self.assertEqual(1, len(counter_names))
            self.assertEqual("abc", counter_names[0])  # metadata counter-names should preserve their original casing

        with self.store.open_session() as session:
            session.counters_for("users/1").increment("XyZ")
            session.save_changes()

        with self.store.open_session() as session:
            val = session.counters_for("users/1").get("xyz")
            self.assertEqual(1, val)

            doc = session.load("users/1", User)
            counter_names = session.advanced.get_counters_for(doc)
            self.assertEqual(2, len(counter_names))

            # metadata counter-names should preserve their original casing
            self.assertEqual("abc", counter_names[0])
            self.assertEqual("XyZ", counter_names[1])

        with self.store.open_session() as session:
            # delete should be case-insensitive to counter name

            session.counters_for("users/1").delete("aBC")
            session.save_changes()

        with self.store.open_session() as session:
            val = session.counters_for("users/1").get("abc")
            self.assertIsNone(val)

            doc = session.load("users/1", User)
            counter_names = session.advanced.get_counters_for(doc)
            self.assertEqual(1, len(counter_names))
            self.assertIn("XyZ", counter_names)

        with self.store.open_session() as session:
            session.counters_for("users/1").delete("xyZ")
            session.save_changes()

        with self.store.open_session() as session:
            val = session.counters_for("users/1").get("Xyz")
            self.assertIsNone(val)

            doc = session.load("users/1", User)
            counter_names = session.advanced.get_counters_for(doc)
            self.assertIsNone(counter_names)

    def test_counter_session_cache_should_be_case_insensitive_to_counter_name(self):
        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1")
            session.counters_for(company).increment("Likes", 333)
            session.counters_for(company).increment("Cats", 999)
            session.save_changes()

        with self.store.open_session() as session:
            company = session.load("companies/1", Company)

            # the document is now tracked by the session
            # so now counters-cache has access to '@counters' from metadata

            # searching for the counter's name in '@counters' should be done in a case insensitive manner
            # counter name should be found in '@counters' => go to server

            counter = session.counters_for_entity(company).get("liKes")
            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertIsNotNone(counter)
            self.assertEqual(333, counter)

            counter = session.counters_for_entity(company).get("cats")
            self.assertEqual(3, session.advanced.number_of_requests)
            self.assertIsNotNone(counter)
            self.assertEqual(999, counter)

            counter = session.counters_for_entity(company).get("caTS")
            self.assertEqual(3, session.advanced.number_of_requests)
            self.assertIsNotNone(counter)
            self.assertEqual(999, counter)

    def test_get_counters_for_document_should_return_names_in_their_original_casing(self):
        with self.store.open_session() as session:
            session.store(User(), "users/1")
            counters_for = session.counters_for("users/1")
            counters_for.increment("AviV")
            counters_for.increment("Karmel")
            counters_for.increment("PAWEL")

            session.save_changes()

        with self.store.open_session() as session:
            # get_all should return counter names in their original casing
            all_counters = session.counters_for("users/1").get_all()
            self.assertEqual(3, len(all_counters))

            keys = all_counters.keys()
            self.assertSequenceContainsElements(keys, "AviV", "Karmel", "PAWEL")
