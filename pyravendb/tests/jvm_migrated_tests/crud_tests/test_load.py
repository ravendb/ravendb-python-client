from pyravendb.tests.test_base import TestBase, User, UserWithId


class GeekUser(User):
    def __init__(self, name, age, favorite_primes, favorite_very_large_primes):
        super().__init__(name, age)
        self.favorite_primes = favorite_primes
        self.favorite_very_large_primes = favorite_very_large_primes


class Foo:
    def __init__(self, name=None):
        self.name = name


class Bar:
    def __init__(self, foo_id=None, foo_ids=None, name=None, bar_id=None):
        self.foo_id = foo_id
        self.foo_ids = foo_ids
        self.name = name
        self.Id = bar_id


class TestLoad(TestBase):
    def setUp(self):
        super(TestLoad, self).setUp()

    def test_load_document_by_id(self):
        with self.store.open_session() as session:
            session.store(User("RavenDB", None), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            self.assertIsNotNone(user)
            self.assertEqual(user.name, "RavenDB")

    def test_load_documents_by_ids(self):
        with self.store.open_session() as session:
            session.store(User("RavenDB", None), "users/1")
            session.store(User("Hibernating Rhinos", None), "users/2")
            session.save_changes()

        with self.store.open_session() as session:
            users = session.load(["users/1", "users/2"], User)
            self.assertEqual(len(users), 2)

    def test_load_document_with_int_array_and_long_array(self):
        # Python 3.x has eliminated long and is only having int.
        # sys.maxsize contains the maximum size in bytes a Python int can be.
        with self.store.open_session() as session:
            session.store(
                GeekUser("Beep", 10, [13, 43, 443, 997], [5000000029, 5000000039]),
                "geeks/1",
            )
            session.store(GeekUser("Bop", 90, [2, 3, 5, 7], [999999999989]), "geeks/2")
            session.save_changes()

        with self.store.open_session() as session:
            geek1 = session.load("geeks/1", GeekUser)
            geek2 = session.load("geeks/2", GeekUser)
            self.assertEqual(geek1.favorite_primes[1], 43)
            self.assertEqual(geek1.favorite_very_large_primes[1], 5000000039)

            self.assertEqual(geek2.favorite_primes[3], 7)
            self.assertEqual(geek2.favorite_very_large_primes[0], 999999999989)

    def test_should_load_many_ids_as_post_request(self):
        ids = []
        with self.store.open_session() as session:
            # Length of all the ids together should be larger than 1024 for POST request
            for i in range(200):
                identifier = f"users/{i}"
                ids.append(identifier)
                session.store(UserWithId(f"Person {i}", None), identifier)
            session.save_changes()

        with self.store.open_session() as session:
            users = session.load(ids, UserWithId)
            self.assertEqual(200, len(users))
            for user in users:
                self.assertIsNotNone(user)

    def test_load_can_use_cache(self):
        with self.store.open_session() as session:
            session.store(User("RavenDB", None), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            self.assertIsNotNone(user)

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            self.assertIsNotNone(user)

    def test_load_null_should_return_null(self):
        with self.store.open_session() as session:
            session.store(User("Tony Montana", None))
            session.store(User("Tony Soprano", None))
            session.save_changes()

        with self.store.open_session() as session:
            user1 = session.load(None, User)
            self.assertIsNone(user1)

    def test_load_multi_ids_with_null_should_return_dictionary_without_nulls(self):
        with self.store.open_session() as session:
            session.store(UserWithId("Tony Montana", None), "users/1")
            session.store(UserWithId("Tony Soprano", None), "users/2")
            session.save_changes()

        with self.store.open_session() as session:
            users_arr = ["users/1", None, "users/2", None]  # jvm - String[]
            users_by_id_1 = dict([(user.Id, user) for _, user in session.load(users_arr).items()])

            users_set = list({"users/1", None, "users/2", None})  # jvm - HashSet(Arrays.asList(...))
            users_by_id_2 = dict([(user.Id, user) for _, user in session.load(users_set).items()])

            self.assertIsNotNone(users_by_id_1["users/1"])
            self.assertIsNotNone(users_by_id_1["users/2"])
            self.assertEqual(len(users_by_id_2), 2)

    def test_load_with_includes(self):
        bar_id = None
        with self.store.open_session() as session:
            foo = Foo("Beginning")
            session.store(foo)

            fid = session.get_document_id(foo)
            bar = Bar(name="End", foo_id=fid)
            session.store(bar)

            bar_id = session.get_document_id(bar)
            session.save_changes()

        with self.store.open_session() as session:
            bar = session.load([bar_id], Bar, includes=lambda x: x.include_documents("foo_id"))
            self.assertIsNotNone(bar)
            self.assertEqual(1, len(bar))
            self.assertIsNotNone(bar[0].Id)

            num_of_reqs = session.advanced.number_of_requests_in_session()
            foo = session.load(bar[0].foo_id, Foo)
            self.assertIsNotNone(foo)
            self.assertEqual(foo.name, "Beginning")
            self.assertEqual(session.advanced.number_of_requests_in_session(), num_of_reqs)
