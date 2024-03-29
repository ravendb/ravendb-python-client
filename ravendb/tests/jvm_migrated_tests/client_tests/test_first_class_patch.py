from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from ravendb.tests.test_base import TestBase
from ravendb.tools.utils import Utils


@dataclass
class Pet:
    name: str = None
    kind: str = None

    @classmethod
    def from_json(cls, json_dict: Dict[str, str]) -> Pet:
        return cls(json_dict["name"], json_dict["kind"])


@dataclass
class Friend:
    name: str = None
    age: int = None
    pet: Pet = None

    @classmethod
    def from_json(cls, json_dict: Dict) -> Friend:
        return cls(
            json_dict["name"],
            int(json_dict["age"]) if json_dict["age"] is not None else None,
            Pet.from_json(json_dict["pet"]) if json_dict["pet"] is not None else None,
        )


@dataclass
class Stuff:
    key: int = None
    phone: str = None
    pet: Pet = None
    friend: Friend = None
    dic: Dict[str] = None

    @classmethod
    def from_json(cls, json_dict: Dict) -> Stuff:
        return cls(
            int(json_dict["key"]) if json_dict["key"] is not None else None,
            json_dict["phone"],
            Pet.from_json(json_dict["pet"]) if json_dict["pet"] is not None else None,
            Friend.from_json(json_dict["friend"]) if json_dict["friend"] is not None else None,
            json_dict["dic"],
        )


@dataclass
class User:
    stuff: List[Stuff] = None
    last_login: datetime = None
    numbers: List[int] = None

    @classmethod
    def from_json(cls, json_dict: Dict) -> User:
        return cls(
            (
                [Stuff.from_json(jdict) if jdict is not None else None for jdict in json_dict["stuff"]]
                if json_dict["stuff"] is not None
                else None
            ),
            Utils.string_to_datetime(json_dict["last_login"]),
            (
                [int(number) if number is not None else None for number in json_dict["numbers"]]
                if json_dict["numbers"] is not None
                else None
            ),
        )


class TestFirstClassPatch(TestBase):
    def setUp(self):
        super(TestFirstClassPatch, self).setUp()
        self.doc_id = "users/1-A"

    def test_can_patch(self):
        stuff = [Stuff(), None, None]
        stuff[0].key = 6

        user = User(numbers=[66], stuff=stuff)

        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        now = datetime.now()

        with self.store.open_session() as session:
            session.advanced.patch(self.doc_id, "numbers[0]", 31)
            session.advanced.patch(self.doc_id, "last_login", now)
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            self.assertEqual(31, loaded.numbers[0])
            self.assertEqual(now, loaded.last_login)

            session.advanced.patch(loaded, "stuff[0].phone", "123456")
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            self.assertEqual("123456", loaded.stuff[0].phone)

    def test_can_patch_and_modify(self):
        user = User(numbers=[66])

        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            loaded.numbers[0] = 1
            session.advanced.patch(loaded, "numbers[0]", 2)
            with self.assertRaises(RuntimeError):
                session.save_changes()

    def test_can_patch_complex(self):
        stuff = [Stuff(key=6), None, None]

        user = User(stuff=stuff)

        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            new_stuff = Stuff(key=4, phone="9255864406", friend=Friend())
            session.advanced.patch(self.doc_id, "stuff[1]", new_stuff)
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)

            self.assertEqual("9255864406", loaded.stuff[1].phone)
            self.assertEqual(4, loaded.stuff[1].key)
            self.assertIsNotNone(loaded.stuff[1].friend)

            pet1 = Pet(kind="Dog", name="Hanan")
            friends_pet = Pet("Miriam", "Cat")
            friend = Friend("Gonras", 28, friends_pet)
            second_stuff = Stuff(4, "9255864406", pet1, friend)
            map_ = {"Ohio": "Columbus", "Utah": "Salt Lake City", "Texas": "Austin", "California": "Sacramento"}

            second_stuff.dic = map_

            session.advanced.patch(loaded, "stuff[2]", second_stuff)
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            self.assertEqual("Hanan", loaded.stuff[2].pet.name)
            self.assertEqual("Gonras", loaded.stuff[2].friend.name)
            self.assertEqual("Miriam", loaded.stuff[2].friend.pet.name)
            self.assertEqual(4, loaded.stuff[2].key)
            self.assertEqual("Salt Lake City", loaded.stuff[2].dic.get("Utah"))

    def test_can_add_to_array(self):
        stuff = [Stuff(key=6)]
        user = User(stuff, numbers=[1, 2])

        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            # push
            session.advanced.patch_array(self.doc_id, "numbers", lambda roles: roles.add(3))
            session.advanced.patch_array(self.doc_id, "stuff", lambda roles: roles.add(Stuff(75)))
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)

            self.assertEqual(3, loaded.numbers[2])
            self.assertEqual(75, loaded.stuff[1].key)

            # concat

            session.advanced.patch_array(loaded, "numbers", lambda roles: roles.add(101, 102, 103))
            session.advanced.patch_array(
                loaded, "stuff", lambda roles: roles.add(Stuff(102)).add(Stuff(phone="123456"))
            )
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            self.assertEqual(6, len(loaded.numbers))
            self.assertEqual(103, loaded.numbers[5])

            self.assertEqual(102, loaded.stuff[2].key)
            self.assertEqual("123456", loaded.stuff[3].phone)

            session.advanced.patch_array(loaded, "numbers", lambda roles: roles.add(201, 202, 203))
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            self.assertEqual(9, len(loaded.numbers))
            self.assertEqual(202, loaded.numbers[7])

    def test_can_remove_from_array(self):
        stuff = [Stuff(6), Stuff(phone="123456")]
        user = User(stuff, numbers=[1, 2, 3])

        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            session.advanced.patch_array(self.doc_id, "numbers", lambda roles: roles.remove_at(1))
            session.advanced.patch_array(self.doc_id, "stuff", lambda roles: roles.remove_at(0))
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            self.assertEqual(2, len(loaded.numbers))
            self.assertEqual(3, loaded.numbers[1])

            self.assertEqual(1, len(loaded.stuff))
            self.assertEqual("123456", loaded.stuff[0].phone)

    def test_can_increment(self):
        s = [Stuff(6), None, None]
        user = User(s, numbers=[66])

        with self.store.open_session() as session:
            session.store(user)
            session.save_changes()

        with self.store.open_session() as session:
            session.advanced.increment(self.doc_id, "numbers[0]", 1)
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            self.assertEqual(67, loaded.numbers[0])

            session.advanced.increment(loaded, "stuff[0].key", -3)
            session.save_changes()

        with self.store.open_session() as session:
            loaded = session.load(self.doc_id, User)
            self.assertEqual(3, loaded.stuff[0].key)

    def test_should_merge_patch_calls(self):
        stuff = [Stuff(6), None, None]

        user = User(stuff, numbers=[66])
        user2 = User(stuff, numbers=[1, 2, 3])

        docid2 = "users/2-A"

        with self.store.open_session() as session:
            session.store(user)
            session.store(user2, docid2)
            session.save_changes()

        now = datetime.utcnow()

        with self.store.open_session() as session:
            session.advanced.patch(self.doc_id, "numbers[0]", 31)
            self.assertEqual(1, len(session._deferred_commands))
            session.advanced.patch(self.doc_id, "last_login", now)
            self.assertEqual(1, len(session._deferred_commands))
            session.advanced.patch(docid2, "numbers[0]", 123)
            self.assertEqual(2, len(session._deferred_commands))
            session.advanced.patch(docid2, "last_login", now)

            session.save_changes()

        with self.store.open_session() as session:
            session.advanced.increment(self.doc_id, "numbers[0]", 1)
            self.assertEqual(1, len(session._deferred_commands))
            session.advanced.patch_array(self.doc_id, "numbers", lambda r: r.add(77))
            self.assertEqual(1, len(session._deferred_commands))
            session.advanced.patch_array(self.doc_id, "numbers", lambda r: r.add(88))
            self.assertEqual(1, len(session._deferred_commands))
            session.advanced.patch_array(self.doc_id, "numbers", lambda r: r.remove_at(1))
            self.assertEqual(1, len(session._deferred_commands))

            session.save_changes()
