from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.server_operations import CreateDatabaseOperation
from pyravendb.raven_operations.admin_operations import PutIndexesOperation
from pyravendb.data.indexes import IndexDefinition
from requests.exceptions import RequestException
from pyravendb.tools.utils import Utils
from pyravendb.tools.utils import _DynamicStructure
from xxhash import xxh64
from enum import Enum
import timeit
from datetime import timedelta


class User:
    def __init__(self, name, age, dog):
        self.name = name
        self.age = age
        self.dog = dog


class Dog:
    def __init__(self, name, brand):
        self.name = name
        self.brand = brand


if __name__ == "__main__":
    with DocumentStore(urls=["http://localhost.fiddler:8080"], database="python_2") as store:
        create_database_operation = CreateDatabaseOperation(database_name="python_2")
        try:
            store.admin.server.send(create_database_operation)
        except Exception as e:
            print(e)
        store.initialize()
        with store.open_session() as session:
            for i in range(0, 100):
                dog = Dog("fazi" + str(i), "chi" + str(i + 2))
                user = User("Idan" + str(i), i, dog)
                session.store(dog)
                session.store(user)
            session.save_changes()

        index_map = "from doc in docs.Users select new" \
                    "{name = doc.name," \
                    "age = doc.age," \
                    "dog = doc.dog}"
        store.admin.send(PutIndexesOperation(IndexDefinition("AllUsers", index_map=index_map)))

        with store.open_session() as session:
            q, stats = list(
                session.query(object_type=User, with_statistics=True, index_name="AllUsers",
                              wait_for_non_stale_results=True, timeout=timedelta(seconds=10),
                              nested_object_types={"dog": Dog}).where_starts_with("name", "I"))
            query = list(
                session.query(object_type=dict, index_name="AllUsers").raw_query("from index 'AllUsers' where age=29"))
            for i in query:
                print(i)
