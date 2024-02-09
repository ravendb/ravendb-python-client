import subprocess
from typing import TYPE_CHECKING, Tuple
from ravendb_embedded.embedded_server import EmbeddedServer
from ravendb.documents.store.definition import DocumentStore
from ravendb.infrastructure.graph import Genre, Movie, User
from ravendb.tests.driver.raven_server_runner import RavenServerRunner

if TYPE_CHECKING:
    from ravendb.tests.driver.raven_server_locator import RavenServerLocator


class RavenTestDriver:
    debug = False

    def __init__(self):
        self._disposed = False

    @property
    def disposed(self) -> bool:
        return self._disposed

    @staticmethod
    def _run_embedded_server_internal(locator: "RavenServerLocator") -> Tuple[DocumentStore, EmbeddedServer]:
        embedded_server = RavenServerRunner.get_embedded_server(locator)
        store = embedded_server.get_document_store("test.manager")
        store.conventions.disable_topology_updates = True

        return store, embedded_server

    @staticmethod
    def _kill_process(p: subprocess.Popen) -> None:
        RavenTestDriver._report_info("Kill global server")
        p.kill()

    @staticmethod
    def _report_info(message: str) -> None:
        pass

    def _setup_database(self, document_store: DocumentStore) -> None:
        pass  # empty by design

    @staticmethod
    def _create_movies_data(store: DocumentStore) -> None:
        with store.open_session() as session:
            scifi = Genre("genres/1", "Sci-Fi")
            fantasy = Genre("genres/2", "Fantasy")
            adventure = Genre("genres/3", "Adventure")

            session.store(scifi)
            session.store(fantasy)
            session.store(adventure)

            star_wars = Movie("movies/1", "Star Wars Ep.1", ["genres/1", "genres/2"])
            firefly = Movie("movies/2", "Firefly Serenity", ["genres/2", "genres/3"])
            indiana_jones = Movie("movies/3", "Indiana Jones and the Temple Of Doom", ["genres/3"])

            session.store(star_wars)
            session.store(firefly)
            session.store(indiana_jones)

            rating11 = User.Rating("movies/1", 5)
            rating12 = User.Rating("movies/2", 7)
            rating21 = User.Rating("movies/2", 7)
            rating22 = User.Rating("movies/3", 9)
            rating31 = User.Rating("movies/3", 5)
            user1 = User("users/1", "Jack", has_rated=[rating11, rating12])
            user2 = User("users/2", "Jill", has_rated=[rating21, rating22])
            user3 = User("users/3", "Bob", has_rated=[rating31])

            session.store(user1)
            session.store(user2)
            session.store(user3)
            session.save_changes()
