import os
import subprocess
import time
from typing import Callable

from ravendb.documents.store.definition import DocumentStore
from ravendb.infrastructure.graph import Genre, Movie, User
from ravendb.tests.driver.raven_server_locator import RavenServerLocator
from ravendb.tests.driver.raven_server_runner import RavenServerRunner


class RavenTestDriver:
    debug = False

    def __init__(self):
        self._disposed = False

    @property
    def disposed(self) -> bool:
        return self._disposed

    def _run_server_internal(
        self, locator: RavenServerLocator, configure_store: Callable[[DocumentStore], None]
    ) -> (DocumentStore, subprocess.Popen):
        process = RavenServerRunner.run(locator)
        self._report_info("Starting global server")
        url = None
        stdout = process.stdout
        startup_duration = time.perf_counter()
        read_lines = []
        while True:
            line = stdout.readline().decode("utf-8")
            read_lines.append(line)

            if line is None:
                raise RuntimeError(str.join(os.linesep, read_lines) + process.stdin.read().decode("utf-8"))

            if time.perf_counter() - startup_duration > 60:
                break

            prefix = "Server available on: "
            if line.startswith(prefix):
                url = line[len(prefix) :].rstrip()
                break

        if url is None:
            self._report_info("Url is None")
            try:
                process.kill()
            except Exception as e:
                self._report_error(e)

            raise RuntimeError("Unable to start server")

        store = DocumentStore([url], "test.manager")
        store.conventions.disable_topology_updates = True

        if configure_store is not None:
            configure_store(store)

        return store.initialize(), process

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
