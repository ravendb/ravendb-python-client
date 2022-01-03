import os
import subprocess
import time
import io
from typing import Callable

from pyravendb.documents import DocumentStore
from pyravendb.tests.driver.raven_server_locator import RavenServerLocator
from pyravendb.tests.driver.raven_server_runner import RavenServerRunner


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
            read_lines.append(line)  # check if utf-8 works fine

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
