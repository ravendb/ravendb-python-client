import os
from abc import abstractmethod
from typing import List


class RavenServerLocator:
    ENV_SERVER_PATH = "RAVENDB_PYTHON_TEST_SERVER_PATH"

    @abstractmethod
    def get_server_path(self) -> str:
        path = os.environ[self.ENV_SERVER_PATH]
        if path.isspace():
            raise RuntimeError(
                "Unable to find RavenDB server path. "
                f"Please make sure {self.ENV_SERVER_PATH} environment variable is set and is valid "
                f"(current value = {path})"
            )

        return path

    @property
    def command(self) -> str:
        return self.get_server_path()

    @property
    def command_arguments(self) -> List[str]:
        return []

    @property
    def certificate_path(self) -> str:
        raise RuntimeError()

    @property
    def server_ca_path(self) -> str:
        raise RuntimeError()
