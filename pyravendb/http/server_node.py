from enum import Enum
from typing import Optional


class ServerNode:
    class Role(Enum):
        NONE = "None"
        PROMOTABLE = "Promotable"
        MEMBER = "Member"
        REHAB = "Rehab"

        def __str__(self):
            return self.value

    def __init__(
        self,
        url: str,
        database: Optional[str] = None,
        cluster_tag: Optional[str] = None,
        server_role: Optional[Role] = None,
    ):
        self.url = url
        self.database = database
        self.cluster_tag = cluster_tag
        self.server_role = server_role
        self.__last_server_version_check = 0
        self.__last_server_version: str = None

    def __eq__(self, other) -> bool:
        if self == other:
            return True
        if other is None or type(self) != type(other):
            return False
        if self.url != other.url if self.url is not None else other.url is not None:
            return False
        return self.database == other.database if self.database is not None else other.database is None

    def __hash__(self) -> int:
        result = self.url.__hash__() if self.url else 0
        result = 31 * result + self.database.__hash__() if self.database is not None else 0
        return result

    @property
    def last_server_version(self) -> str:
        return self.__last_server_version

    def should_update_server_version(self) -> bool:
        if self.last_server_version is None or self.__last_server_version_check > 100:
            return True

        self.__last_server_version_check += 1
        return False

    def update_server_version(self, server_version: str):
        self.__last_server_version = server_version
        self.__last_server_version_check = 0

    def discard_server_version(self) -> None:
        self.__last_server_version_check = None
        self.__last_server_version_check = 0
