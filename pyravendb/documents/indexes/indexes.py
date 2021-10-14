from __future__ import annotations
import string
from abc import abstractmethod
from typing import Union


class AbstractCommonApiForIndexes:
    @abstractmethod
    def __init__(self):
        self.__additional_sources: Union[None, dict[str, str]] = None
        self.__additional_assemblies: Union[None, set[AdditionalAssembly]] = None
        self.__configuration = IndexConfiguration()

    @property
    def is_map_reduce(self) -> bool:
        return False

    @property
    def index_name(self) -> str:
        return type(self).__name__.replace("_", "/")

    @property
    def additional_sources(self) -> dict[str, str]:
        return self.__additional_sources

    @additional_sources.setter
    def additional_sources(self, value: dict[str, str]):
        self.__additional_sources = value

    @property
    def additional_assemblies(self):
        return self.__additional_assemblies

    @additional_assemblies.setter
    def additional_assemblies(self, value: set[AdditionalAssembly]):
        self.__additional_assemblies = value

    @property
    def configuration(self) -> IndexConfiguration:
        return self.__configuration

    @configuration.setter
    def configuration(self, value: IndexConfiguration):
        self.__configuration = value


class AdditionalAssembly:
    def __init__(
        self,
        assembly_name: str = None,
        assembly_path: str = None,
        package_name: str = None,
        package_version: str = None,
        package_source_url: str = None,
        usings: set[str] = None,
    ):
        self.assembly_name = assembly_name
        self.assembly_path = assembly_path
        self.package_name = package_name
        self.package_version = package_version
        self.package_source_url = package_source_url
        self.usings = usings

    @staticmethod
    def only_usings(usings: set[str]) -> AdditionalAssembly:
        if not usings:
            raise ValueError("Using cannot be None or empty")
        return AdditionalAssembly(usings=usings)

    @staticmethod
    def from_runtime(assembly_name: str, usings: set[str] = None) -> AdditionalAssembly:
        if not assembly_name or assembly_name.isspace():
            raise ValueError("assembly_name cannot be None or whitespace.")
        return AdditionalAssembly(assembly_name=assembly_name, usings=usings)

    @staticmethod
    def from_path(assembly_path: str, usings: set[str] = None) -> AdditionalAssembly:
        if not assembly_path or assembly_path.isspace():
            raise ValueError("assembly_path cannot be None or whitespace")
        return AdditionalAssembly(assembly_path=assembly_path, usings=usings)

    @staticmethod
    def from_nuget(
        package_name: str, package_version: str, package_source_url: str = None, usings: set[str] = None
    ) -> AdditionalAssembly:
        if not package_name or package_name.isspace():
            raise ValueError("package_name cannot be None or whitespace")
        if not package_version or package_version.isspace():
            raise ValueError("package_version cannot be None or whitespace")

        return AdditionalAssembly(
            package_name=package_name,
            package_version=package_version,
            package_source_url=package_source_url,
            usings=usings,
        )


class IndexConfiguration(dict):
    pass
