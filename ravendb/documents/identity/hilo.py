from __future__ import annotations
import datetime
from threading import Lock
from typing import Any, Union, Optional, Iterable, Dict, Tuple, Callable

from typing import TYPE_CHECKING

import ravendb.documents.commands.crud as commands_crud
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.documents import DocumentStore
    from ravendb.documents.conventions import DocumentConventions


class GenerateEntityIdOnTheClient:
    def __init__(self, conventions: "DocumentConventions", generate_id: Callable[[Any], str]):
        self.__conventions = conventions
        self.__generate_id = generate_id

    def try_get_id_from_instance(self, entity: object) -> Tuple[bool, Union[str, None]]:
        if not entity:
            raise ValueError("Entity cannot be None")
        identity_property = "Id"  # todo: make sure it's ok, create get_identity_property within conventions if not
        value = entity.__dict__.get(identity_property, None)
        if isinstance(value, str):
            return True, value
        return False, None

    def get_or_generate_document_id(self, entity) -> str:
        key = self.try_get_id_from_instance(entity)[1]
        if key is None:
            key = self.__generate_id(entity)

        if key and key.startswith("/"):
            raise ValueError(f"Cannot use value '{key}' as a document id because it begins with a '/'")

        return key

    def generate_document_key_for_storage(self, entity: object) -> str:
        key = self.get_or_generate_document_id(entity)
        self.try_set_identity(entity, key)
        return key

    def try_set_identity(self, entity: object, key: str, is_projection: bool = False) -> None:
        self.__try_set_identity_internal(entity, key, is_projection)

    def __try_set_identity_internal(self, entity: Union[object, dict], key: str, is_projection: bool = False) -> None:
        identity_property = "Id"  # todo: make sure it's ok, create get_identity_property...

        if identity_property is None:
            return

        if is_projection and entity.__getattribute__(identity_property):
            # identity property was already set
            return

        if type(entity) == dict:
            entity[identity_property] = key
            return
        entity.__setattr__(identity_property, key)


class MultiDatabaseHiLoGenerator:
    def __init__(self, store: DocumentStore):
        self._store = store
        self._generators: Dict[str, MultiTypeHiLoGenerator] = {}

    def generate_document_id(self, database: str, entity: object) -> str:
        database = self._store.get_effective_database(database)
        generator = self._generators.get(database, None)
        if generator is None:
            generator = self.generate_multi_type_hi_lo_func(database)
            self._generators[database] = generator
        return generator.generate_document_id(entity)

    def generate_multi_type_hi_lo_func(self, database: str) -> MultiTypeHiLoGenerator:
        return MultiTypeHiLoGenerator(self._store, database)

    def return_unused_range(self) -> None:
        for generator in self._generators.values():
            generator.return_unused_range()


class MultiTypeHiLoGenerator:
    def __init__(self, store: DocumentStore, db_name: str):
        self._store = store
        self._db_name = db_name
        self._conventions = store.get_request_executor(db_name).conventions
        self.__identity_parts_separator = self._conventions.identity_parts_separator

        self.__generator_lock = Lock()
        self.__id_generators_by_tag: Dict[str, HiLoIdGenerator] = {}

    def generate_document_id(self, entity: object) -> str:
        identity_parts_separator = self._conventions.identity_parts_separator
        if self.__identity_parts_separator != identity_parts_separator:
            self.__maybe_refresh(identity_parts_separator)

        type_tag_name = self._conventions.get_collection_name(entity)

        if len(type_tag_name) == 0:
            return None

        tag = self._conventions.transform_class_collection_name_to_document_id_prefix(type_tag_name)

        value = self.__id_generators_by_tag.get(tag)
        if value:
            return value.generate_document_id(entity)

        with self.__generator_lock:
            value = self.__id_generators_by_tag.get(tag)

            if value:
                return value.generate_document_id(entity)

            value = self._create_generator_for(tag)
            self.__id_generators_by_tag[tag] = value

        return value.generate_document_id(entity)

    def __maybe_refresh(self, identity_parts_separator: str) -> None:
        with self.__generator_lock:
            if self.__identity_parts_separator == identity_parts_separator:
                return
            id_generators = list(self.__id_generators_by_tag.values())
            self.__id_generators_by_tag.clear()
            self.__identity_parts_separator = identity_parts_separator

        if id_generators is not None:
            try:
                self.__return_unused_range(id_generators)
            except Exception:
                pass  # ignored

    def _create_generator_for(self, tag: str):
        return HiLoIdGenerator(tag, self._store, self._db_name, self.__identity_parts_separator)

    def return_unused_range(self) -> None:
        self.__return_unused_range(self.__id_generators_by_tag.values())

    @staticmethod
    def __return_unused_range(generators: Iterable[HiLoIdGenerator]) -> None:
        for generator in generators:
            generator.return_unused_range()


class HiLoIdGenerator:
    def __init__(self, tag: str, store: DocumentStore, db_name: str, identity_parts_separator: str):
        self.__store = store
        self.__tag = tag
        self.__db_name = db_name
        self.__identity_parts_separator = identity_parts_separator
        self.__range = self.RangeValue(1, 0)

        self.__generator_lock = Lock()

        self.__last_batch_size: Union[None, int] = None
        self.__last_range_date: Union[None, datetime.datetime] = None
        self._prefix: Union[None, str] = None
        self._server_tag: Union[None, str] = None

    def _get_document_id_from_id(self, next_id: int) -> str:
        return self._prefix + str(next_id) + "-" + (self._server_tag if self._server_tag else "")

    @property
    def range(self) -> RangeValue:
        return self.__range

    @range.setter
    def range(self, value: RangeValue):
        self.__range = value

    class RangeValue:
        def __init__(self, min_val: int, max_val: int):
            self.min_val = min_val
            self.max_val = max_val
            self.current = min_val - 1

    def generate_document_id(self, entity: object) -> str:
        return self._get_document_id_from_id(self.next_id())

    def next_id(self) -> int:
        while True:
            range = self.__range

            range.current += 1
            key = range.current

            if key <= range.max_val:
                return key

            # local range is exhausted, need to get a new range
            with self.__generator_lock:
                key = self.__range.current
                if key <= self.__range.max_val:
                    return key

                self.__get_next_range()

    def __get_next_range(self) -> None:
        hilo_command = commands_crud.NextHiLoCommand(
            self.__tag,
            self.__last_batch_size,
            self.__last_range_date,
            self.__identity_parts_separator,
            self.__range.max_val,
        )

        re = self.__store.get_request_executor(self.__db_name)
        re.execute_command(hilo_command)

        self._prefix = hilo_command.result.prefix
        self._server_tag = hilo_command.result.server_tag
        self.__last_range_date = hilo_command.result.last_range_at
        self.__last_batch_size = hilo_command.result.last_size
        self.__range = HiLoIdGenerator.RangeValue(hilo_command.result.low, hilo_command.result.high)

    def return_unused_range(self) -> None:
        return_command = commands_crud.HiLoReturnCommand(self.__tag, self.__range.current, self.__range.max_val)

        re = self.__store.get_request_executor(self.__db_name)
        re.execute_command(return_command)


class HiLoResult:
    def __init__(
        self,
        prefix: Optional[str] = None,
        low: Optional[int] = None,
        high: Optional[int] = None,
        last_size: Optional[int] = None,
        server_tag: Optional[str] = None,
        last_range_at: Optional[datetime.datetime] = None,
    ):
        self.prefix = prefix
        self.low = low
        self.high = high
        self.last_size = last_size
        self.server_tag = server_tag
        self.last_range_at = last_range_at

    @classmethod
    def from_json(cls, json_dict: dict) -> HiLoResult:
        return cls(
            json_dict["Prefix"],
            json_dict["Low"],
            json_dict["High"],
            json_dict["LastSize"],
            json_dict["ServerTag"],
            Utils.string_to_datetime(json_dict["LastRangeAt"]),
        )
