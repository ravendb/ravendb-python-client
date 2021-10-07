from collections import Callable
from typing import Any, Union

from pyravendb.data.document_conventions import DocumentConventions


class GenerateEntityIdOnTheClient:
    def __init__(self, conventions: DocumentConventions, generate_id: Callable[[Any], str]):
        self.__conventions = conventions
        self.__generate_id = generate_id

    def try_get_id_from_instance(self, entity: object) -> tuple[bool, Union[str, None]]:
        if not entity:
            raise ValueError("Entity cannot be None")
        identity_property = "Id"  # todo: make sure it's ok, create get_identity_property within conventions if not
        value = entity.__getattribute__(identity_property)
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

    def __try_set_identity_internal(self, entity: object, key: str, is_projection: bool = False) -> None:
        identity_property = "Id"  # todo: make sure it's ok, create get_identity_property...

        if identity_property is None:
            return

        if is_projection and entity.__getattribute__(identity_property):
            # identity property was already set
            return

        entity.__setattr__(identity_property, key)
