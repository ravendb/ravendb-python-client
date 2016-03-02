from tools.generate_id import GenerateEntityIdOnTheClient
from store.advanced_session_operation import Advanced
from custom_exceptions import exceptions
from d_commands import commands_data
from tools.utils import Utils


class _SaveChangesData(object):
    def __init__(self, commands, deferred_command_count, entities=None):
        self.commands = commands
        self.entities = [] if entities is None else entities
        self.deferred_command_count = deferred_command_count


class DocumentSession(object):
    """
      Implements Unit of Work for accessing the RavenDB server

      @param database: the name of the database we open a session to
      :type str
      @param document_store: the store that we work on
      :type DocumentStore
      """

    def __init__(self, database, document_store):
        self.database = database
        self.document_store = document_store
        self._entities_by_key = {}
        self._deleted_entities = set()
        self._entities_and_metadata = {}
        self._known_missing_ids = set()
        self.id_value = None
        self._defer_commands = set()
        self._number_of_requests_in_session = 0
        self._advanced = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def conventions(self):
        return self.document_store.conventions

    @property
    def advanced(self):
        if self._advanced is None:
            self._advanced = Advanced(self)
        return self._advanced

    def save_entity(self, key, entity, original_metadata, metadata, document):
        self._known_missing_ids.discard(key)
        self._entities_by_key[key] = entity

        self._entities_and_metadata[self._entities_by_key[key]] = {
            "original_value": document.copy(), "metadata": metadata,
            "original_metadata": original_metadata, "etag": metadata.get("etag", None), "key": key}

    def _convert_and_save_entity(self, key, document, object_type):
        conventions = self.document_store.conventions
        entity, metadata, original_metadata = Utils.convert_to_entity(document, object_type, conventions)
        self.save_entity(key, entity, original_metadata, metadata, document)

    def _multi_load(self, keys, object_type):
        if len(keys) == 0:
            return []
        ids_of_not_existing_object = [key for key in keys if
                                      key not in self._known_missing_ids or key not in self._entities_by_key]

        if len(ids_of_not_existing_object) > 0:
            self._increment_requests_count()
            response = self.document_store.database_commands.get(keys)["Results"]
            for i in range(0, len(response)):
                if response[i] is None:
                    self._known_missing_ids.add(keys[i])
                    continue
                self._convert_and_save_entity(keys[i], response[i], object_type)
        return [None if key in self._known_missing_ids else self._entities_by_key[key] for key in keys]

    def load(self, key_or_keys, object_type=None):
        """
        @param key_or_keys: Identifier of a document that will be loaded.
        :type str or list
        @param object_type:the class we want to get
        :type classObj:
        @return: instance of object_type or None if document with given Id does not exist.
        :rtype:object_type or None
        """
        if not key_or_keys:
            raise ValueError("None or empty key is invalid")
        if isinstance(key_or_keys, list):
            return self._multi_load(key_or_keys, object_type)

        if key_or_keys in self._known_missing_ids:
            return None
        if key_or_keys in self._entities_by_key:
            return self._entities_by_key[key_or_keys]

        self._increment_requests_count()
        response = self.document_store.database_commands.get(key_or_keys)["Results"]
        if len(response) == 0 or response[0] is None:
            self._known_missing_ids.add(key_or_keys)
            return None
        self._convert_and_save_entity(key_or_keys, response[0], object_type)
        return self._entities_by_key[key_or_keys]

    def delete_by_entity(self, entity):
        if entity is None:
            raise ValueError("None entity is invalid")
        if entity not in self._entities_and_metadata:
            raise exceptions.InvalidOperationException(
                "{0} is not associated with the session, cannot delete unknown entity instance".format(entity))
        if "Raven-Read-Only" in self._entities_and_metadata[entity]["original_metadata"]:
            raise exceptions.InvalidOperationException(
                "{0} is marked as read only and cannot be deleted".format(entity))
        self._deleted_entities.add(entity)
        self._defer_commands.add(commands_data.DeleteCommandData(self._entities_and_metadata[entity]["key"]))
        self._known_missing_ids.add(self._entities_and_metadata[entity]["key"])

    def delete(self, key_or_entity):
        """
        @param key_or_entity:can be the key or the entity we like to delete
        :type str or object:
        """
        if key_or_entity is None:
            raise ValueError("None key is invalid")
        self._known_missing_ids.add(key_or_entity)
        if key_or_entity in self._entities_by_key or key_or_entity in self._entities_and_metadata:
            entity = self._entities_by_key[
                key_or_entity] if key_or_entity in self._entities_by_key else key_or_entity
            if self._has_change(entity):
                raise exceptions.InvalidOperationException(
                    "Can't delete changed entity using identifier. Use delete_by_entity(entity) instead.")
            if entity not in self._entities_and_metadata:
                raise exceptions.InvalidOperationException(
                    "{0} is not associated with the session, cannot delete unknown entity instance".format(entity))
            if "Raven-Read-Only" in self._entities_and_metadata[entity]["original_metadata"]:
                raise exceptions.InvalidOperationException(
                    "{0} is marked as read only and cannot be deleted".format(entity))
            self._deleted_entities.add(entity)
            return
        self._defer_commands.add(commands_data.DeleteCommandData(key_or_entity))

    def assert_no_non_unique_instance(self, entity, key):
        if not (key is None or key.endswith("/") or key not in self._entities_by_key or self._entities_by_key[
            key] is entity):
            raise exceptions.NonUniqueObjectException(
                "Attempted to associate a different object with id '{0}'.".format(key))

    def store(self, entity, key=None, etag=None, force_concurrency_check=False):
        """
        @param entity: Entity that will be stored
        :type object:
        @param key: Entity will be stored under this key, (None to generate automatically)
        :type str:
        @param etag: Current entity etag, used for concurrency checks (null to skip check)
        :type str
        @param force_concurrency_check:
        :type bool
        """
        if entity is None:
            raise ValueError("None entity value is invalid")
        if entity in self._entities_and_metadata:
            if etag is not None:
                self._entities_and_metadata["etag"] = etag
            return

        if key is None:
            entity_id = GenerateEntityIdOnTheClient.try_get_id_from_instance(entity)
        else:
            GenerateEntityIdOnTheClient.try_set_id_on_entity(entity, key)
            entity_id = key

        if entity in self._deleted_entities:
            raise exceptions.InvalidOperationException(
                "Can't store object, it was already deleted in this session.  Document id: " + key)
        self.assert_no_non_unique_instance(entity, entity_id)

        if not entity_id:
            entity_id = self.document_store.generate_id(entity)
            GenerateEntityIdOnTheClient.try_set_id_on_entity(entity, entity_id)

        metadata = self.conventions.build_default_metadata(entity)
        metadata["etag"] = etag

        self._entities_by_key[entity_id] = entity
        self._known_missing_ids.discard(entity_id)

        self._deleted_entities.discard(entity)
        self._entities_and_metadata[entity] = {"original_value": {}, "metadata": metadata,
                                               "original_metadata": {},
                                               "etag": etag,
                                               "key": entity_id,
                                               "force_concurrency_check": force_concurrency_check}

    def save_changes(self):
        data = _SaveChangesData(list(self._defer_commands), len(self._defer_commands))
        self._defer_commands.clear()
        self._prepare_for_delete_commands(data)
        self._prepare_for_puts_commands(data)
        if len(data.commands) == 0:
            return
        self._increment_requests_count()
        batch_result = self.document_store.database_commands.batch(data.commands)
        if batch_result is None:
            raise exceptions.InvalidOperationException(
                "Cannot call Save Changes after the document store was disposed.")
        self._update_batch_result(batch_result, data)

    def _update_batch_result(self, batch_result, data):

        i = data.deferred_command_count
        batch_result_length = len(batch_result)
        while i < batch_result_length:
            item = batch_result[i]
            if item["Method"] != "PUT":
                continue
            entity = data.entities[i - data.deferred_command_count]
            if entity not in self._entities_and_metadata:
                continue
            self._entities_by_key[item["Key"]] = entity
            document_metadata = self._entities_and_metadata[entity]
            document_metadata["etag"] = str(item["Etag"])
            document_metadata["original_metadata"] = item["Metadata"].copy()
            document_metadata["metadata"] = item["Metadata"]
            document_metadata["original_value"] = entity.__dict__
            i += 1

    def _prepare_for_delete_commands(self, data):
        keys_to_delete = []
        for entity in self._deleted_entities:
            keys_to_delete.append(self._entities_and_metadata[entity]["key"])

        for key in keys_to_delete:
            existing_entity = None
            etag = None
            if key in self._entities_by_key:
                existing_entity = self._entities_by_key[key]
                if existing_entity in self._entities_and_metadata:
                    etag = self._entities_and_metadata[existing_entity]["etag"]
                self._entities_and_metadata.pop(existing_entity, None)
                self._entities_by_key.pop(key, None)
            data.entities.append(existing_entity)
            data.commands.append(commands_data.DeleteCommandData(key, etag))
        self._deleted_entities.clear()

    def _prepare_for_puts_commands(self, data):
        for entity in self._entities_and_metadata:
            if self._has_change(entity):
                key = self._entities_and_metadata[entity]["key"]
                metadata = self._entities_and_metadata[entity]["metadata"]
                etag = self._entities_and_metadata[entity]["etag"]
                data.entities.append(entity)
                if key is not None:
                    self._entities_by_key.pop(key)
                data.commands.append(commands_data.PutCommandData(key, etag, entity.__dict__, metadata))

    def _has_change(self, entity):
        if self._entities_and_metadata[entity]["original_metadata"] != self._entities_and_metadata[entity][
            "metadata"] or self._entities_and_metadata[entity]["original_value"] != entity.__dict__:
            return True
        return False

    def _increment_requests_count(self):
        self._number_of_requests_in_session += 1
        if self._number_of_requests_in_session > self.conventions.max_number_of_request_per_session:
            raise exceptions.InvalidOperationException(
                "The maximum number of requests ({0}) allowed for this session has been reached. Raven limits the number \
                of remote calls that a session is allowed to make as an early warning system. Sessions are expected to \
                be short lived, and Raven provides facilities like batch saves (call save_changes() only once).\
                You can increase the limit by setting DocumentConvention.\
                MaxNumberOfRequestsPerSession or MaxNumberOfRequestsPerSession, but it is advisable \
                that you'll look into reducing the number of remote calls first, \
                since that will speed up your application significantly and result in a\
                more responsive application.".format(self.conventions.max_number_of_request_per_session))
