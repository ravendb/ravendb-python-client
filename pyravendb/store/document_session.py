from pyravendb.tools.generate_id import GenerateEntityIdOnTheClient
from pyravendb.custom_exceptions import exceptions
from pyravendb.store.session_query import Query
from pyravendb.d_commands import commands_data
from pyravendb.tools.utils import Utils
from collections import OrderedDict


class _SaveChangesData(object):
    def __init__(self, commands, deferred_command_count, entities=None):
        self.commands = commands
        self.entities = [] if entities is None else entities
        self.deferred_command_count = deferred_command_count


class documentsession(object):
    """
      Implements Unit of Work for accessing the RavenDB server

      @param database: the name of the database we open a session to
      :type str
      @param document_store: the store that we work on
      :type DocumentStore
      """

    def __init__(self, database, document_store, database_commands, session_id, force_read_from_master):
        self.session_id = session_id
        self.database = database
        self.document_store = document_store
        self.database_commands = database_commands
        self._entities_by_key = {}
        self._includes = {}
        self._deleted_entities = set()
        self._entities_and_metadata = {}
        self._known_missing_ids = set()
        self.id_value = None
        self._defer_commands = set()
        self._number_of_requests_in_session = 0
        self._query = None
        self.advanced = Advanced(self)
        self._force_read_from_master = force_read_from_master

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def number_of_requests_in_session(self):
        return self._number_of_requests_in_session

    @property
    def entities_and_metadata(self):
        return self._entities_and_metadata

    @property
    def conventions(self):
        return self.document_store.conventions

    @property
    def query(self):
        if self._query is None:
            self._query = Query(self)
        return self._query

    def save_includes(self, includes=None):
        if includes:
            for include in includes:
                if include["@metadata"]["@id"] not in self._entities_by_key:
                    self._includes[include["@metadata"]["@id"]] = include

    def save_entity(self, key, entity, original_metadata, metadata, document, force_concurrency_check=False):
        if key is not None:
            self._known_missing_ids.discard(key)

            if key not in self._entities_by_key:
                self._entities_by_key[key] = entity

                self._entities_and_metadata[self._entities_by_key[key]] = {
                    "original_value": document.copy(), "metadata": metadata,
                    "original_metadata": original_metadata, "etag": metadata.get("etag", None), "key": key,
                    "force_concurrency_check": force_concurrency_check}

    def _convert_and_save_entity(self, key, document, object_type, nested_object_types):
        if key not in self._entities_by_key:
            entity, metadata, original_metadata = Utils.convert_to_entity(document, object_type, self.conventions,
                                                                          nested_object_types)
            self.save_entity(key, entity, original_metadata, metadata, document)

    def _multi_load(self, keys, object_type, includes, nested_object_types):
        if len(keys) == 0:
            return []

        ids_of_not_existing_object = keys
        if not includes:
            ids_in_includes = [key for key in keys if key in self._includes]
            if len(ids_in_includes) > 0:
                for include in ids_in_includes:
                    self._convert_and_save_entity(include, self._includes[include], object_type, nested_object_types)
                    self._includes.pop(include)

            ids_of_not_existing_object = [key for key in keys if
                                          key not in self._entities_by_key]

        # We need to remove duplicate ids in here for our index lookup strategy below to work
        # because RavenDB is smart enough to not return duplicate results
        ids_of_not_existing_object = list(OrderedDict.fromkeys([key for key in ids_of_not_existing_object if key not in self._known_missing_ids]))

        if len(ids_of_not_existing_object) > 0:
            self.increment_requests_count()
            response = self.database_commands.get(ids_of_not_existing_object, includes)
            if response:
                results = response["Results"]
                response_includes = response["Includes"]
                for i in range(0, len(results)):
                    if results[i] is None:
                        self._known_missing_ids.add(ids_of_not_existing_object[i])
                        continue
                    self._convert_and_save_entity(keys[i], results[i], object_type, nested_object_types)
                self.save_includes(response_includes)
        return [None if key in self._known_missing_ids else self._entities_by_key[
            key] if key in self._entities_by_key else None for key in keys]

    def load(self, key_or_keys, object_type=None, includes=None, nested_object_types=None):
        """
        @param key_or_keys: Identifier of a document that will be loaded.
        :type str or list
        @param includes: The path to a reference inside the loaded documents can be list (property name)
        :type list or str
        @param object_type: The class we want to get
        :type classObj:
        @param nested_object_types: A dict of classes for nested object the key will be the name of the class and the
         value will be the object we want to get for that attribute
        :type str
        @return: instance of object_type or None if document with given Id does not exist.
        :rtype:object_type or None
        """
        if not key_or_keys:
            raise ValueError("None or empty key is invalid")
        if includes and not isinstance(includes, list):
            includes = [includes]

        if isinstance(key_or_keys, list):
            return self._multi_load(key_or_keys, object_type, includes, nested_object_types)

        if key_or_keys in self._known_missing_ids:
            return None
        if key_or_keys in self._entities_by_key and not includes:
            return self._entities_by_key[key_or_keys]

        if key_or_keys in self._includes:
            self._convert_and_save_entity(key_or_keys, self._includes[key_or_keys], object_type,
                                          nested_object_types)
            self._includes.pop(key_or_keys)
            if not includes:
                return self._entities_by_key[key_or_keys]

        self.increment_requests_count()
        response = self.database_commands.get(key_or_keys, includes=includes)
        if response:
            result = response["Results"]
            response_includes = response["Includes"]
            if len(result) == 0 or result[0] is None:
                self._known_missing_ids.add(key_or_keys)
                return None
            self._convert_and_save_entity(key_or_keys, result[0], object_type, nested_object_types)
            self.save_includes(response_includes)
        return self._entities_by_key[key_or_keys] if key_or_keys in self._entities_by_key else None

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
        self._known_missing_ids.add(self._entities_and_metadata[entity]["key"])

    def delete(self, key_or_entity):
        """
        @param key_or_entity:can be the key or the entity we like to delete
        :type str or object:
        """
        if key_or_entity is None:
            raise ValueError("None key is invalid")
        if not isinstance(key_or_entity, str):
            self.delete_by_entity(key_or_entity)
        self._known_missing_ids.add(key_or_entity)
        if key_or_entity in self._entities_by_key:
            entity = self._entities_by_key[key_or_entity]
            if self._has_change(entity):
                raise exceptions.InvalidOperationException(
                    "Can't delete changed entity using identifier. Use delete_by_entity(entity) instead.")
            if entity not in self._entities_and_metadata:
                raise exceptions.InvalidOperationException(
                    "{0} is not associated with the session, cannot delete unknown entity instance".format(entity))
            if "Raven-Read-Only" in self._entities_and_metadata[entity]["original_metadata"]:
                raise exceptions.InvalidOperationException(
                    "{0} is marked as read only and cannot be deleted".format(entity))
            self.delete_by_entity(entity)
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
                self._entities_and_metadata[entity]["etag"] = etag
            self._entities_and_metadata[entity]["force_concurrency_check"] = force_concurrency_check
            return

        if key is None:
            entity_id = GenerateEntityIdOnTheClient.try_get_id_from_instance(entity)
        else:
            GenerateEntityIdOnTheClient.try_set_id_on_entity(entity, key)
            entity_id = key

        self.assert_no_non_unique_instance(entity, entity_id)

        if not entity_id:
            entity_id = self.document_store.generate_id(entity)
            GenerateEntityIdOnTheClient.try_set_id_on_entity(entity, entity_id)

        for command in self._defer_commands:
            if command.key == entity_id:
                raise exceptions.InvalidOperationException(
                    "Can't store document, there is a deferred command registered for this document in the session. "
                    "Document id: " + entity_id)

        if entity in self._deleted_entities:
            raise exceptions.InvalidOperationException(
                "Can't store object, it was already deleted in this session.  Document id: " + entity_id)

        metadata = self.conventions.build_default_metadata(entity)
        metadata["etag"] = etag
        self._deleted_entities.discard(entity)
        self.save_entity(entity_id, entity, {}, metadata, {}, force_concurrency_check=force_concurrency_check)

    def save_changes(self):
        data = _SaveChangesData(list(self._defer_commands), len(self._defer_commands))
        self._defer_commands.clear()
        self._prepare_for_delete_commands(data)
        self._prepare_for_puts_commands(data)
        if len(data.commands) == 0:
            return
        self.increment_requests_count()
        batch_result = self.database_commands.batch(data.commands)
        if batch_result is None:
            raise exceptions.InvalidOperationException(
                "Cannot call Save Changes after the document store was disposed.")
        self._update_batch_result(batch_result, data)

    def _update_batch_result(self, batch_result, data):

        i = data.deferred_command_count
        batch_result_length = len(batch_result)
        while i < batch_result_length:
            item = batch_result[i]
            if item["Method"] == "PUT":
                entity = data.entities[i - data.deferred_command_count]
                if entity in self._entities_and_metadata:
                    self._entities_by_key[item["Key"]] = entity
                    document_metadata = self._entities_and_metadata[entity]
                    document_metadata["etag"] = str(item["Etag"])
                    document_metadata["original_metadata"] = item["Metadata"].copy()
                    document_metadata["metadata"] = item["Metadata"]
                    document_metadata["original_value"] = entity.__dict__.copy()
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
                    etag = self._entities_and_metadata[existing_entity]["metadata"][
                        "@etag"] if self.advanced.use_optimistic_concurrency else None
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
                etag = None
                if self.advanced.use_optimistic_concurrency or self._entities_and_metadata[entity][
                    "force_concurrency_check"]:
                    etag = self._entities_and_metadata[entity]["etag"] or metadata.get("@etag", Utils.empty_etag())
                data.entities.append(entity)
                if key is not None:
                    self._entities_by_key.pop(key)
                    document = entity.__dict__.copy()
                    document.pop('Id', None)
                data.commands.append(commands_data.PutCommandData(key, etag, document, metadata))

    def _has_change(self, entity):
        if self._entities_and_metadata[entity]["original_metadata"] != self._entities_and_metadata[entity]["metadata"] \
                or self._entities_and_metadata[entity]["original_value"] != entity.__dict__:
            return True
        return False

    def increment_requests_count(self):
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


class Advanced(object):
    def __init__(self, session):
        self.session = session
        self.use_optimistic_concurrency = session.conventions.default_use_optimistic_concurrency

    def number_of_requests_in_session(self):
        return self.session.number_of_requests_in_session

    def get_document_id(self, instance):
        if instance is not None:
            if instance in self.session.entities_and_metadata:
                return self.session.entities_and_metadata[instance]["key"]
        return None
