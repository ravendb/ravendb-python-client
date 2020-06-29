from pyravendb.tools.generate_id import GenerateEntityIdOnTheClient
from pyravendb.store.session_query import Query
from pyravendb.commands import commands_data
from pyravendb.commands.raven_commands import *
from pyravendb.raven_operations.operations import GetAttachmentOperation
from pyravendb.commands.commands_data import PutAttachmentCommandData, DeleteAttachmentCommandData, DeleteCommandData
from pyravendb.tools.utils import Utils
from pyravendb.data.operation import AttachmentType
from pyravendb.data.timeseries import TimeSeriesRangeResult
from .session_timeseries import TimeSeries
from .session_counters import DocumentCounters
from typing import Dict, List


class _SaveChangesData(object):
    def __init__(self, commands, deferred_command_count, entities=None):
        self.commands = commands
        self.entities = [] if entities is None else entities
        self.deferred_command_count = deferred_command_count


class DocumentSession(object):
    def __init__(self, database, document_store, requests_executor, session_id, **kwargs):
        """
        Implements Unit of Work for accessing the RavenDB server

        @param str database: the name of the database we open a session to
        @param DocumentStore document_store: the store that we work on
        """
        self.session_id = session_id
        self.database = database
        self._document_store = document_store
        self._requests_executor = requests_executor
        self._documents_by_id = {}
        self._included_documents_by_id = {}
        self._deleted_entities = set()
        self._documents_by_entity = {}
        self._timeseries_defer_commands = {}
        self._time_series_by_document_id = {}
        self._counters_defer_commands = {}
        self._counters_by_document_id = {}
        self._known_missing_ids = set()
        self.id_value = None
        self._defer_commands = set()
        self._number_of_requests_in_session = 0
        self._query = None
        self.use_optimistic_concurrency = kwargs.get("use_optimistic_concurrency", False)
        self.no_tracking = kwargs.get("no_tracking", False)
        self.no_caching = kwargs.get("no_caching", False)
        self.advanced = Advanced(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def requests_executor(self):
        return self._requests_executor

    @property
    def number_of_requests_in_session(self):
        return self._number_of_requests_in_session

    @property
    def documents_by_entity(self):
        return self._documents_by_entity

    @property
    def deleted_entities(self):
        return self._deleted_entities

    @property
    def documents_by_id(self):
        return self._documents_by_id

    @property
    def time_series_by_document_id(self) -> Dict[str, Dict[str, List[TimeSeriesRangeResult]]]:
        return self._time_series_by_document_id

    @property
    def counters_by_document_id(self):
        return self._counters_by_document_id

    def defer(self, command, *args):
        self._defer_commands.add(command)
        for arg in args:
            self._defer_commands.add(arg)

    @property
    def defer_commands(self):
        return self._defer_commands

    @property
    def counters_defer_commands(self):
        return self._counters_defer_commands

    @property
    def timeseries_defer_commands(self):
        return self._timeseries_defer_commands

    @property
    def known_missing_ids(self):
        return self._known_missing_ids

    @property
    def included_documents_by_id(self):
        return self._included_documents_by_id

    @property
    def conventions(self):
        return self._document_store.conventions

    @property
    def query(self):
        if self._query is None:
            self._query = Query(self)
        return self._query

    def save_includes(self, includes=None):
        if includes:
            for key, value in includes.items():
                if key not in self._documents_by_id:
                    self._included_documents_by_id[key] = value

    def save_entity(self, key, entity, original_metadata, metadata, original_document, force_concurrency_check=False):
        if key is not None:
            self._known_missing_ids.discard(key)

            if key not in self._documents_by_id:
                self._documents_by_id[key] = entity

                self._documents_by_entity[self._documents_by_id[key]] = {
                    "original_value": original_document, "metadata": metadata,
                    "original_metadata": original_metadata, "change_vector": metadata.get("change_vector", None),
                    "key": key, "force_concurrency_check": force_concurrency_check}

    def _convert_and_save_entity(self, key, document, object_type, nested_object_types):

        if key not in self._documents_by_id:
            entity, metadata, original_metadata, original_document = Utils.convert_to_entity(document, object_type,
                                                                                             self.conventions,
                                                                                             nested_object_types)
            self.save_entity(key, entity, original_metadata, metadata, original_document)

    def _multi_load(self, keys, object_type, includes, nested_object_types):
        if len(keys) == 0:
            return []

        ids_of_not_existing_object = set(keys)
        if not includes:
            ids_in_includes = [key for key in ids_of_not_existing_object if key in self._included_documents_by_id]
            if len(ids_in_includes) > 0:
                for include in ids_in_includes:
                    self._convert_and_save_entity(include, self._included_documents_by_id[include], object_type,
                                                  nested_object_types)
                    self._included_documents_by_id.pop(include)

            ids_of_not_existing_object = [key for key in ids_of_not_existing_object if
                                          key not in self._documents_by_id]

        ids_of_not_existing_object = [key for key in ids_of_not_existing_object if key not in self._known_missing_ids]

        if len(ids_of_not_existing_object) > 0:
            self.increment_requests_count()
            command = GetDocumentCommand(ids_of_not_existing_object, includes)
            response = self._requests_executor.execute(command)
            if response:
                results = response["Results"]
                includes = response["Includes"]
                for i in range(0, len(results)):
                    if results[i] is None:
                        self._known_missing_ids.add(ids_of_not_existing_object[i])
                        continue
                    self._convert_and_save_entity(ids_of_not_existing_object[i], results[i], object_type,
                                                  nested_object_types)
                self.save_includes(includes)
        return [None if key in self._known_missing_ids else self._documents_by_id[
            key] if key in self._documents_by_id else None for key in keys]

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
        if key_or_keys in self._documents_by_id and not includes:
            return self._documents_by_id[key_or_keys]

        if key_or_keys in self._included_documents_by_id:
            self._convert_and_save_entity(key_or_keys, self._included_documents_by_id[key_or_keys], object_type,
                                          nested_object_types)
            self._included_documents_by_id.pop(key_or_keys)
            if not includes:
                return self._documents_by_id[key_or_keys]

        self.increment_requests_count()
        command = GetDocumentCommand(key_or_keys, includes=includes)
        response = self._requests_executor.execute(command)
        if response:
            result = response["Results"]
            includes = response["Includes"]
            if len(result) == 0 or result[0] is None:
                self._known_missing_ids.add(key_or_keys)
                return None
            self._convert_and_save_entity(key_or_keys, result[0], object_type, nested_object_types)
            self.save_includes(includes)
        return self._documents_by_id[key_or_keys] if key_or_keys in self._documents_by_id else None

    def delete_by_entity(self, entity):
        if entity is None:
            raise ValueError("None entity is invalid")
        if entity not in self._documents_by_entity:
            raise exceptions.InvalidOperationException(
                "{0} is not associated with the session, cannot delete unknown entity instance".format(entity))
        if "Raven-Read-Only" in self._documents_by_entity[entity]["original_metadata"]:
            raise exceptions.InvalidOperationException(
                "{0} is marked as read only and cannot be deleted".format(entity))
        self._included_documents_by_id.pop(self._documents_by_entity[entity]["key"], None)
        self._known_missing_ids.add(self._documents_by_entity[entity]["key"])
        self._deleted_entities.add(entity)

    def delete(self, key_or_entity, expected_change_vector=None):
        """
        @param str or object key_or_entity: Can be the key or the entity we like to delete
        @param str expected_change_vector: A change vector to use for concurrency checks
        :type str or object:
        """
        if key_or_entity is None:
            raise ValueError("None key is invalid")
        if not isinstance(key_or_entity, str):
            self.delete_by_entity(key_or_entity)
            return
        if key_or_entity in self._documents_by_id:
            entity = self._documents_by_id[key_or_entity]
            if self._has_change(entity):
                raise exceptions.InvalidOperationException(
                    "Can't delete changed entity using identifier. Use delete_by_entity(entity) instead.")
            if entity not in self._documents_by_entity:
                raise exceptions.InvalidOperationException(
                    "{0} is not associated with the session, cannot delete unknown entity instance".format(entity))
            if "Raven-Read-Only" in self._documents_by_entity[entity]["original_metadata"]:
                raise exceptions.InvalidOperationException(
                    "{0} is marked as read only and cannot be deleted".format(entity))
            self.delete_by_entity(entity)
            return
        self._known_missing_ids.add(key_or_entity)
        self._included_documents_by_id.pop(key_or_entity, None)
        self.defer(commands_data.DeleteCommandData(key_or_entity, expected_change_vector))

    def assert_no_non_unique_instance(self, entity, key):
        if not (key is None or key.endswith("/") or key not in self._documents_by_id
                or self._documents_by_id[key] is entity):
            raise exceptions.NonUniqueObjectException(
                "Attempted to associate a different object with id '{0}'.".format(key))

    def store(self, entity, key="", change_vector=""):
        """
        @param entity: Entity that will be stored
        :type object:
        @param key: Entity will be stored under this key, (None to generate automatically)
        :type str:
        @param change_vector: Current entity change_vector, used for concurrency checks (None to skip check)
        :type str
        """

        if entity is None:
            raise ValueError("None entity value is invalid")

        force_concurrency_check = self._get_concurrency_check_mode(entity, key, change_vector)
        if entity in self._documents_by_entity:
            if change_vector:
                self._documents_by_entity[entity]["change_vector"] = change_vector
            self._documents_by_entity[entity]["force_concurrency_check"] = force_concurrency_check
            return

        if not key:
            entity_id = GenerateEntityIdOnTheClient.try_get_id_from_instance(entity)
        else:
            GenerateEntityIdOnTheClient.try_set_id_on_entity(entity, key)
            entity_id = key

        self.assert_no_non_unique_instance(entity, entity_id)

        if not entity_id:
            entity_id = self._document_store.generate_id(self.database, entity)
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
        self._deleted_entities.discard(entity)
        self.save_entity(entity_id, entity, {}, metadata, {}, force_concurrency_check=force_concurrency_check)

    def _get_concurrency_check_mode(self, entity, key="", change_vector=""):
        if key == "":
            return "disabled" if change_vector is None else "forced"
        if change_vector == "":
            if key is None:
                entity_key = GenerateEntityIdOnTheClient.try_get_id_from_instance(entity)
                return "forced" if entity_key is None else "auto"
            return "auto"

        return "disabled" if change_vector is None else "forced"

    def time_series_for(self, entity_or_document_id, name):
        """
        Get A time series object associated with the document
        """
        return TimeSeries(self, entity_or_document_id, name)

    def counters_for(self, entity_or_document_id):
        """
        Get A counters object associated with the document
        """
        return DocumentCounters(self, entity_or_document_id)

    def save_changes(self):
        data = _SaveChangesData(list(self._defer_commands), len(self._defer_commands))
        self._defer_commands.clear()
        self._prepare_for_delete_commands(data)
        self._prepare_for_puts_commands(data)
        if len(data.commands) == 0:
            return
        self.increment_requests_count()
        batch_command = BatchCommand(data.commands)
        batch_result = self._requests_executor.execute(batch_command)
        if batch_result is None:
            raise exceptions.InvalidOperationException(
                "Cannot call Save Changes after the document store was disposed.")
        self._update_batch_result(batch_result, data)

    def _update_batch_result(self, batch_result, data):

        i = 0
        batch_result_length = len(batch_result)
        while i < batch_result_length:
            item = batch_result[i]
            if isinstance(item, dict) and "Type" in item:
                if item["Type"] == "PUT":
                    entity = data.entities[i - data.deferred_command_count]
                    if entity in self._documents_by_entity:
                        self._documents_by_id[item["@id"]] = entity
                        document_info = self._documents_by_entity[entity]
                        document_info["change_vector"] = item["@change-vector"]
                        item.pop("Type", None)
                        document_info["original_metadata"] = item.copy()
                        document_info["metadata"] = item
                        document_info["key"] = item["@id"]
                        document_info["original_value"] = entity.__dict__.copy()
                elif item["Type"] == "Counters":
                    cache = self._counters_by_document_id.get(item["Id"], None)
                    if not cache:
                        self._counters_by_document_id[item["Id"]] = [False, {}]
                        cache = self._counters_by_document_id[item["Id"]]

                    for counter in item["CountersDetail"]["Counters"]:
                        cache[1][counter["CounterName"]] = counter["TotalValue"]

                    self._counters_defer_commands.clear()
                elif item["Type"] == "TimeSeries":
                    self._timeseries_defer_commands.clear()
            i += 1

    def _prepare_for_delete_commands(self, data):
        keys_to_delete = []
        for entity in self._deleted_entities:
            keys_to_delete.append(self._documents_by_entity[entity]["key"])

        for key in keys_to_delete:
            change_vector = None
            if key in self._documents_by_id:
                existing_entity = self._documents_by_id[key]
                if existing_entity in self._documents_by_entity:
                    change_vector = self._documents_by_entity[existing_entity]["metadata"][
                        "@change-vector"] if self.advanced.use_optimistic_concurrency else None
                self._documents_by_entity.pop(existing_entity, None)
                self._documents_by_id.pop(key, None)
                data.entities.append(existing_entity)
            data.commands.append(commands_data.DeleteCommandData(key, change_vector))
        self._deleted_entities.clear()

    def _prepare_for_puts_commands(self, data):
        for entity in self._documents_by_entity:
            if self._has_change(entity):
                key = self._documents_by_entity[entity]["key"]
                metadata = self._documents_by_entity[entity]["metadata"]
                change_vector = None
                if self.advanced.use_optimistic_concurrency and self._documents_by_entity[entity][
                    "force_concurrency_check"] != "disabled" or self._documents_by_entity[entity][
                    "force_concurrency_check"] == "forced":
                    change_vector = self._documents_by_entity[entity]["change_vector"]
                data.entities.append(entity)
                if key:
                    self._documents_by_id.pop(key)
                    document = entity.__dict__.copy()
                    document.pop('Id', None)
                data.commands.append(commands_data.PutCommandData(key, change_vector, document, metadata))

    def _has_change(self, entity):
        import json
        entity_to_dict = json.loads(json.dumps(entity, default=self.conventions.json_default_method))
        if self._documents_by_entity[entity]["original_value"] != entity_to_dict or self._documents_by_entity[entity][
            "original_metadata"] != self._documents_by_entity[entity]["metadata"]:
            return True
        return False

    def increment_requests_count(self):
        self._number_of_requests_in_session += 1
        if self._number_of_requests_in_session > self.conventions.max_number_of_request_per_session:
            raise exceptions.InvalidOperationException(
                "The maximum number of requests ({0}) allowed for this session has been reached. Raven limits the number \
                of remote calls that a session is allowed to make as an early warning system. Sessions are expected to \
                be short lived, and Raven provides facilities like batch saves (call save_changes() only once).\
                You can increase the limit by setting DocumentConventions.\
                MaxNumberOfRequestsPerSession or MaxNumberOfRequestsPerSession, but it is advisable \
                that you'll look into reducing the number of remote calls first, \
                since that will speed up your application significantly and result in a\
                more responsive application.".format(self.conventions.max_number_of_request_per_session))


class Advanced(object):

    def __init__(self, session):
        self.session = session
        self.use_optimistic_concurrency = session.use_optimistic_concurrency if session.use_optimistic_concurrency \
            else session.conventions.use_optimistic_concurrency
        self._attachment = None

    @property
    def attachment(self):
        if not self._attachment:
            self._attachment = _Attachment(self.session)
        return self._attachment

    def stream(self, query):
        from pyravendb.store.stream import IncrementalJsonParser
        import ijson

        index_query = query.get_index_query()
        if index_query.wait_for_non_stale_results:
            raise exceptions.NotSupportedException("Since stream() does not wait for indexing (by design), "
                                                   "streaming query with wait_for_non_stale_results is not supported.")
        self.session.increment_requests_count()
        command = QueryStreamCommand(index_query)
        with self.session.requests_executor.execute(command) as response:
            basic_parse = IncrementalJsonParser.basic_parse(response)
            parser = ijson.backend.common.parse(basic_parse)

            results = ijson.backend.common.items(parser, "Results")
            for result in next(results, None):
                document, metadata, _, _ = Utils.convert_to_entity(result, query.object_type, self.session.conventions,
                                                                   query.nested_object_types)
                yield {"document": document, "metadata": metadata, "id": metadata.get("@id", None),
                       "change-vector": metadata.get("@change-vector", None)}

    def number_of_requests_in_session(self):
        return self.session.number_of_requests_in_session

    def get_document_id(self, instance):
        if instance is not None:
            if instance in self.session.documents_by_entity:
                return self.session.documents_by_entity[instance]["key"]
        return None

    @property
    def document_store(self):
        """
        The document store associated with this session
        """
        return self.session._document_store

    def clear(self):
        self.session.documents_by_entity.clear()
        self.session.documents_by_id.clear()
        self.session.deleted_entities.clear()
        self.session.included_documents_by_id.clear()
        self.session.known_missing_ids.clear()


class _Attachment:
    def __init__(self, session):
        self._session = session
        self._store = session.advanced.document_store
        self._defer_commands = session.defer_commands

    def _command_exists(self, document_id):
        for command in self._defer_commands:
            if not isinstance(command, (PutAttachmentCommandData, DeleteAttachmentCommandData, DeleteCommandData)):
                continue

            if command.key == document_id:
                return command

    def throw_not_in_session(self, entity):
        raise ValueError(
            repr(entity) + " is not associated with the session, cannot add attachment to it. "
                           "Use document Id instead or track the entity in the session.")

    def store(self, entity_or_document_id, name, stream, content_type=None, change_vector=None):
        if not isinstance(entity_or_document_id, str):
            entity = self._session.documents_by_entity.get(entity_or_document_id, None)
            if not entity:
                self.throw_not_in_session(entity_or_document_id)
            entity_or_document_id = entity["metadata"]["@id"]

        if not entity_or_document_id:
            raise ValueError(entity_or_document_id)
        if not name:
            raise ValueError(name)

        defer_command = self._command_exists(entity_or_document_id)
        if defer_command:
            message = "Can't store attachment {0} of document {1}".format(name, entity_or_document_id)
            if defer_command.type == "DELETE":
                message += ", there is a deferred command registered for this document to be deleted."
            elif defer_command.type == "AttachmentPUT":
                message += ", there is a deferred command registered to create an attachment with the same name."
            elif defer_command.type == "AttachmentDELETE":
                message += ", there is a deferred command registered to delete an attachment with the same name."
            raise exceptions.InvalidOperationException(message)

        entity = self._session.documents_by_id.get(entity_or_document_id, None)
        if entity and entity in self._session.deleted_entities:
            raise exceptions.InvalidOperationException(
                "Can't store attachment " + name + " of document" + entity_or_document_id +
                ", the document was already deleted in this session.")

        self._session.defer(PutAttachmentCommandData(entity_or_document_id, name, stream, content_type, change_vector))

    def delete(self, entity_or_document_id, name):
        if not isinstance(entity_or_document_id, str):
            entity = self._session.documents_by_entity.get(entity_or_document_id, None)
            if not entity:
                self.throw_not_in_session(entity_or_document_id)
            entity_or_document_id = entity["metadata"]["@id"]

        if not entity_or_document_id:
            raise ValueError(entity_or_document_id)
        if not name:
            raise ValueError(name)

        defer_command = self._command_exists(entity_or_document_id)
        if defer_command:
            if defer_command.type in ("Delete", "AttachmentDELETE"):
                return
            if defer_command.type == "AttachmentPUT":
                raise exceptions.InvalidOperationException(
                    "Can't delete attachment " + name + " of document " + entity_or_document_id +
                    ",there is a deferred command registered to create an attachment with the same name.")

        entity = self._session.documents_by_id.get(entity_or_document_id, None)
        if entity and entity in self._session.deleted_entities:
            return

        self._session.defer(DeleteAttachmentCommandData(entity_or_document_id, name, None))

    def get(self, entity_or_document_id, name):
        if not isinstance(entity_or_document_id, str):
            entity = self._session.documents_by_entity.get(entity_or_document_id, None)
            if not entity:
                self.throw_not_in_session(entity_or_document_id)
            entity_or_document_id = entity["metadata"]["@id"]

        operation = GetAttachmentOperation(entity_or_document_id, name, AttachmentType.document, None)
        return self._store.operations.send(operation)
