from custom_exceptions import exceptions
from d_commands import commands_data
from tools.utils import Utils


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
        self._entities = {}
        self._deleted_entities = []
        self._original_entities = {}
        self._known_missing_ids = {}
        self.id_value = None
        self.__number_of_requests_in_session = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def load(self, key):
        if key is None:
            raise ValueError("None key is invalid")
        if key in self._entities:
            return self._entities[key]
        if self._is_missing(key):
            return None
        self.increment_requests_count()
        response = self.document_store.database_commands.get(key)["Results"]
        if None in response:
            self._known_missing_ids[key] = None
            return None
        self._entities[key] = response[0]
        self._original_entities[key] = response[0].copy()
        return self._entities[key]

    def _is_missing(self, key):
        return key in self._known_missing_ids

    def delete(self, key):
        if key is None:
            raise ValueError("None key is invalid")
        if key in self._entities:
            if self._entities[key] != self._original_entities[key]:
                raise exceptions.InvalidOperationException("Can't delete changed entity using identifier")
            self._entities.pop(key)
            self._original_entities.pop(key)
        self._deleted_entities.append(key)
        self._known_missing_ids[key] = None

    def store(self, entity, key=None, etag=None):
        """
        @param entity: Entity that will be stored
        :type object:
        @param key: Entity will be stored under this key, (None to generate automatically)
        :type str:
        @param etag: Current entity etag, used for concurrency checks (null to skip check)
        :type str
        """
        if key is None:
            entity_id = Utils.try_get_id_from_instance(entity)
            if entity_id and entity_id in self._entities:
                raise exceptions.NonUniqueObjectException(
                    "Attempted to associate a different object with id {0}".format(key))
        elif key in self._entities:
            raise exceptions.NonUniqueObjectException(
                "Attempted to associate a different object with id {0}".format(key))
        else:
            entity_id = key

        if not entity_id:
            entity_id = self.__create_id(entity)
        document = Utils.capitalize_dict_keys(vars(entity))
        document["@metadata"] = self.document_store.conventions.build_default_metadata(entity)
        self._entities[entity_id] = document

    def __create_id(self, entity):
        # in generate_id we get 1 if we made a request to the server or 0 if not
        entity_id, was_requests = self.document_store.generate_id(entity)
        if was_requests:
            self.increment_requests_count()
        return entity_id

    def save_changes(self):
        self.increment_requests_count()
        batch_commands = self.__make_batch_commands()
        self.document_store.database_commands.batch(batch_commands)

    def __make_batch_commands(self):
        commands = []
        for key in list(self._entities):
            if key in self._original_entities:
                if self._entities[key] != self._original_entities[key]:
                    entity = self._entities.pop(key)
                    metadata = entity.pop("@metadata")
                    commands.append(commands_data.PutCommandData(key=key, document=entity, metadata=metadata))
            else:
                entity = self._entities.pop(key)
                metadata = entity.pop("@metadata")
                commands.append(commands_data.PutCommandData(key=key, document=entity, metadata=metadata))

        for key in self._deleted_entities:
            commands.append(commands_data.DeleteCommandData(key=key))
        return commands

    def increment_requests_count(self):
        self.__number_of_requests_in_session += 1
        if self.__number_of_requests_in_session > self.document_store.conventions.max_number_of_request_per_session:
            raise exceptions.InvalidOperationException(
                "The maximum number of requests ({0}) allowed for this session has been reached. Raven limits the number \
                of remote calls that a session is allowed to make as an early warning system. Sessions are expected to \
                be short lived, and Raven provides facilities like batch saves (call save_changes() only once).\
                You can increase the limit by setting DocumentConvention.\
                MaxNumberOfRequestsPerSession or MaxNumberOfRequestsPerSession, but it is advisable \
                that you'll look into reducing the number of remote calls first, \
                since that will speed up your application significantly and result in a\
                more responsive application.".format(self.document_store.conventions.max_number_of_request_per_session))
