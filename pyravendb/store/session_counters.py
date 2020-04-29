from pyravendb.commands.commands_data import CountersBatchCommandData
from pyravendb.raven_operations.counters_operations import *


class DocumentCounters:

    @staticmethod
    def raise_not_in_session(entity):
        raise ValueError(
            repr(entity) + " is not associated with the session, cannot add counter to it. "
                           "Use document_id instead or track the entity in the session.")

    @staticmethod
    def raise_document_already_deleted_in_session(document_id, counter_name):
        raise exceptions.InvalidOperationException(f"Can't increment counter {counter_name} of document {document_id}, "
                                                   f"the document was already deleted in this session.")

    @staticmethod
    def raise_increment_after_delete(document_id, counter_name):
        raise exceptions.InvalidOperationException(f"Can't increment counter {counter_name} of document {document_id}, "
                                                   f"there is a deferred command registered to "
                                                   f"delete a counter with the same name.")

    @staticmethod
    def raise_delete_after_increment(document_id, counter_name):
        raise exceptions.InvalidOperationException(f"Can't delete counter {counter_name} of document {document_id}, "
                                                   f"there is a deferred command registered to "
                                                   f"increment a counter with the same name.")

    def __init__(self, session, entity_or_document_id):
        self._session = session
        if not isinstance(entity_or_document_id, str):
            entity = self._session.documents_by_entity.get(entity_or_document_id, None)
            if not entity:
                self.raise_not_in_session(entity_or_document_id)
            entity_or_document_id = entity.get("key", None)

        if not entity_or_document_id:
            raise ValueError(entity_or_document_id)

        self._document_id = entity_or_document_id

    def increment(self, counter_name, delta=1):
        if not counter_name:
            raise ValueError("Invalid counter")

        document = self._session.documents_by_id.get(self._document_id, None)
        if document and document in self._session.deleted_entities:
            self.raise_document_already_deleted_in_session(self._document_id, self._name)

        counter_operation = CounterOperation(counter_name, CounterOperationType.increment, delta)

        command: CountersBatchCommandData = self._session.counters_defer_commands.get(self._document_id, None)
        if command:
            if command.has_delete(counter_name):
                raise self.raise_increment_after_delete(self._document_id, counter_name)
            command.counters.add_operations(counter_operation)
        else:
            command = CountersBatchCommandData(self._document_id, counter_operations=counter_operation)
            self._session.counters_defer_commands[self._document_id] = command
            self._session.defer(command)

    def delete(self, counter_name):
        if not counter_name:
            raise ValueError("None or empty counter is invalid")

        document = self._session.documents_by_id.get(self._document_id, None)
        if document and document in self._session.deleted_entities:
            return

        counter_operation = CounterOperation(counter_name, CounterOperationType.delete)
        command: CountersBatchCommandData = self._session.counters_defer_commands.get(self._document_id, None)
        if command:
            if command.has_delete(counter_name):
                return
            if command.has_increment(counter_name):
                raise self.raise_delete_after_increment(self._document_id, counter_name)

            command.counters.add_operations(counter_operation)

        command = CountersBatchCommandData(self._document_id, counter_operations=counter_operation)
        self._session.counters_defer_commands[self._document_id] = command
        self._session.defer(command)

        cache = self._session.counters_by_document_id.get(self._document_id, None)
        if cache:
            cache[1].pop(counter_name, None)

    def get_all(self):
        """
        Get all the counters for the document
        """
        cache = self._session.counters_by_document_id.get(self._document_id, None)
        if not cache:
            cache = [False, {}]

        document = self._session.documents_by_id.get(self._document_id)
        info = self._session.documents_by_entity.get(document, None) if document else None
        metadata_counters = info["metadata"].get("@counters", None) if info else None

        missing_counters = False
        if cache[1] and metadata_counters:
            for counter in metadata_counters:
                if counter in cache[1]:
                    continue
                missing_counters = True
                break

        elif not cache[0]:
            missing_counters = True

        if missing_counters:
            self._session.increment_requests_count()
            details = self._session.advanced.document_store.operations.send(
                GetCountersOperation(document_id=self._document_id))

            for counter_detail in details["Counters"]:
                cache[1][counter_detail["CounterName"]] = counter_detail["TotalValue"]

        if not self._session.no_tracking:
            self._session.counters_by_document_id[self._document_id] = cache

        cache[0] = True
        return cache[1]

    def get(self, counter_names: List[str] or str):
        """
         Get the counter by counter name
        """
        cache = self._session.counters_by_document_id.get(self._document_id, None)
        if not isinstance(counter_names, list):
            counter_names = [counter_names]

        if not cache:
            cache = [False, {}]

        document = self._session.documents_by_id.get(self._document_id)
        info = self._session.documents_by_entity.get(document, None) if document else None
        metadata_counters = info["metadata"].get("@counters", None) if info else None

        result = {}
        missing_counters = False
        for counter_name in counter_names:
            val = cache[1].get(counter_name, None)
            if not val or val and metadata_counters and counter_name not in metadata_counters or not cache[0]:
                missing_counters = True
                break
            result[counter_name] = val

        if missing_counters:
            self._session.increment_requests_count()
            details = self._session.advanced.document_store.operations.send(
                GetCountersOperation(document_id=self._document_id, counters=counter_names))

            for counter_detail in details["Counters"]:
                cache[1][counter_detail["CounterName"]] = counter_detail["TotalValue"]
                result[counter_detail["CounterName"]] = counter_detail["TotalValue"]

        if not self._session.no_tracking:
            self._session.counters_by_document_id[self._document_id] = cache

        return result if len(counter_names) > 1 else result.get(counter_names[0], None)
