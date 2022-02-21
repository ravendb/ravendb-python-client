from pyravendb import constants
from pyravendb.legacy.commands import CountersBatchCommandData
from pyravendb.tools.utils import CaseInsensitiveDict


class DocumentCounters:
    @staticmethod
    def raise_not_in_session(entity):
        raise ValueError(
            repr(entity) + " is not associated with the session, cannot add counter to it. "
            "Use document_id instead or track the entity in the session."
        )

    @staticmethod
    def raise_document_already_deleted_in_session(document_id, counter_name):
        raise exceptions.InvalidOperationException(
            f"Can't increment counter {counter_name} of document {document_id}, "
            f"the document was already deleted in this session."
        )

    @staticmethod
    def raise_increment_after_delete(document_id, counter_name):
        raise exceptions.InvalidOperationException(
            f"Can't increment counter {counter_name} of document {document_id}, "
            f"there is a deferred command registered to "
            f"delete a counter with the same name."
        )

    @staticmethod
    def raise_delete_after_increment(document_id, counter_name):
        raise exceptions.InvalidOperationException(
            f"Can't delete counter {counter_name} of document {document_id}, "
            f"there is a deferred command registered to "
            f"increment a counter with the same name."
        )

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
        cache = self._session.included_counters_by_document_id.get(self._document_id, None)
        if cache:
            cache[1].pop(counter_name, None)

    def get_all(self):
        """
        Get all the counters for the document
        """
        cache = self._session.counters_by_document_id.get(self._document_id, None)
        if not cache:
            cache = [False, CaseInsensitiveDict()]
        missing_counters = not cache[0]

        document = self._session.advanced.get_document_by_id(self._document_id)
        if document:
            metadata_counters = document["metadata"].get(constants.Documents.Metadata.COUNTERS, None)
            if not metadata_counters:
                missing_counters = False
            elif len(cache[1]) >= len(metadata_counters):
                missing_counters = False
                for c in metadata_counters:
                    if str(c) in cache[1]:
                        continue
                    missing_counters = True
                    break

        if missing_counters:
            self._session.increment_requests_count()
            details = self._session.advanced.document_store.operations.send(
                GetCountersOperation(document_id=self._document_id)
            )
            cache[1].clear()
            for counter_detail in details["Counters"]:
                cache[1][counter_detail["CounterName"]] = counter_detail["TotalValue"]

        cache[0] = True
        if not self._session._no_tracking:
            self._session.counters_by_document_id[self._document_id] = cache
        return cache[1]

    def get(self, *counter_names: str):
        """
        Get the counter by counter name
        """

        cache = self._session.counters_by_document_id.get(self._document_id, None)
        if len(counter_names) == 1:
            value = None
            counter = counter_names[0]
            if cache:
                value = cache[1].get(counter, None)
                if value:
                    return value
            else:
                cache = [False, CaseInsensitiveDict()]
            document = self._session.advanced.get_document_by_id(self._document_id)
            metadata_has_counter_name = False
            if document:
                metadata_counters = document["metadata"].get(constants.Documents.Metadata.COUNTERS)
                if metadata_counters:
                    metadata_has_counter_name = counter.lower() in list(map(str.lower, metadata_counters))
            if (document is None and not cache[0]) or metadata_has_counter_name:
                self._session.increment_requests_count()
                details = self._session.advanced.document_store.operations.send(
                    GetCountersOperation(document_id=self._document_id, counters=list(counter_names))
                )
                counter_detail = details.get("Counters", None)[0]
                value = counter_detail["TotalValue"] if counter_detail else None

            cache[1].update({counter: value})
            if self._session._no_tracking:
                self._session.counters_by_document_id.update({self._document_id: cache})
            return value

        if cache is None:
            cache = [False, CaseInsensitiveDict()]

        metadata_counters = None
        document = self._session.advanced.get_document_by_id(self._document_id)

        if document:
            metadata_counters = document["metadata"].get(constants.Documents.Metadata.COUNTERS)

        result = {}

        for counter in counter_names:
            has_counter = counter in cache[1]
            val = cache[1].get(counter, None)
            not_in_metadata = True

            if document and metadata_counters:
                for metadata_counter in metadata_counters:
                    if str(metadata_counter).lower() == counter.lower():
                        not_in_metadata = False

            if has_counter or cache[0] or document and not_in_metadata:
                result[counter] = val
                continue

            result.clear()
            self._session.increment_requests_count()
            details = self._session.advanced.document_store.operations.send(
                GetCountersOperation(document_id=self._document_id, counters=list(counter_names))
            )

            for counter_detail in details["Counters"]:
                if not counter_detail:
                    continue
                cache[1][counter_detail["CounterName"]] = counter_detail["TotalValue"]
                result[counter_detail["CounterName"]] = counter_detail["TotalValue"]
            break

        if not self._session._no_tracking:
            self._session.counters_by_document_id[self._document_id] = cache

        return result if len(counter_names) > 1 else result[counter_names[0]]
