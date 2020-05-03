from pyravendb.raven_operations.counters_operations import *
from typing import List, Optional


class _CommandData(object):
    def __init__(self, key, command_type, change_vector=None, metadata=None):
        self.key = key
        self._type = command_type
        self.metadata = metadata
        self.change_vector = change_vector
        self.additionalData = None
        self.__command = True

    def to_json(self):
        raise NotImplementedError

    @property
    def command(self):
        return self.__command

    @property
    def type(self):
        return self._type


class PutCommandData(_CommandData):
    def __init__(self, key, change_vector=None, document=None, metadata=None):
        super(PutCommandData, self).__init__(key, "PUT", change_vector, metadata)
        self.document = document

    def to_json(self):
        self.document["@metadata"] = self.metadata
        data = {"Type": self._type, "Id": self.key, "Document": self.document}
        if self.change_vector:
            data["ChangeVector"] = self.change_vector
        return data


class DeleteCommandData(_CommandData):
    def __init__(self, key, change_vector=None):
        super(DeleteCommandData, self).__init__(key, "DELETE", change_vector)
        self.additionalData = None

    def to_json(self):
        data = {"Id": self.key, "ChangeVector": self.change_vector, "Type": self._type}
        if self.change_vector:
            data["ChangeVector"] = self.change_vector
        return {"Id": self.key, "Type": self._type}


class PatchCommandData(_CommandData):
    def __init__(self, key, scripted_patch, change_vector=None, patch_if_missing=None, additional_data=None,
                 debug_mode=False):
        """
        @param key: key of a document to patch.
        :type str
        @param scripted_patch: (using JavaScript) that is used to patch the document.
        :type ScriptedPatchRequest
        @param change_vector: Current document change_vector, used for concurrency checks (null to skip check).
        :type str
        @param patch_if_missing: (using JavaScript) that is used to patch a default document.
        :type ScriptedPatchRequest
        @param additional_data: Additional command data. For internal use only.
        :type dict
        @param debug_mode: Indicates in the Operation should be run in debug mode.
                           for additional information in response.
        :type bool

        """

        super(PatchCommandData, self).__init__(key, "PATCH", change_vector)
        if additional_data is None:
            additional_data = {}
        self.scripted_patch = scripted_patch
        self.patch_if_missing = patch_if_missing
        self.additional_data = additional_data
        self.debug_Mode = debug_mode

    def to_json(self):
        data = {"Id": self.key, "Type": self._type,
                "Patch": self.scripted_patch.to_json(),
                "DebugMode": self.debug_Mode}

        if self.change_vector:
            data["ChangeVector"] = self.change_vector

        if self.patch_if_missing is not None:
            data["PatchIfMissing"] = self.patch_if_missing.to_json()

        return data


class PutAttachmentCommandData(_CommandData):
    def __init__(self, document_id, name, stream, content_type, change_vector):
        """
        @param document_id: The id of the document
        @param name: Name of the attachment
        @param stream: The attachment as bytes (ex.open("file_path", "rb"))
        @param content_type: The type of the attachment (ex.image/png)
        @param change_vector: The change vector of the document
        """

        if not document_id:
            raise ValueError(document_id)
        if not name:
            raise ValueError(name)

        super(PutAttachmentCommandData, self).__init__(document_id, "AttachmentPUT", change_vector)
        self.name = name
        self.stream = stream
        self.content_type = content_type

    def to_json(self):
        data = {"Id": self.key, "Name": self.name, "ContentType": self.content_type, "Type": self._type}

        if self.change_vector:
            data["ChangeVector"] = self.change_vector

        return data


class DeleteAttachmentCommandData(_CommandData):
    def __init__(self, document_id, name, change_vector):
        """
        @param document_id: The id of the document
        @param name: Name of the attachment
        @param change_vector: The change vector of the document
        """

        if not document_id:
            raise ValueError(document_id)
        if not name:
            raise ValueError(name)

        super(DeleteAttachmentCommandData, self).__init__(document_id, "AttachmentDELETE", change_vector)
        self.name = name

    def to_json(self):
        data = {"Id": self.key, "Name": self.name, "Type": self._type}

        if self.change_vector:
            data["ChangeVector"] = self.change_vector

        return data


class TimeSeriesBatchCommandData(_CommandData):
    def __init__(self, document_id: str, name: str, operation: "TimeSeriesOperation", change_vector=None):

        if not document_id:
            raise ValueError("None or empty document id is Invalid")

        if not name:
            raise ValueError("None or empty name is Invalid")

        super().__init__(key=document_id, command_type="TimeSeries", change_vector=change_vector)
        self._time_series = operation

    @property
    def time_series(self):
        return self._time_series

    def to_json(self):
        return {"Id": self.key, "TimeSeries": self.time_series.to_json(), "Type": self.type}


class CountersBatchCommandData(_CommandData):
    def __init__(self, document_id: str, counter_operations: List[CounterOperation] or CounterOperation,
                 from_etl: Optional[bool] = None, change_vector=None):
        if not document_id:
            raise ValueError("None or empty document id is Invalid")
        if not counter_operations:
            raise ValueError("invalid counter_operations")
        if not isinstance(counter_operations, list):
            counter_operations = [counter_operations]

        super().__init__(key=document_id, command_type="Counters", change_vector=change_vector)
        self._from_etl = from_etl
        self._counters = DocumentCountersOperation(document_id=self.key, operations=counter_operations)

    def has_increment(self, counter_name):
        self.has_operation_of_type(CounterOperationType.increment, counter_name)

    def has_delete(self, counter_name):
        self.has_operation_of_type(CounterOperationType.delete, counter_name)

    def has_operation_of_type(self, operation_type: CounterOperationType, counter_name: str):
        for op in self.counters.operations:
            if op.counter_name != counter_name:
                continue

            if op.counter_operation_type == operation_type:
                return True
        return False

    @property
    def counters(self):
        return self._counters

    def to_json(self):
        json_dict = {"Id": self.key, "Counters": self.counters.to_json(), "Type": self.type}
        if self._from_etl:
            json_dict["FromEtl"] = self._from_etl
        return json_dict
