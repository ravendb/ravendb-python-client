class _CommandData(object):
    def __init__(self, key, etag=None, metadata=None):
        self.key = key
        self.metadata = metadata
        self.etag = etag
        self.additionalData = None
        self.__command = True

    def to_json(self):
        raise NotImplementedError

    @property
    def command(self):
        return self.__command


class PutCommandData(_CommandData):
    def __init__(self, key, etag=None, document=None, metadata=None):
        super(PutCommandData, self).__init__(key, etag, metadata)
        self._method = "PUT"
        self.document = document

    def to_json(self):
        self.document["@metadata"] = self.metadata
        return {"Method": self._method, "Key": self.key, "Document": self.document, "Etag": self.etag}


class DeleteCommandData(_CommandData):
    def __init__(self, key, etag=None):
        super(DeleteCommandData, self).__init__(key, etag)
        self._method = "DELETE"
        self.additionalData = None

    def to_json(self):
        return {"Key": self.key, "Etag": self.etag, "Method": self._method}


class ScriptedPatchCommandData(_CommandData):
    def __init__(self, key, scripted_patch, etag=None, metadata=None, patch_if_missing=None, additional_data=None,
                 debug_mode=False):
        """
        @param key: key of a document to patch.
        :type str
        @param scripted_patch: (using JavaScript) that is used to patch the document.
        :type ScriptedPatchRequest
        @param etag: Current document etag, used for concurrency checks (null to skip check).
        :type str
        @param metadata: representing document's metadata.
        :type dict
        @param patch_if_missing: (using JavaScript) that is used to patch a default document.
        :type ScriptedPatchRequest
        @param additional_data: Additional command data. For internal use only.
        :type dict
        @param debug_mode: Indicates in the operation should be run in debug mode.
                           for additional information in response.
        :type bool

        """

        super(ScriptedPatchCommandData, self).__init__(key, etag)
        if additional_data is None:
            additional_data = {}
        if metadata is None:
            metadata = {}
        self._method = "EVAL"
        self.scripted_patch = scripted_patch
        self.metadata = metadata
        self.patch_if_missing = patch_if_missing
        self.additional_data = additional_data
        self.debug_Mode = debug_mode

    def to_json(self):
        data = {"Key": self.key, "Method": self._method, "Patch": self.scripted_patch.to_json(),
                "DebugMode": self.debug_Mode, "Metadata": self.metadata}
        if self.etag is not None:
            data["Etag"] = self.etag
        if self.patch_if_missing is not None:
            data["PatchIfMissing"] = self.patch_if_missing.to_json()

        return data
