import json
from typing import Dict

from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.tools.utils import Utils


def _update_metadata_modifications(metadata_dictionary: MetadataAsDictionary, metadata: Dict) -> bool:
    dirty = False
    if metadata_dictionary is not None:
        if metadata_dictionary.is_dirty:
            dirty = True
        for key, value in metadata_dictionary.items():
            if value is None or isinstance(value, MetadataAsDictionary) and value.is_dirty is True:
                dirty = True
            metadata[key] = json.loads(json.dumps(value, default=Utils.json_default))
        if len(metadata) != len(metadata_dictionary):
            # looks like some props were removed
            to_remove = set()

            fields = metadata.keys()
            for field in fields:
                if field not in metadata_dictionary:
                    to_remove.add(field)

            for s in to_remove:
                del metadata[s]

    return dirty
