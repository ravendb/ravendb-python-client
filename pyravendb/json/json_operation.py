import pyravendb.tools.utils

import pyravendb.constants as constants
from pyravendb.tools.utils import Utils


class JsonOperation:
    @staticmethod
    def entity_changed(new_obj, document_info, changes, default_json):
        doc_changes = list() if changes is not None else None
        if not document_info.get("is_new_document") and document_info.get("entity"):
            original_dict = Utils.entity_to_dict(document_info.get("original_value"), default_json)
            original_dict.update({"@metadata": document_info.get("metadata")})
            return JsonOperation.compare_json(
                "",
                document_info.get("key"),
                original_dict,
                new_obj,
                changes,
                doc_changes,
            )
        if changes is None:
            return True
        JsonOperation.new_change(None, None, None, None, doc_changes, "document_added")
        changes.update({document_info["key"]: doc_changes})
        return True

    @staticmethod
    def new_change(field_path, name, new_value, old_value, doc_changes, change_type):
        changes = {
            "old_value": old_value,
            "new_value": new_value,
            "change": change_type,
            "field_name": name,
            "field_path": field_path,
        }
        doc_changes.append(changes)

    @staticmethod
    def compare_json(field_path, key, original_json, new_json, changes, doc_changes):
        old_json_props = set(original_json.keys())
        new_json_props = set(new_json.keys())

        new_fields = new_json_props - old_json_props
        removed_fields = old_json_props - new_json_props

        for field in removed_fields:
            if changes is None:
                return True
            JsonOperation.new_change(field_path, field, None, None, doc_changes, "removed_field")

        for prop in new_json_props:
            if (
                prop == constants.Documents.Metadata.LAST_MODIFIED
                or prop == constants.Documents.Metadata.COLLECTION
                or prop == constants.Documents.Metadata.CHANGE_VECTOR
                or prop == constants.Documents.Metadata.ID
            ):
                continue

            if prop in new_fields:
                if changes is None:
                    return True
                JsonOperation.new_change(field_path, prop, new_json[prop], None, doc_changes, "new_field")
                continue

            new_prop = new_json[prop]
            old_prop = original_json[prop]

            if isinstance(new_prop, (int, float, bool, str)):
                if new_prop == old_prop or JsonOperation.compare_values(old_prop, new_prop):
                    continue
                if changes is None:
                    return True
                JsonOperation.new_change(field_path, prop, new_prop, old_prop, doc_changes, "field_changed")

            elif new_prop is None:
                if old_prop is None:
                    continue
                if changes is None:
                    return True
                JsonOperation.new_change(field_path, prop, None, old_prop, doc_changes, "field_changed")

            elif isinstance(new_prop, (list, set)):
                if not isinstance(old_prop, (list, set)):
                    if changes is None:
                        return True
                    JsonOperation.new_change(field_path, prop, new_prop, old_prop, doc_changes, "field_changed")
                    continue
                changed = JsonOperation.compare_json_array(
                    JsonOperation.field_path_combine(field_path, prop),
                    key,
                    old_prop,
                    new_prop,
                    changes,
                    doc_changes,
                    prop,
                )
                if changes is None and changed is True:
                    return True
            else:
                if old_prop is None:
                    if changes is None:
                        return True
                    JsonOperation.new_change(field_path, prop, new_prop, None, doc_changes, "field_changed")
                changed = JsonOperation.compare_json(
                    JsonOperation.field_path_combine(field_path, prop), key, old_prop, new_prop, changes, doc_changes
                )
                if changes is None and changed is True:
                    return True
        if changes is None or len(doc_changes) <= 0:
            return False
        changes.update({key: doc_changes})
        return True

    @staticmethod
    def compare_values(old_prop, new_prop):
        return old_prop == new_prop and type(old_prop) == type(new_prop)

    @staticmethod
    def field_path_combine(path1, path2):
        return path2 if not path1 else f"{path1}.{path2}"

    @staticmethod
    def add_index_field_path(field_path, position):
        return f"{field_path}[{position}]"

    @staticmethod
    def compare_json_array(field_path, key, old_collection, new_collection, changes, doc_changes, prop_name):
        # if we don't care about the changes
        if len(old_collection) != len(new_collection) and changes is None:
            return True
        position = 0
        changed = False

        while position < len(old_collection) and position < len(new_collection):
            old_collection_item = old_collection[position]
            new_collection_item = new_collection[position]
            if old_collection_item is None:
                if new_collection_item is not None:
                    changed = True
                    if changes is not None:
                        JsonOperation.new_change(
                            JsonOperation.add_index_field_path(field_path, position),
                            prop_name,
                            new_collection_item,
                            old_collection_item,
                            doc_changes,
                            "array_value_added",
                        )
            elif isinstance(old_collection_item, (list, set)):
                if isinstance(new_collection_item, (list, set)):
                    changed |= JsonOperation.compare_json_array(
                        JsonOperation.add_index_field_path(field_path, position),
                        key,
                        old_collection_item,
                        new_collection_item,
                        changes,
                        doc_changes,
                        prop_name,
                    )
                else:
                    changed = True
                    if changes is None:
                        JsonOperation.new_change(
                            JsonOperation.add_index_field_path(field_path, position),
                            prop_name,
                            new_collection_item,
                            old_collection_item,
                            doc_changes,
                            "array_value_changed",
                        )
            elif isinstance(new_collection_item, (int, float, bool, str)):
                if not str(old_collection_item) == str(new_collection_item):
                    if changes is not None:
                        JsonOperation.new_change(
                            JsonOperation.add_index_field_path(field_path, position),
                            prop_name,
                            new_collection_item,
                            old_collection_item,
                            doc_changes,
                            "array_value_changed",
                        )
                    changed = True
            else:
                if type(old_collection_item) is type(new_collection_item):
                    changed |= JsonOperation.compare_json(
                        JsonOperation.add_index_field_path(field_path, position),
                        key,
                        old_collection_item,
                        new_collection_item,
                        changes,
                        doc_changes,
                    )
                else:
                    changed = True
                    if changes is not None:
                        JsonOperation.new_change(
                            JsonOperation.add_index_field_path(field_path, position),
                            prop_name,
                            new_collection_item,
                            old_collection_item,
                            doc_changes,
                            "array_value_added",
                        )
            position += 1
        if changes is None:
            return changed

        # if one of the arrays is larger than the other
        while position < len(old_collection):
            old_collection_item = old_collection[position]
            JsonOperation.new_change(
                field_path, prop_name, None, old_collection_item, doc_changes, "array_value_removed"
            )
            position += 1

        while position < len(new_collection):
            new_collection_item = new_collection[position]
            JsonOperation.new_change(field_path, prop_name, new_collection_item, None, doc_changes, "array_value_added")
            position += 1

        return changed
