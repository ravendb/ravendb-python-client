from data.indexes import SortOptions


class DocumentConvention(object):
    def __init__(self):
        self.max_number_of_request_per_session = 30
        self.max_ids_to_catch = 32

    @staticmethod
    def default_transform_type_tag_name(name):
        count = sum(1 for c in name if c.isupper())
        if count <= 1:
            return name.lower() + 's'
        return name + 's'

    @staticmethod
    def build_default_metadata(entity):
        if entity is None:
            return {}
        return {"Raven-Entity-Name": str(entity.__class__.__name__ + 's'),
                "Raven-Python-Type": "{0}.{1}".format(entity.__class__.__module__, entity.__class__.__name__)}

    @staticmethod
    def try_get_type_from_metadata(metadata):
        if "Raven-Python-Type" in metadata:
            return metadata["Raven-Python-Type"]
        return None

    @staticmethod
    def uses_range_type(obj):
        if obj is None:
            return False

        if isinstance(obj, int) or isinstance(obj, long) or isinstance(obj, float):
            return True

    @staticmethod
    def get_default_sort_option(type_name):
        if not type_name:
            return None
        if type_name == "int" or type_name == "float":
            return SortOptions.float.value
        if type_name == "long":
            return SortOptions.long.value
