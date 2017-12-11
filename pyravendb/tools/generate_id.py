class GenerateEntityIdOnTheClient(object):
    @staticmethod
    def try_set_id_on_entity(entity, key):
        if hasattr(entity, "Id"):
            entity.Id = key

    @staticmethod
    def try_get_id_from_instance(entity):
        if entity is None:
            raise ValueError("None entity is invalid")
        return getattr(entity, "Id", None)
