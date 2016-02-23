class GenerateEntityIdOnTheClient(object):
    @staticmethod
    def try_set_id_on_entity(entity, key):
        entity.id = key

    @staticmethod
    def try_get_id_from_instance(entity):
        if entity is None:
            raise ValueError("None entity is invalid")
        id = getattr(entity, "id", None)
        return id
