def create_entity_with_mapper(dict_obj, mapper, object_type):
    from pyravendb.tools.utils import Utils

    def parse_dict_rec(data):
        try:
            if not isinstance(data, dict):
                for i in range(len(data)):
                    data[i] = Utils.initialize_object(parse_dict_rec(data[i])[0], object_type)
                return data, False

            for key in data:
                if isinstance(data[key], dict):
                    parse_dict_rec(data[key])
                    data[key] = mapper(key, data[key])
                    if key is None:
                        pass
                if isinstance(data[key], (tuple, list)):
                    for item in data[key]:
                        parse_dict_rec(item)
                    data[key] = mapper(key, data[key])
            return data, True
        except TypeError as e:
            raise TypeError("Can't parse to custom object", e)

    first_parsed, need_to_parse = parse_dict_rec(dict_obj)
    # After create a complete dict for our object we need to create the object with the object_type
    if first_parsed is not None and need_to_parse:
        return Utils.initialize_object(first_parsed, object_type)
    return first_parsed
