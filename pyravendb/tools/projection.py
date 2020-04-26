def create_entity_with_mapper(dict_obj, mapper, object_type, convert_to_snake_case=None):
    """
    This method will create an entity from dict_obj and mapper
    In case convert_to_snake_case is empty will convert dict_obj keys to snake_case
    convert_to_snake_case can be dictionary with special words you can change ex. From -> from_date
    """
    from pyravendb.tools.utils import Utils

    def parse_dict_rec(data):
        try:
            if not isinstance(data, dict):
                try:
                    for i in range(len(data)):
                        data[i] = Utils.initialize_object(parse_dict_rec(data[i])[0], object_type,
                                                          convert_to_snake_case)
                except TypeError:
                    return data, False

            for key in data:
                if isinstance(data[key], dict):
                    parse_dict_rec(data[key])
                    data[key] = mapper(key, data[key])
                    if key is None:
                        pass
                elif isinstance(data[key], (tuple, list)):
                    for item in data[key]:
                        parse_dict_rec(item)
                    data[key] = mapper(key, data[key])
                else:
                    mapper_result = mapper(key, data[key])
                    data[key] = mapper_result if mapper_result else data[key]
            return data, True
        except TypeError as e:
            raise TypeError("Can't parse to custom object", e)

    first_parsed, need_to_parse = parse_dict_rec(dict_obj)
    # After create a complete dict for our object we need to create the object with the object_type
    if first_parsed is not None and need_to_parse:
        return Utils.initialize_object(first_parsed, object_type, convert_to_snake_case)
    return first_parsed
