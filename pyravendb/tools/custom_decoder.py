from pyravendb.tools.utils import Utils
import json


class JsonDecoder(json.JSONDecoder):
    """
    This custom JsonDecoder can add to json.loads function to work with pyravendb mappers solution.
    just use it like this json.loads(YOUR_OBJECT, cls=JsonDecoder, object_mapper=YOUR_MAPPER)
    Note that that last object that returns from the loads function will be a dict.
    To get the dict as your custom object you can create it with the return dict or use the parse_json method
    """

    def __init__(self, **kwargs):
        self.object_mapper = kwargs.pop("object_mapper", lambda key, value: None)
        super(JsonDecoder, self).__init__(object_hook=self.object_hook)
        self.step_down = None

    def object_hook(self, dict_object):
        if self.step_down is not None:
            for key in dict_object:
                result = self.object_mapper(key, dict_object[key])
                if result is not None:
                    dict_object[key] = result
        self.step_down = dict_object
        return self.step_down


def parse_json(json_string, object_type, mappers, convert_to_snake_case=False):
    """
    This function will use the custom JsonDecoder and the conventions.mappers to recreate your custom object
    in the parse json string state just call this method with the json_string your complete object_type and with your
    mappers dict.
    the mappers dict must contain in the key the object_type (ex. User) and the value will contain a method that get
    key, value (the key will be the name of the object property we like to parse and the value
    will be the properties of the object)
    """
    obj = json.loads(json_string, cls=JsonDecoder, object_mapper=mappers.get(object_type, None))

    if obj is not None:
        try:
            obj = object_type(**obj)
        except TypeError:
            initialize_dict, set_needed = Utils.make_initialize_dict(obj, object_type.__init__, convert_to_snake_case)
            o = object_type(**initialize_dict)
            if set_needed:
                for key, value in obj.items():
                    setattr(o, key, value)
            obj = o
    return obj
