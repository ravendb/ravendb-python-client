class PatchRequest(object):
    def __init__(self, script, values=None):
        if values is None:
            values = {}
        self.script = script
        self.values = values

    def to_json(self):
        return {"Script": self.script, "Values": self.values}
