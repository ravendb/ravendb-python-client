class DatabaseDocument(object):
    def __init__(self, database_id, settings=None, secured_settings=None, disabled=False):
        if secured_settings is None:
            secured_settings = {}
        if settings is None:
            settings = {}
        self.database_id = database_id
        self.settings = settings
        self.secured_settings = secured_settings
        self.disabled = disabled

    def to_json(self):
        return {"Disabled": self.disabled, "SecuredSettings": self.secured_settings, "Settings": self.settings}
