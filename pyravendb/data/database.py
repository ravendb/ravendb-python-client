import enum


class DatabaseDocument(object):
    __slots__ = ["database_id", "settings", "secured_settings", "disabled"]

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


class ReplicationDocument(object):
    def __init__(self, destinations):
        self.Id = "Raven/Replication/Destinations"
        self.destinations = []
        self.add_destinations(destinations)
        self.source = None

    def add_destinations(self, destinations):
        if not isinstance(destinations, list):
            destinations = [destinations]
        self.destinations = [destination.to_json() for destination in destinations]


class ReplicationDestination(object):
    __slots__ = ["destination", "url", "database", "user_name", "password", "domain", "api_key", "disabled",
                 "_humane"]

    def __init__(self, url, database, user_name=None, password=None, domain=None, api_key=None, disabled=False,
                 client_visible_url=None):
        self._humane = None if url is None else "{0} {1}".format(url, database)
        self.destination = {"url": url[:-1] if url.endswith('/') else url, "database": database, "username": user_name,
                            "password": password,
                            "domain": domain, "ApiKey": api_key,
                            "disabled": disabled, "clientVisibleUrl": client_visible_url,
                            "humane": None if url is None else "{0} {1}".format(url, database)}

    def to_json(self):
        return self.destination


class ApiKeyDefinition(object):
    def __init__(self, enabled=True, secret=None, server_admin=False, resources_access_mode=None):
        '''
        @param enabled: The api_key is enabled in the database
        :type bool
        @param secret: the secret key
        :type str
        @param resources_access_mode: resources is a dict with str as key (db/{Name oF the database}) and Access mods as value
        None
        ReadOnly
        ReadWrite
        Admin
        just put as str one of the above option
        '''
        self.enabled = enabled
        self.secret = secret
        self.server_admin = server_admin
        self.resources_access_mode = resources_access_mode

    def to_json(self):
        return {"Enabled": self.enabled,
                "ResourcessAccessMode": self.resources_access_mode if self.resources_access_mode is not None else {},
                "Secret": self.secret,
                "ServerAdmin": self.server_admin}
