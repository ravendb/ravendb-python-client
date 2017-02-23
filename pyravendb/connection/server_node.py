class ServerNode(object):
    def __init__(self, url, database, api_key, current_token, is_failed ):
        self.url = url
        self.database = database
        self.api_key = api_key
        self.current_token = current_token
        self.is_failed = is_failed
