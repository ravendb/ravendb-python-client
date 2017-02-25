class ServerNode(object):
    def __init__(self, url, database, api_key=None, current_token=None, is_failed=False):
        self.url = url
        self.database = database
        self.api_key = api_key
        self.current_token = current_token
        self.is_failed = is_failed
