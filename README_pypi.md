
# Official Python client for RavenDB NoSQL Database

## Installation
Install from [PyPi](https://pypi.python.org/pypi), as [ravendb](https://pypi.org/project/ravendb/).
```bash
pip install ravendb
```

## Introduction
Python client API (v7.2) for [RavenDB](https://ravendb.net/) , a NoSQL document database.

**Type-hinted entire project and API results** - using the API is now much more comfortable with IntelliSense

## Releases

### [Click here](https://github.com/ravendb/ravendb-python-client/releases) to view all Releases and Changelog.


### Demo

##### Working with secured server
```python
from ravendb import DocumentStore

URLS = ["https://raven.server.url"]
DB_NAME = "SecuredDemo"
CERT_PATH = "path\\to\\cert.pem"


class User:
    def __init__(self, name: str, tag: str):
        self.name = name
        self.tag = tag


store = DocumentStore(URLS, DB_NAME)
store.certificate_pem_path = CERT_PATH
store.initialize()
user = User("Gracjan", "Admin")

with store.open_session() as session:
    session.store(user, "users/1")
    session.save_changes()

with store.open_session() as session:
    user = session.load("users/1", User)
    assert user.name == "Gracjan"
    assert user.tag == "Admin"
```
----
#### RavenDB Documentation
https://ravendb.net/docs/article-page/5.3/python

----
#### GitHub
https://github.com/ravendb/ravendb-python-client

-----
##### Bug Tracker
http://issues.hibernatingrhinos.com/issues/RDBC