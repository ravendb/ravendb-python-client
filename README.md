## Overview 
PyRavenDB is a python client api for [ravenDB](https://ravendb.net/) document database
```
    with document_store.documentstore(url="http://localhost:8080", database="PyRavenDB") as store:
        store.initialize()
        with store.open_session() as session:
            foo = session.load("foos/1")
```

## Installation
There are three ways to install and use the Appium Python client.

1. Install from [PyPi](https://pypi.python.org/pypi), as [pyravendb](https://pypi.python.org/pypi/pyravendb).
	```
	pip install pyravendb
	```

2. Install from source, via PyPi. From pyravendb, download, open the source (pyravendb-x.x.x.zip) and run setup.py.
	```
    python setup.py install
	```
3. Install from source via [GitHub](https://github.com/IdanHaim/RavenDB-Python-Client).
	```
    git clone https://github.com/IdanHaim/RavenDB-Python-Client.git
    cd RavenDB-Python-Client
    python setup.py install
	```

## Usage
coming soon