## Overview 
PyRavenDB is a python client api for [RavenDB](https://ravendb.net/), a NoSQL document database.
The API can handle most CRUD scenarios, including full support for replication, failover, dynamic queries, etc.


```python
with document_store.documentstore(url="http://localhost:8080", database="PyRavenDB") as store:
	store.initialize()
	with store.open_session() as session:
		foo = session.load("foos/1")
```

## Installation
There are three ways to install pyravendb.

1. Install from [PyPi](https://pypi.python.org/pypi), as [pyravendb](https://pypi.python.org/pypi/pyravendb).
	```bash
	pip install pyravendb
	```

2. Install from source, via PyPi. From pyravendb, download, open the source (pyravendb-x.x.x.zip) and run setup.py.
	```bash
    python setup.py install
	```
3. Install from source via [GitHub](https://github.com/IdanHaim/RavenDB-Python-Client).
 
	```bash
    git clone https://github.com/IdanHaim/RavenDB-Python-Client.git
    cd RavenDB-Python-Client
    python setup.py install
	```

## Usage
##### Load a single or several document\s from the store:
 ```python
    from pyravendb.store import document_store
    
    store =  document_store.documentstore(url="http://localhost:8080", database="PyRavenDB")
    store.initialize() 
    with store.open_session() as session:
    	foo = session.load("foos/1")
```

load method have the option to track entity for you the only thing you need to do is add ```object_type```  when you call to load 
(load method will return a dynamic_stracture object by default) for class with nested object you can call load with ```nested_object_types``` dictionary for the other types. just put in the key the name of the object and in the value his class (without this option you will get the document as it is) .

```pyton
	foo = session.load("foos/1", object_type=Foo)
```

```python
	class FooBar(object):
		def __init__(self,name,foo):
			self.name = name
			self.foo = foo
	
	foo_bar = session.load("FooBars/1", object_type=FooBar,nested_object_types={"foo":Foo})
			
```
To load several documents at once, supply a list of ids to session.load.

```python
	foo = session.load(["foos/1", "foos/2", "foos/3"], object_type=Foo)
```

##### Delete a document
To delete a document from the store,  use ```session.delete()``` with the id or entity you would like to delete.

```python
with store.open_session() as session:
       foo = session.delete("foos/1")
```

##### Store a new document
to store a new document, use ```session.store(entity)``` with the entity you would like to store (entity must be an object)
For storing with dict we will use database_commands the put command (see the source code for that).

```python
class Foo(object):
   def __init__(name,key = None):
   	self.name = name
      	self.key = key
      
class FooBar(object):
   def __init__(self,name,foo):
	self.name = name
	self.foo = foo

with store.open_session() as session:
	foo = Foo("PyRavenDB")
    session.store(foo)
    session.save_changes()
```

###### To save all the changes we made we need to call ```session.save_changes()```.

##### Query

* ```object_type``` - Give the object type you want to get from query.
* ```index_name``` -  The name of index you want the query to work on (If empty the index will be dynamic).
* ```using_default_operator``` - QueryOperator enum QueryOperator.AND or QueryOperator.OR by default.
* ```wait_for_non_stale_results``` - False by default if True the query will wait until the index will be non stale.
* ```includes``` - A list of the properties we like to include in the query.
* ``` with_statistics``` - when True the qury will return stats about the query.
* ```nested_object_types``` - A dict of classes for nested object the key will be the name of the class and the value will be 
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&nbsp;the object we want to get for that attribute
	
```python
with store.open_session() as session:
	query_result = list(session.query().where_equals("name", "test101")
	query_result = list(session.query(object_type=Foo).where_starts_with("name", "n"))
	query_result = list(session.query(object_type=Foo).where_ends_with("name", "7"))
	query_result = list(session.query(object_type=FooBar,nested_object_types={"foo":Foo}).where(name="foo_bar"))
	
```

You can also build the query with several options using the builder pattern.

```python
with store.open_session() as session:
	list(session.query(wait_for_non_stale_results=True).where_not_none("name").order_by_descending("name"))
``` 

For the query you can also use the `where` feature which can get a variable number of arguments (**kwargs)
```python
with store.open_session() as session:
	query_results = list(session.query().where(name="test101", key=[4, 6, 90]))

```
`name` and `key` are the field names for which we query

##### Includes
A list of the properties we like to include in the query or in load.
The include property wont show in our result but when we load or query for it we wont requests it from the server.
The includes will save on the session cache.
```python
class FooBar(object):
	def __init__(name, foo_key)
    	self.name = name
        self.foo = foo_key

store =  document_store.documentstore(url="http://localhost:8080", database="PyRavenDB")
store.initialize() 
with store.open_session() as session:
	query_result = list(session.query(includes="foo").where_equals("name", "testing_includes")
    foo_bar = session.load("FooBars/1", object_type=FooBar, includes=foo)
    
```

##### Replication

Replication works using plain HTTP requests to replicate all changes from one server instance to another.
* enable Replication bundle (```"Raven/ActiveBundles"```) on a database e.g. you can create new database with the following code:
	```python
    from pyravendb.data import database
    
    database_document = database.DatabaseDocument(database_id="PyRavenDB", settings={"Raven/DataDir": "test", "Raven/ActiveBundles": "Replication"})
     store.database_commands.admin_commands.create_database(database_document=database_document)
	```

    ###### database_commands are a set of low level operations that can be used to manipulate data and change configuration on a server. 

* setup a replication by creating the ```ReplicationDestinations``` document with appropriate settings.
	```python
    with store.open_session("PyRavenDB") as session:
            replication_document = database.ReplicationDocument([database.ReplicationDestination(url="http://localhost:8080", database="destination_database_name")])
            session.store(replication_document)
            session.save_changes()
    ```
    
	###### Failover
	There are four possible failover for replication:
	
	1.<B>allow_reads_from_secondaries</B> - This is usually the safest approach, because it means that you can still serve
	    read requests when the primary node is down, but don't have to deal with replication
	    conflicts if there are writes to the secondary when the primary node is down (<B>we use this option by default</B>).
	    
	2.<B>allow_reads_from_secondaries_and_writes_to_secondaries</B> - Allow reads from and writes to secondary server(s).
	    Choosing this option requires that you'll have some way of propagating changes
	    made to the secondary server(s) to the primary node when the primary goes back
	    up.
	    A typical strategy to handle this is to make sure that the replication is setup
	    in a master/master relationship, so any writes to the secondary server will be
	    replicated to the master server.
	    Please note, however, that this means that your code must be prepared to handle
	    conflicts in case of different writes to the same document across nodes.
	
	3.<B>fail_immediately</B> - Immediately fail the request, without attempting any failover. This is true for both
	    reads and writes. The RavenDB client will not even check that you are using replication.
	    This is mostly useful when your replication setup is meant to be used for backups / external
	    needs, and is not meant to be a failover storage.
	
	4.<B>read_from_all_servers</B> - Read requests will be spread across all the servers, instead of doing all the work against the master.
	    Write requests will always go to the master.
	    This is useful for striping, spreading the read load among multiple servers. The idea is that this will give us
	    better read performance overall.
	    A single session will always use the same server, we don't do read striping within a single session.
	    Note that using this means that you cannot set UserOptimisticConcurrency to true,
	    because that would generate concurrency exceptions.
	    If you want to use that, you have to open the session with ForceReadFromMaster set to true.
	
	failover behavior can be found in `store.conventions`.</br >
	To change the failover behavior just use the following code. do it before you initailze the store:
	
	```python
	from pyravendb.store import document_store
	from pyravendb.data import document_convention
	
	store = document_store.documentstore(url="http://localhost:8080", database="PyRavenDB")
	store.conventions.failover_behavior = document_convention.Failover.fail_immediately
	store.initialize()
	```
	
##### API Key authentication
PyRavenDB also supports API Keys authentication.</br>
The ApiKey is a string in format apiKeyName/apiKeySecret.</br>
To authenticate the user by using API keys we need to create a document with Raven/ApiKeys/apiKeyName as a key and a dict  as a content on the system database with the following structure:
* <B>Name</B> : apiKeyName
* <B>Secret</B> : ThisIsMySecret
* <B>Enabled:</B> True or False
* <B>Databases</B> = A list with one or several dicts with the following structure:
	* <B>Admin</B>: True or False (False as a default, not a must)
	* <B>TenantId</B>: The database Id (* for all, must)
	* <B>ReadOnly</B>: True or False (False as a default, not a must)

First we open a store to the system database (system database must be declared explicitly) and then use `database_commands.put()` to put the document into the system database:
```python
with document_store.documentstore(url="http://localhost:8080", database = "system") as store:
	store.initialize()
    store.database_commands.put("Raven/ApiKeys/sample", {"Name": "sample", "Secret": "ThisIsMySecret", 
    							"Enabled": True, "Databases":[{"TenantId": "*"}, {"TenantId": "<system>","ReadOnly": True}]})

```

Now, to perform any actions against specified database , we need to provide the API key</br>
(in our example it will be `"sample/ThisIsMySecret"`).

```
store = document_store.documentstore(url="http://localhost:8080", database = "PyRavenDB", api_key = "sample/ThisIsMySecret")
store.initialize()
```
