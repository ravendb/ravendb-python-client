## Overview 
PyRavenDB is a python client api for [RavenDB](https://ravendb.net/), a NoSQL document database.
The API can handle most CRUD scenarios, including full support for replication, failover, dynamic queries, etc.


```
with document_store.documentstore(url="http://localhost:8080", database="PyRavenDB") as store:
store.initialize()
with store.open_session() as session:
foo = session.load("foos/1")
```

## Installation
There are three ways to install pyravendb.

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
##### Load a single or several document\s from the store:
 ```
    from pyravendb.store import document_store
    
    store =  document_store.documentstore(url="http://localhost:8080", database="PyRavenDB")
    store.initialize() 
    with store.open_session() as session:
    	foo = session.load("foos/1")
```

load method have the option to track entity for you the only thing you need to do is add ```object_type```  when you call to load 
(load method will return a dynamic_structure object by default) for class with nested object you can call load with ```nested_object_types``` dictionary for the other types. just put in the key the name of the object and in the value his class (without this option you will get the document as it is) .

```
	foo = session.load("foos/1", object_type=Foo)
```

```
	class FooBar(object):
		def __init__(self,name,foo):
			self.name = name
			self.foo = foo
	
	foo_bar = session.load("FooBars/1", object_type=FooBar,nested_object_types={"foo":Foo})
			
```
To load several documents at once, supply a list of ids to session.load.

```
	foo = session.load(["foos/1", "foos/2", "foos/3"], object_type=Foo)
```

##### Delete a document
To delete a document from the store,  use ```session.delete()``` with the id or entity you would like to delete.

```
with store.open_session() as session:
       foo = session.delete("foos/1")
```

##### Store a new document
to store a new document, use ```session.store(entity)``` with the entity you would like to store (entity must be an object)
For storing with dict we will use database_commands the put command (see the source code for that).

```
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
	
```
with store.open_session() as session:
	query_result = list(session.query().where_equals("name", "test101")
	query_result = list(session.query(object_type=Foo).where_starts_with("name", "n"))
	query_result = list(session.query(object_type=Foo).where_ends_with("name", "7"))
	query_result = list(session.query(object_type=FooBar,nested_object_types={"foo":Foo}).where(name="foo_bar"))
	
```

You can also build the query with several options using the builder pattern.

```
with store.open_session() as session:
	list(session.query(wait_for_non_stale_results=True).where_not_none("name").order_by_descending("name"))
``` 

For the query you can also use the `where` feature which can get a variable number of arguments (**kwargs)
```
with store.open_session() as session:
	query_results = list(session.query().where(name="test101", key=[4, 6, 90]))

```
`name` and `key` are the field names for which we query

##### Includes
A list of the properties we like to include in the query or in load.
The include property wont show in our result but when we load or query for it we wont requests it from the server.
The includes will save on the session cache.
```
class FooBar(object):
	def __init__(name, foo_key)
    	self.name = name
        self.foo = foo_key

store =  document_store.DocumentStore(url="http://localhost:8080", database="PyRavenDB")
store.initialize() 
with store.open_session() as session:
	query_result = list(session.query(includes="foo").where_equals("name", "testing_includes")
    foo_bar = session.load("FooBars/1", object_type=FooBar, includes=foo)
    
```

##### Replication

Replication works using plain HTTP requests to replicate all changes from one server instance to another.
* Replication bundle is always enable just need to add destination

* setup a replication by creating the ```ReplicationDestinations``` document with appropriate settings.
	```
    with store.open_session("PyRavenDB") as session:
            replication_document = database.ReplicationDocument([database.ReplicationDestination(url="http://localhost:8080", database="destination_database_name")])
            session.store(replication_document)
            session.save_changes()
    ```
    
	###### Failover
	The failover in replication is divide to two operation read and write, and in each
	operation we choose the behavior for this operation in the case of failover.  
	
	<B>ReadBehavior</B> 
    * leader_only
    * leader_with_failover
    * leader_with_failover_when_request_time_sla_threshold_is_reached
    * round_robin
    * round_robin_failover_when_request_time_sla_threshold_is_reached
    * fastest_node 
     
    <B>WriteBehavior</B>
    * leader_only
    * leader_with_failover
	
	failover behavior can be found in `store.conventions`.</br >
