
# Official Python client for RavenDB NoSQL Database 🐍

## Installation
Install from [PyPi](https://pypi.python.org/pypi), as [ravendb](https://pypi.org/project/ravendb).
```bash
pip install ravendb
````

## Introduction and changelog
Python client API (v7.0) for [RavenDB](https://ravendb.net/), a NoSQL document database.

**Type-hinted entire project and API results** - using the API is now much more comfortable with IntelliSense

## Releases

* [Click here](https://github.com/ravendb/ravendb-python-client/releases) to view all Releases and Changelog.

---
![](.github/readme_content/typehints.gif)

---

## What's new?

###### 5.2.5+
- Changes available in the [releases](https://github.com/ravendb/ravendb-python-client/releases) section.

###### 5.2.4
- Bulk insert dependencies [bugfix](https://github.com/ravendb/ravendb-python-client/pull/184) 

###### 5.2.3
- **Counters**
- Counters indexes

###### 5.2.2
- New feature - **[Bulk insert](https://github.com/ravendb/ravendb-python-client/pull/161)**
- Bugfixes - Cluster-wide operations ([here](https://github.com/ravendb/ravendb-python-client/pull/166))

###### 5.2.1
- Bugfixes - Serialization while loading/querying ([here](https://github.com/ravendb/ravendb-python-client/pull/163))

###### 5.2.0
- **Subscriptions**
  - Document streams
  - Secured subscriptions


- **Querying**
  - Major bugfixes
  - Spatial querying and indexing
  - Highlighting fixes
  - **Custom document parsers & loaders**

###### 5.2.0b3
- **New features**
  - Conditional Load
  - SelectFields & Facets
  - Projections
  - MoreLikeThis
  - Suggestions


- **Improvements**
  - Compare exchange
  - Querying
  - DocumentConventions
  - Patching
  - Spatial queries
  - Aggregations


###### 5.2.0b2

- **Lazy Operations**
  - Lazy loading
  - Lazy querying
  - Lazy compare exchange operations


- **Structure**
  - Important classes are now available to import from the top level `ravendb` module

...and many bugfixes



---

###### 5.2.0b1

- **Querying** 
  - Simpler, well type hinted querying
  - Group by, aggregations
  - Spatial querying
  - Boost, fuzzy, proximity
  - Subclauses support
 
- **Static Indexes**
  - Store fields, index fields, pick analyzers & more using `AbstractIndexCreationTask`
  - Full indexes CRUD
  - Index related commands (priority, erros, start/stop, pause, lock)
  - Additional assemblies, map-reduce, index query with results "of_type" 
  
    
- **CRUD**
  - Type hints for results and includes
  - Support for dataclasses
 
----

#### Querying features
![](.github/readme_content/document_query.gif)

----

- **Attachments**
  - New attachments API
  - Better type hints 


- **HTTPS**
  - Support for https connection
  - Certificates CRUD operations


- **Lazy load**
  - New feature


- **Cluster Transactions, Compare Exchange**
  - New feature
  

### **Coming soon, work in progress**
  - Replication & ETL Commands
  - Streaming (https://github.com/ravendb/ravendb-python-client/pull/168)

----

## Documentation

* This readme provides short examples for the following:  
   [Getting started](#getting-started),  
   [Crud example](#crud-example),  
   [Query documents](#query-documents),  
   [Attachments](#attachments),  
   [Changes API](#changes-api),  
   [Suggestions](#suggestions),  
   [Patching](#advanced-patching),  
   [Subscriptions](#subscriptions),  
   [Counters](#counters),  
   [Bulk Insert](#bulk-insert),  
   [Using classes](#using-classes-for-entities),  
   [Working with secure server](#working-with-a-secure-server),  
   [Building & running tests](#building)  

* For more information go to the online [RavenDB Documentation](https://ravendb.net/docs/article-page/latest/nodejs/client-api/what-is-a-document-store).

## Getting started

1. Import the `DocumentStore` class from the ravendb module
```python
from ravendb import DocumentStore
```

2. Initialize the document store (you should have a single DocumentStore instance per application)
```python
store = DocumentStore('http://live-test.ravendb.net', 'databaseName')
store.initialize()
```

3. Open a session
```python
with store.open_session() as session:
```

4. Call `save_changes()` when you're done
```python
    user = session.load('users/1-A') 
    user.name = "Gracjan"
    session.save_changes()
    
# Data is now persisted
# You can proceed e.g. finish web request
```

## CRUD example

### Store documents

```python
product = Product(
    Id=None,
    title='iPhone 14 Pro Max',
    price=1199.99,
    currency='USD',
    storage=256,
    manufacturer='Apple',
    in_stock=True,
    last_update=datetime.datetime.now(),
)

session.store(product, 'products/1-A')
print(product.Id) # products/1-A
session.save_changes()
```


>##### Some related tests:
> <small>[store()](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/crud_tests/test_store.py#L8)</small>  
> <small>[ID generation - session.store()](https:///github.com/ravendb/ravendb-python-client/blob/2fea44a04d83f7419ef85b8c069188f7002dd231/ravendb/tests/session_tests/test_store_entities.py#L26)</small>  
> <small>[store document with @metadata](https:///github.com/ravendb/ravendb-python-client/blob/2fea44a04d83f7419ef85b8c069188f7002dd231/ravendb/tests/session_tests/test_store_entities.py#L49-L59)</small>  
> <small>[storing docs with same ID in same session should throw](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/crud_tests/test_track_entity.py#L9)</small>  


### Load documents

```python
product = session.load('products/1-A')
print(product.title) # iPhone 14 Pro Max
print(product.Id)    # products/1-A
```

> ##### Related tests:
> <small>[load()](https:///github.com/ravendb/ravendb-python-client/blob/1c8322f7b31245fdf5a36a6b938ebb813ff09aed/ravendb/tests/session_tests/test_load.py#L77)</small>

### Load documents with include

```python
# users/1
# {
#      "name": "John",
#      "kids": ["users/2", "users/3"]
# }

session = store.open_session()
user1 = session.include("kids").load("users/1")
  # Document users/1 and all docs referenced in "kids"
  # will be fetched from the server in a single request.

user2 = session.load("users/2") 
# this won't call server again

assert(user1 is not None)
assert(user2 is not None)
assert(session.advanced.number_of_requests == 1)
```

>##### Related tests:
> <small>[can load with includes](https:///github.com/ravendb/ravendb-python-client/blob/2fea44a04d83f7419ef85b8c069188f7002dd231/ravendb/tests/jvm_migrated_tests/crud_tests/test_load.py#L124)</small>  

### Update documents

```python
import datetime

product = session.load('products/1-A')
product.in_stock = False
product.last_update = datetime.datetime.now()
session.save_changes()
# ...
product =  session.load('products/1-A')
print(product.in_stock) # false
print(product.last_update) # the current date
```

>##### Related tests:
> <small>[update document](https:///github.com/ravendb/ravendb-python-client/blob/09734a3ee40b81ef9ab1c6eef61f2dfa0054a7e0/ravendb/tests/jvm_migrated_tests/crud_tests/test_crud.py#L272)</small>  
> <small>[update document metadata](https:///github.com/ravendb/ravendb-python-client/blob/2fea44a04d83f7419ef85b8c069188f7002dd231/ravendb/tests/jvm_migrated_tests/metadata_tests/test_RavenDB_10641.py#L13)</small>

### Delete documents

1. Using entity
```python
product =  session.load('products/1-A')
session.delete(product)
session.save_changes()

product = session.load('products/1-A')
print(product) # None
```

2. Using document ID
```python
 session.delete('products/1-A')
```

>##### Related tests:
> <small>[delete doc by entity](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/crud_tests/test_delete.py#L8)</small>  
> <small>[delete doc by ID](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/crud_tests/test_delete.py#L22)</small>  
> <small>[cannot delete after change](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/session_tests/test_delete.py#L50-L56)</small>  
> <small>[loading deleted doc returns null](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/crud_tests/test_track_entity.py#L25)</small>

## Query documents

1. Use `query()` session method:  

Query by collection:

```python
import datetime
from ravendb import DocumentStore, QueryOperator

store = DocumentStore()
store.initialize()

session = store.open_session()
session.query_collection(str()).not_()
```
Query by index name:  
```python
query = session.query_index("productsByCategory")
```
Query by index:
```python
query = session.query_index_type(Product_ByName, Product) # the second argument (object_type) is optional, as always
```
Query by entity type:  
```python
query = session.query(object_type=User) # object_type is an optional argument, but we can call this method only with it 
```

2. Build up the query - apply search conditions, set ordering, etc.  
   Query supports chaining calls:
```python
query = session.query_collection("Users") 
    .wait_for_non_stale_results() 
    .using_default_operator(QueryOperator.AND) 
    .where_equals("manufacturer", "Apple")
    .where_equals("in_stock", True)
    .where_between("last_update", datetime.datetime(2022,11,1), datetime.datetime.now()).order_by("price")
```

3. Execute the query to get results:
```python
results =  list(query) # get all results
# ...
first_result =  query.first() # gets first result
# ...
single =  query.single()  # gets single result 
```

### Query methods overview

#### select_fields() - projections using a single field

```python
# RQL
# from users select name

# Query
class UserProj:
    def __init__(self, name: str = None, age: int = None):
        self.name = name
        self.age = age


user_names = [user_proj.name for user_proj in session.query_collection("Users").select_fields(UserProj, "name")]

# Sample results
# John, Stefanie, Thomas
```

#### select_fields() - projections using multiple fields
```python
# RQL
# from users select name, age

# Query
 results = list(session.query_collection("Users").select_fields(UserProj, "name", "age"))
    

# Sample results
# [ { name: 'John', age: 30 },
#   { name: 'Stefanie', age: 25 },
#   { name: 'Thomas', age: 25 } ]
```

>##### Related tests:
> <small>[query with projections (query only two fields)](https:///github.com/ravendb/ravendb-python-client/blob/3aae2bce066d1559e6d574ce7fa137b80b9ef920/ravendb/tests/jvm_migrated_tests/client_tests/test_query.py#L138-L158)</small>  
> <small>[can_project_id_field](https:///github.com/ravendb/ravendb-python-client/blob/bbe89b7c71d1b7e289085420a5531fa6662fca6e/ravendb/tests/jvm_migrated_tests/crud_tests/test_RavenDB_14811.py#L49-L86)</small>

#### distinct()
```python
# RQL
# from users select distinct age

# Query
 [user_proj.age for user_proj in session.query_collection("Users").select_fields(UserProj, "age").distinct()]
    

# Sample results
# [ 30, 25 ]
```


#### where_equals() / where_not_equals()
```python
# RQL
# from users where age = 30 

# Query
 list(session.query_collection("Users").where_equals("age", 30))

# Saple results
# [ User {
#    name: 'John',
#    age: 30,
#    kids: [...],
#    registered_at: 2017-11-10T23:00:00.000Z } ]
```

>##### Related tests:
> <small>[where equals](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L135-L148)</small>  
> <small>[where not equals](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L237-L244)</small>

#### where_in()
```python
# RQL
# from users where name in ("John", "Thomas")

# Query
list(session.query_collection("Users").where_in("name", ["John", "Thomas"]))

# Sample results
# [ User {
#     name: 'John',
#     age: 30,
#     registered_at: 2017-11-10T23:00:00.000Z,
#     kids: [...],
#     id: 'users/1-A' },
#   User {
#     name: 'Thomas',
#     age: 25,
#     registered_at: 2016-04-24T22:00:00.000Z,
#     id: 'users/3-A' } ]
```

>##### Related tests:
> <small>[where in](https:///github.com/ravendb/ravendb-python-client/blob/333a2961de1f924ea7b42529eed64d2317ef4891/ravendb/tests/session_tests/test_query.py#L110-L116)</small>  
> <small>[query with where in](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L150-L154)</small>


#### where_starts_with() / where_ends_with()
```python
# RQL
# from users where startsWith(name, 'J')

# Query
list(session.query_collection("Users").where_starts_with("name", "J"))

# Sample results
# [ User {
#    name: 'John',
#    age: 30,
#    kids: [...],
#    registered_at: 2017-11-10T23:00:00.000Z } ]
```

>##### Related tests:
> <small>[query with where clause](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L135-L148)</small>

#### where_between()

```python
# RQL
# from users where registeredAt between '2016-01-01' and '2017-01-01'

# Query
import datetime

list(session.query_collection("Users").where_between("registered_at", datetime.datetime(2016, 1, 1), datetime.datetime(2017,1,1)))

# Sample results
# [ User {
#     name: 'Thomas',
#     age: 25,
#     registered_at: 2016-04-24T22:00:00.000Z,
#     id: 'users/3-A' } ]
```

>##### Related tests:
> <small>[where between](https:///github.com/ravendb/ravendb-python-client/blob/333a2961de1f924ea7b42529eed64d2317ef4891/ravendb/tests/session_tests/test_query.py#L141-L148)</small>  
> <small>[query with where between](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L156-L161)</small>

#### where_greater_than() / where_greater_than_or_equal() / where_less_than() / where_less_than_or_equal()
```python
# RQL
# from users where age > 29

# Query
list(session.query_collection("Users").where_greater_than("age", 29))

# Sample results
# [ User {
#   name: 'John',
#   age: 30,
#   registered_at: 2017-11-10T23:00:00.000Z,
#   kids: [...],
#   id: 'users/1-A' } ]
```

>##### Related tests:
> <small>[query with where less than](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L163-L168)</small>  
> <small>[query with where less than or equal](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L170-L174)</small>  
> <small>[query with where greater than](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L176-L181)</small>  
> <small>[query with where greater than or equal](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L183-L187)</small>  

#### where_exists()
Checks if the field exists.
```python
# RQL
# from users where exists("age")

# Query
session.query_collection("Users").where_exists("kids")

# Sample results
# [ User {
#   name: 'John',
#   age: 30,
#   registered_at: 2017-11-10T23:00:00.000Z,
#   kids: [...],
#   id: 'users/1-A' } ]
```

>##### Related tests:
> <small>[query where exists](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L253-L268)</small>

#### contains_any() / contains_all()
```python
# RQL
# from users where kids in ('Mara')

# Query
list(session.query_collection("Users").contains_all("kids", ["Mara", "Dmitri"]))

# Sample results
# [ User {
#   name: 'John',
#   age: 30,
#   registered_at: 2017-11-10T23:00:00.000Z,
#   kids: ["Dmitri", "Mara"]
#   id: 'users/1-A' } ]
```

>##### Related tests:
> <small>[queries with contains any/all](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_contains.py#L13-L36)</small>

#### search()
Perform full-text search.
```python
# RQL
# from users where search(kids, 'Mara')

# Query
 list(session.query_collection("Users").search("kids", "Mara Dmitri"))

# Sample results
# [ User {
#   name: 'John',
#   age: 30,
#   registered_at: 2017-11-10T23:00:00.000Z,
#   kids: ["Dmitri", "Mara"]
#   id: 'users/1-A' } ]
```

>##### Related tests:
> <small>[search()](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/session_tests/test_full_text_search.py#L66-L71)</small>  
> <small>[query search with or](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/session_tests/test_full_text_search.py#L73-L85)</small>  

#### open_subclause() / close_subclause()
```python
# RQL
# from users where exists(kids) or (age = 25 and name != Thomas)

# Query
list(session.query_collection("Users").where_exists("kids").or_else()
    .open_subclause()
    .where_equals("age", 25)
    .where_not_equals("name", "Thomas")
    .close_subclause())

# Sample results
# [ User {
#     name: 'John',
#     age: 30,
#     registered_at: 2017-11-10T23:00:00.000Z,
#     kids: ["Dmitri", "Mara"]
#     id: 'users/1-A' },
#   User {
#     name: 'Stefanie',
#     age: 25,
#     registered_at: 2015-07-29T22:00:00.000Z,
#     id: 'users/2-A' } ]
```

>##### Related tests:
> <small>[working with subclause](https:///github.com/ravendb/ravendb-python-client/blob/9d8fb25cdb65607ab31ed9635cc32e90c4443f71/ravendb/tests/jvm_migrated_tests/issues_tests/test_ravenDB_5669.py#L36-L56)</small>

#### not_()
```python
# RQL
# from users where age != 25

# Query
list(session.query_collection("Users").not_().where_equals("age", 25))


# Sample results
# [ User {
#   name: 'John',
#   age: 30,
#   registered_at: 2017-11-10T23:00:00.000Z,
#   kids: ["Dmitri", "Mara"]
#   id: 'users/1-A' } ]
```

>##### Related tests:
> <small>[query where not](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L237-L244)</small>

#### or_else() / and_also()
```python
# RQL
# from users where exists(kids) or age < 30

# Query
list(session.query_collection("Users")
    .where_exists("kids")
    .or_else()
    .where_less_than("age", 30))

# Sample results
#  [ User {
#     name: 'John',
#     age: 30,
#     registered_at: 2017-11-10T23:00:00.000Z,
#     kids: [ 'Dmitri', 'Mara' ],
#     id: 'users/1-A' },
#   User {
#     name: 'Thomas',
#     age: 25,
#     registered_at: 2016-04-24T22:00:00.000Z,
#     id: 'users/3-A' },
#   User {
#     name: 'Stefanie',
#     age: 25,
#     registered_at: 2015-07-29T22:00:00.000Z,
#     id: 'users/2-A' } ]
```

>##### Related tests:
> <small>[working with subclause](https:///github.com/ravendb/ravendb-python-client/blob/9d8fb25cdb65607ab31ed9635cc32e90c4443f71/ravendb/tests/jvm_migrated_tests/issues_tests/test_ravenDB_5669.py#L36-L56)</small>

#### using_default_operator()
If neither `and_also()` nor `or_else()` is called then the default operator between the query filtering conditions will be `AND` .  
You can override that with `using_default_operator` which must be called before any other where conditions.
```python
# RQL
# from users where exists(kids) or age < 29
# Query
from ravendb import QueryOperator

list(session.query_collection("Users")
    .using_default_operator(QueryOperator.OR) # override the default 'AND' operator
    .where_exists("kids")
    .where_less_than("age", 29))

# Sample results
#  [ User {
#     name: 'John',
#     age: 30,
#     registered_at: 2017-11-10T23:00:00.000Z,
#     kids: [ 'Dmitri', 'Mara' ],
#     id: 'users/1-A' },
#   User {
#     name: 'Thomas',
#     age: 25,
#     registered_at: 2016-04-24T22:00:00.000Z,
#     id: 'users/3-A' },
#   User {
#     name: 'Stefanie',
#     age: 25,
#     registered_at: 2015-07-29T22:00:00.000Z,
#     id: 'users/2-A' } ]
```

#### order_by() / order_by_desc() / order_by_score() / random_ordering()
```python
# RQL
# from users order by age

# Query
list(session.query_collection("Users").order_by("age"))

# Sample results
# [ User {
#     name: 'Stefanie',
#     age: 25,
#     registered_at: 2015-07-29T22:00:00.000Z,
#     id: 'users/2-A' },
#   User {
#     name: 'Thomas',
#     age: 25,
#     registered_at: 2016-04-24T22:00:00.000Z,
#     id: 'users/3-A' },
#   User {
#     name: 'John',
#     age: 30,
#     registered_at: 2017-11-10T23:00:00.000Z,
#     kids: [ 'Dmitri', 'Mara' ],
#     id: 'users/1-A' } ]
```

>##### Related tests:
> <small>[order_by()](https:///github.com/ravendb/ravendb-python-client/blob/333a2961de1f924ea7b42529eed64d2317ef4891/ravendb/tests/session_tests/test_query.py#L149-L158)</small>  
> <small>[order_by_desc()](https:///github.com/ravendb/ravendb-python-client/blob/333a2961de1f924ea7b42529eed64d2317ef4891/ravendb/tests/session_tests/test_query.py#L160-L168)</small>  
> <small>[query random order](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L270-L274)</small>  
> <small>[order by AlphaNumeric](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/issues_tests/test_ravenDB_15825.py#L42-L50)</small>  
> <small>[query with boost - order by score](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L276-L303)</small>

#### take()
Limit the number of query results.
```python
# RQL
# from users order by age

# Query
 list(session.query_collection("Users")
    .order_by("age") 
    .take(2)) # only the first 2 entries will be returned

# Sample results
# [ User {
#     name: 'Stefanie',
#     age: 25,
#     registered_at: 2015-07-29T22:00:00.000Z,
#     id: 'users/2-A' },
#   User {
#     name: 'Thomas',
#     age: 25,
#     registered_at: 2016-04-24T22:00:00.000Z,
#     id: 'users/3-A' } ]
```

>##### Related tests:
> <small>[query skip take](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L246-L251)</small>  

#### skip()
Skip a specified number of results from the start.
```python
# RQL
# from users order by age

# Query
list(session.query_collection("Users").order_by("age") 
    .take(1) # return only 1 result
    .skip(1)) # skip the first result, return the second result

# Sample results
# [ User {
#     name: 'Thomas',
#     age: 25,
#     registered_at: 2016-04-24T22:00:00.000Z,
#     id: 'users/3-A' } ]
```

>##### Related tests:
> <small>[raw query skip take](https:///github.com/ravendb/ravendb-python-client/blob/3aae2bce066d1559e6d574ce7fa137b80b9ef920/ravendb/tests/jvm_migrated_tests/client_tests/test_query.py#L82-L88)</small>  

#### Getting query statistics
Use the `statistics()` method to obtain query statistics.  
```python
# Query
statistics: QueryStatistics = None
def __statistics_callback(stats: QueryStatistics):
    nonlocal statistics
    statistics = stats  # plug-in the reference, value will be changed later

results = list(session.query_collection("Users")
               .where_greater_than("age", 29)
               .statistics(__statistics_callback))

# Sample results
# QueryStatistics {
#   is_stale: false,
#   duration_in_ms: 744,
#   total_results: 1,
#   skipped_results: 0,
#   timestamp: 2018-09-24T05:34:15.260Z,
#   index_name: 'Auto/users/Byage',
#   index_timestamp: 2018-09-24T05:34:15.260Z,
#   last_query_time: 2018-09-24T05:34:15.260Z,
#   result_etag: 8426908718162809000 }
```

>##### Related tests:
> <small>[can get stats in aggregation query](https:///github.com/ravendb/ravendb-python-client/blob/edbd2951402daa42c2fb2bb0ff51cd5c0d6e6e23/ravendb/tests/jvm_migrated_tests/issues_tests/test_ravenDB_12902.py#L174-L187)</small>  

####) / first() / single() / count()
)` - returns all results

`first()` - first result only

`single()` - first result, throws error if there's more entries

`count()` - returns the number of entries in the results (not affected by `take()`)

>##### Related tests:
> <small>[query first and single](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L219-L227)</small>  

## Attachments

#### Store attachments
```python
doc = User(name="John")

# Store a document, the entity will be tracked.
session.store(doc)

with open("photo.png", "rb+") as file:
    session.advanced.attachments.store(doc, "photo.png", file.read(), "image/png")

# OR store attachment using document ID
session.advanced.attachments.store(doc.Id, "photo.png", file.read(), "image/png")

# Persist all changes
session.save_changes()
```

>##### Related tests:
> <small>[can put attachments](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/jvm_migrated_tests/query_tests/test_query.py#L219-L227)</small>  

#### Get attachments
```python
# Get an attachment
attachment =  session.advanced.attachments.get(document_id, "photo.png")

# Attachment.details contains information about the attachment:
#     { 
#       name: 'photo.png',
#       document_id: 'users/1-A',
#       content_type: 'image/png',
#       hash: 'MvUEcrFHSVDts5ZQv2bQ3r9RwtynqnyJzIbNYzu1ZXk=',
#       change_vector: '"A:3-K5TR36dafUC98AItzIa6ow"',
#       size: 4579 
#     }

```

>##### Related tests:
> <small>[can get & delete attachments](https:///github.com/ravendb/ravendb-python-client/blob/2fea44a04d83f7419ef85b8c069188f7002dd231/ravendb/tests/jvm_migrated_tests/attachments_tests/test_attachments_session.py#L110-L155)</small>

#### Check if attachment exists
```python
session.advanced.attachments.exists(doc.Id, "photo.png")
# True

session.advanced.attachments.exists(doc.Id, "not_there.avi")
# False
```

>##### Related tests:
> <small>[attachment exists](https:///github.com/ravendb/ravendb-python-client/blob/2fea44a04d83f7419ef85b8c069188f7002dd231/ravendb/tests/jvm_migrated_tests/attachments_tests/test_attachments_session.py#L156-L168)</small>  

#### Get attachment names
```python
# Use a loaded entity to determine attachments' names
session.advanced.attachments.get_names(doc)

# Sample results:
# [ { name: 'photo.png',
#     hash: 'MvUEcrFHSVDts5ZQv2bQ3r9RwtynqnyJzIbNYzu1ZXk=',
#     content_type: 'image/png',
#     size: 4579 } ]
```
>##### Related tests:
> <small>[get attachment names](https:///github.com/ravendb/ravendb-python-client/blob/2fea44a04d83f7419ef85b8c069188f7002dd231/ravendb/tests/jvm_migrated_tests/attachments_tests/test_attachments_session.py#L67-L92)</small>  

## Changes API

Listen for database changes e.g. document changes.

```python
# Subscribe to change notifications
changes = store.changes()

all_documents_changes = []

# Subscribe for all documents, or for specific collection (or other database items)
all_observer = self.store.changes().for_all_documents()

close_method = all_observer.subscribe_with_observer(ActionObserver(on_next=all_documents_changes.append))
all_observer.ensure_subscribe_now()

session = store.open_session()
session.store(User("Starlord"))
session.save_changes()

# ...
# Dispose the changes instance when you're done
close_method()
```

>##### Related tests:
> <small>[can obtain single document changes](https:///github.com/ravendb/ravendb-python-client/blob/be5e5934f3a0820e5f67ecd3a1d1ffb518d31961/ravendb/tests/jvm_migrated_tests/server_tests/documents/notifications/test_changes.py#L110-L148)</small>  
> <small>[can obtain all documents changes](https:///github.com/ravendb/ravendb-python-client/blob/be5e5934f3a0820e5f67ecd3a1d1ffb518d31961/ravendb/tests/jvm_migrated_tests/server_tests/documents/notifications/test_changes.py#L35-L71)</small>  
> <small>[can obtain notification about documents starting with](https:///github.com/ravendb/ravendb-python-client/blob/e674fb939bb51ff72c949cf8e6751298b457bffb/ravendb/tests/database_changes/test_database_changes.py#L164-L185)</small>  

## Suggestions

Suggest options for similar/misspelled terms

```python
from ravendb.documents.indexes.definitions import FieldIndexing
from ravendb import AbstractIndexCreationTask
# Some documents in users collection with misspelled name term
# [ User {
#     name: 'Johne',
#     age: 30,
#     ...
#     id: 'users/1-A' },
#   User {
#     name: 'Johm',
#     age: 31,
#     ...
#     id: 'users/2-A' },
#   User {
#     name: 'Jon',
#     age: 32,
#     ...
#     id: 'users/3-A' },
# ]

# Static index definition
class UsersIndex(AbstractIndexCreationTask):
    def __init__(self):
        super().__init__()
        self.map = "from u in docs.Users select new { u.name }"
        # Enable the suggestion feature on index-field 'name'
        self._index("name", FieldIndexing.SEARCH)
        self._index_suggestions.add("name")

# ...
session = store.open_session()

# Query for similar terms to 'John'
# Note: the term 'John' itself will Not be part of the results

suggestedNameTerms =  list(session.query_index_type(UsersIndex, User)
    .suggest_using(lambda x: x.by_field("name", "John")) 
    .execute())

# Sample results:
# { name: { name: 'name', suggestions: [ 'johne', 'johm', 'jon' ] } }
```

>##### Related tests:
> <small>[can suggest]()</small>  
> <small>[canChainSuggestions](https:///github.com/ravendb/ravendb-python-client/blob/69abf78d5f583490d079eab99f1e80795e3c4391/ravendb/tests/jvm_migrated_tests/issues_tests/test_ravenDB_9584.py#L55-L68)</small>  
> <small>[canUseAliasInSuggestions](https:///github.com/ravendb/ravendb-python-client/blob/69abf78d5f583490d079eab99f1e80795e3c4391/ravendb/tests/jvm_migrated_tests/issues_tests/test_ravenDB_9584.py#L70-L79)</small>  
> <small>[canUseSuggestionsWithAutoIndex](https:///github.com/ravendb/ravendb-python-client/blob/69abf78d5f583490d079eab99f1e80795e3c4391/ravendb/tests/jvm_migrated_tests/issues_tests/test_ravenDB_9584.py#L42-L54)</small>  
> <small>[can suggest using linq](https:///github.com/ravendb/ravendb-python-client/blob/44dd51d75518aeef3a4a8c66434ef5d5e058ccc1/ravendb/tests/jvm_migrated_tests/suggestions_tests/test_suggestions.py#L115-L124)</small>  
> <small>[can suggest using multiple words](https:///github.com/ravendb/ravendb-python-client/blob/44dd51d75518aeef3a4a8c66434ef5d5e058ccc1/ravendb/tests/jvm_migrated_tests/suggestions_tests/test_suggestions.py#L86-L100)</small>  
> <small>[can get suggestions with options](https:///github.com/ravendb/ravendb-python-client/blob/44dd51d75518aeef3a4a8c66434ef5d5e058ccc1/ravendb/tests/jvm_migrated_tests/suggestions_tests/test_suggestions.py#L126-L138)</small>  

## Advanced patching

```python
# Increment 'age' field by 1
session.advanced.increment("users/1", "age", 1)

# Set 'underAge' field to false
session.advanced.patch("users/1", "underAge", False)

session.save_changes()
```

>##### Related tests:
> <small>[can patch](https:///github.com/ravendb/ravendb-python-client/blob/7165c5833b0a943478b029c13aee998aece078cf/ravendb/tests/jvm_migrated_tests/client_tests/test_first_class_patch.py#L78)</small>  
> <small>[can patch complex](https:///github.com/ravendb/ravendb-python-client/blob/7165c5833b0a943478b029c13aee998aece078cf/ravendb/tests/jvm_migrated_tests/client_tests/test_first_class_patch.py#L121)</small>  
> <small>[can add to array](https:///github.com/ravendb/ravendb-python-client/blob/7165c5833b0a943478b029c13aee998aece078cf/ravendb/tests/jvm_migrated_tests/client_tests/test_first_class_patch.py#L161)</small>  
> <small>[can increment](https:///github.com/ravendb/ravendb-python-client/blob/7165c5833b0a943478b029c13aee998aece078cf/ravendb/tests/jvm_migrated_tests/client_tests/test_first_class_patch.py#L226)</small>  
> <small>[patchWillUpdateTrackedDocumentAfterSaveChanges](https:///github.com/ravendb/ravendb-python-client/blob/2fea44a04d83f7419ef85b8c069188f7002dd231/ravendb/tests/jvm_migrated_tests/issues_tests/test_ravenDB_11552.py#L151)</small>  
> <small>[can patch multiple documents](https:///github.com/ravendb/ravendb-python-client/blob/a3221a60dcf8b8719e5706f34addb40d85582977/ravendb/tests/jvm_migrated_tests/client_tests/test_patch.py#L10)</small>  

## Subscriptions

```python
# Create a subscription task on the server
# Documents that match the query will be send to the client worker upon opening a connection
from ravendb import DocumentStore
from ravendb.documents.subscriptions.worker import SubscriptionBatch
from ravendb.documents.subscriptions.options import SubscriptionCreationOptions, SubscriptionWorkerOptions

store = DocumentStore("http://live-test.ravendb.net", "TestDatabase")
store.initialize()

subscription_name = store.subscriptions.create_for_options(SubscriptionCreationOptions(query="from users where age >= 30"))

# Open a connection
# Create a subscription worker that will consume document batches sent from the server
# Documents are sent from the last document that was processed for this subscription

with store.subscriptions.get_subscription_worker(SubscriptionWorkerOptions(subscription_name)) as subscription_worker:
    def __callback(x: SubscriptionBatch):
        # Process the incoming batch items
        # Sample batch.items:
        # [ Item {
        #     change_vector: 'A:2-r6nkF5nZtUKhcPEk6/LL+Q',
        #     id: 'users/1-A',
        #     raw_result:
        #      { name: 'John',
        #        age: 30,
        #        registered_at: '2017-11-11T00:00:00.0000000',
        #        kids: [Array],
        #        '@metadata': [Object],
        #        id: 'users/1-A' },
        #     rawMetadata:
        #      { '@collection': 'Users',
        #        '@nested-object-types': [Object],
        #        'Raven-Node-Type': 'User',
        #        '@change-vector': 'A:2-r6nkF5nZtUKhcPEk6/LL+Q',
        #        '@id': 'users/1-A',
        #        '@last-modified': '2018-10-18T11:15:51.4882011Z' },
        #     exception_message: undefined } ]
        # ...
    
    
    def __exception_callback(ex: Exception):
        # Handle exceptions here
    
    subscription_worker.add_on_unexpected_subscription_error(__exception_callback)
    subscription_worker.run(__callback)
```

>##### Related tests:
> <small>[can subscribe to index and document](https:///github.com/ravendb/ravendb-python-client/blob/e674fb939bb51ff72c949cf8e6751298b457bffb/ravendb/tests/database_changes/test_database_changes.py#L133-L163)</small>  
> <small>[should stream all documents](https:///github.com/ravendb/ravendb-python-client/blob/aa98bebc953561c4d5d1b94d4af809c9778137f7/ravendb/tests/jvm_migrated_tests/client_tests/subscriptions_tests/test_basic_subscription.py#L125-L161)</small>  
> <small>[should send all new and modified docs](https:///github.com/ravendb/ravendb-python-client/blob/aa98bebc953561c4d5d1b94d4af809c9778137f7/ravendb/tests/jvm_migrated_tests/client_tests/subscriptions_tests/test_basic_subscription.py#L331-L364)</small>  
> <small>[should respect max doc count in batch](https:///github.com/ravendb/ravendb-python-client/blob/aa98bebc953561c4d5d1b94d4af809c9778137f7/ravendb/tests/jvm_migrated_tests/client_tests/subscriptions_tests/test_basic_subscription.py#L79-L106)</small>  
> <small>[can disable subscription](https:///github.com/ravendb/ravendb-python-client/blob/aa98bebc953561c4d5d1b94d4af809c9778137f7/ravendb/tests/jvm_migrated_tests/client_tests/subscriptions_tests/test_basic_subscription.py#L540-L549)</small>  
> <small>[can delete subscription](https:///github.com/ravendb/ravendb-python-client/blob/aa98bebc953561c4d5d1b94d4af809c9778137f7/ravendb/tests/jvm_migrated_tests/client_tests/subscriptions_tests/test_basic_subscription.py#L293-L309)</small>  

## Counters 
There are many ways to play with counters. The most common path is to use session API (`session.counters_for()`).
```python

    with store.open_session() as session:
        user1 = User("Aviv1")
        user2 = User("Aviv2")
        session.store(user1, "users/1-A")
        session.store(user2, "users/2-A")
        session.save_changes()

    # storing counters via session API
    with store.open_session() as session:
        session.counters_for("users/1-A").increment("likes", 100)
        session.counters_for("users/1-A").increment("downloads", 500)
        session.counters_for("users/2-A").increment("votes", 1000)

        session.save_changes()

    # alternatively, loading counters via GetCountersOperation
    counters = store.operations.send(GetCountersOperation("users/1-A", ["likes", "downloads"])).counters
    
    # loading counters via session API
    with store.open_session() as session:
        user1_likes = session.counters_for("users/1-A").get("likes")

    # deleting counters via session API
    with store.open_session() as session:
        session.counters_for("users/1-A").delete("likes")
        session.counters_for("users/1-A").delete("downloads")
        session.counters_for("users/2-A").delete("votes")

        session.save_changes()
```


##### Playing with counters using CounterBatchOperation
```python

counter_operation = DocumentCountersOperation(document_id="users/1-A", operations=[])
counter_operation.add_operations(
    CounterOperation("Likes", counter_operation_type=CounterOperationType.INCREMENT, delta=4)
)
counter_operation.add_operations(
    CounterOperation(
        "Shares",
        counter_operation_type=CounterOperationType.INCREMENT,
        delta=422,
    )
)
counter_operation.add_operations(CounterOperation("Likes", counter_operation_type=CounterOperationType.DELETE))

counter_batch = CounterBatch(documents=[counter_operation])
results = self.store.operations.send(CounterBatchOperation(counter_batch))
```
>##### Related tests:
> <small>[incrementing counters](https:///github.com/ravendb/ravendb-python-client/blob/df4e92fbcfb07872e1d7cc920bff5196d19a3aa7/ravendb/tests/session_tests/test_counters.py#L24-L38)</small>  
> <small>[document counters operation](https:///github.com/ravendb/ravendb-python-client/blob/f5149b943959ffa4ad7afaeef07c17583b4cb2e6/ravendb/tests/jvm_migrated_tests/client_tests/counters_tests/test_counters_single_node.py#L18-L61)</small>  
> <small>[including counters](https:///github.com/ravendb/ravendb-python-client/blob/305459fbc4ba36c838a7fbde2b98d88d3aefa482/ravendb/tests/jvm_migrated_tests/client_tests/indexing_tests/counters_tests/test_basic_counters_indexes_strong_syntax.py#L29)</small>  
> <small>[counters indexes](https:///github.com/ravendb/ravendb-python-client/blob/4ac192652bf57d304e1b48032d9acee56f2590b8/ravendb/tests/counters_tests/test_query_on_counters.py#L683)</small>



## Bulk insert
Bulk insert is the efficient way to store a large amount of documents.

https://ravendb.net/docs/article-page/5.4/csharp/client-api/bulk-insert/how-to-work-with-bulk-insert-operation.

For example:

```python
foo_bar1 = FooBar("John Doe")
foo_bar2 = FooBar("Jane Doe")
foo_bar3 = FooBar("John")
foo_bar4 = FooBar("Jane")

with store.bulk_insert() as bulk_insert:
    bulk_insert.store(foo_bar1)
    bulk_insert.store(foo_bar2)
    bulk_insert.store_as(foo_bar3, "foobars/66")
    bulk_insert.store_as(foo_bar4, "foobars/99", MetadataAsDictionary({"some_metadata_value": 75}))
```
The 3rd insert will store document named "foobars/66".

The 4th insert will store document with custom name and extra metadata.

>##### Related tests:
> <small>[simple bulk insert](https:///github.com/ravendb/ravendb-python-client/blob/45cb934e8ecc45d935afe3dd1dc96b40f94bcb00/ravendb/tests/jvm_migrated_tests/client_tests/test_bulk_inserts.py#L18-L44)</small>  
> <small>[modifying metadata on bulk insert](https:///github.com/ravendb/ravendb-python-client/blob/45cb934e8ecc45d935afe3dd1dc96b40f94bcb00/ravendb/tests/jvm_migrated_tests/client_tests/test_bulk_inserts.py#L56-L68)</small>  

## Using classes for entities

1. Define your model as class.

```python
import datetime


class Product:
    def __init__(self, Id: str = None, title: str = None, price: int = None, currency: str = None, storage: int = None,
                 manufacturer: str = None, in_stock: bool = False, last_update: datetime.datetime = None):
        self.Id = Id
        self.title = title
        self.price = price
        self.currency = currency
        self.storage = storage
        self.manufacturer = manufacturer
        self.in_stock = in_stock
        self.last_update = last_update
```

2. To store a document pass its instance to `store()`.  
   The collection name will automatically be detected from the entity's class name.

```python
import datetime
from models import Product

product = Product(None, 'iPhone X', 999.99, 'USD', 64, 'Apple', True, datetime.datetime(2017,10,1))
product = session.store(product)
print(isinstance(product, Product))  # True
print('products/' in product.Id)  # True
session.save_changes()
```

3. Loading a document  
```python
product =  session.load('products/1-A')
print(isinstance(product, Product))     # True
print(product.Id)                       # products/1-A
```

4. Querying for documents  
```python
products =  list(session.query_collection("Products"))

for product in products:
    print(isinstance(product, Product))   # True
    print("products/" in product.Id)      # True
```  

P.S Python client does support `dataclasses`

>##### Related tests:
> <small>[using dataclasses](https:///github.com/ravendb/ravendb-python-client/blob/2fea44a04d83f7419ef85b8c069188f7002dd231/ravendb/tests/session_tests/test_store_entities.py#L101-L112)</small>  

## Working with secured server
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

## Running tests

```bash
# To run the suite, set the following environment variables:
# 
# - Location of RavenDB server binary:
# RAVENDB_TEST_SERVER_PATH="C:\\work\\test\\Server\\Raven.Server.exe" 
#
# - Certificates paths for tests requiring a secure server:
# RAVENDB_TEST_SERVER_CERTIFICATE_PATH="C:\\work\\test\\cluster.server.certificate.pfx"
# RAVENDB_TEST_CLIENT_CERTIFICATE_PATH="C:\\work\\test\\python.pem"
# RAVENDB_TEST_CA_PATH="C:\\work\\test\\ca.crt"
#
# - Certificate hostname: 
# RAVENDB_TEST_HTTPS_SERVER_URL="https://a.nodejstest.development.run:7326"
#

python -m unittest discover
```



---

#### RavenDB Documentation
https://ravendb.net/docs/article-page/5.3/python

-----
##### Bug Tracker
http://issues.hibernatingrhinos.com/issues/RDBC
