
# TBA

## Installation
Install from [PyPi](https://pypi.python.org/pypi), as [ravendb](https://pypi.python.org/pypi/ravendb).
```bash
pip install ravendb
````
## Introduction and changelog
Python client API (v5.2) for [RavenDB](https://ravendb.net/) , a NoSQL document database.


Although new API isn't compatible with the previous one, it comes with **many improvements and new features**.

**Package has been reworked to match Java and other RavenDB clients**

**Type-hinted entire project and API results** - using the API is now much more comfortable with IntelliSense


---

![](https://github.com/ravendb/ravendb-python-client/blob/v5.2beta1/.github/readme_content/typehints.gif)

---

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
![](https://github.com/ravendb/ravendb-python-client/blob/v5.2beta1/.github/readme_content/document_query.gif)

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
  - More lazy operations - querying and compare exchange
  - Select fields
  - Task related commands (crud for replication, subscriptions, etl) + connection strings
  - Counters, Time Series
  - Streams and Subscriptions


The client is still in the **beta** phase.

##### Bug Tracker
http://issues.hibernatingrhinos.com/issues/RDBC
