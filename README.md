mongo-migrator
==============

Based of mongo-migrator written by bist (show parent project).
Thank you very much for your work.

This script can be used to migrate from Oracle to Mongo.

Installation
-------------

Currently, mongo-migrator require python 3.x version.
Use classic setuptools for installation.

>  python setup.py install

Installation produce mongo-migrator executable.

Usage
------

In order to see all available options execute:

> mongo-migrator -h (or --help)

In order to use and migrate data it is needed a YAML Configuration file where are configured:

* oracle configuration params:
    - username: username to use on connection to Oracle
    - password: password to use on connection to Oracle
    - dsn: tnsname alias of the connection to use and present on tnsnames.ora. Note export TNSADMIN variable to define tnsnames.ora file location.

* mongo configuration params:
    - username: username to use on connection to mongo database
    - password: password to use on connection to mongo database
    - host: host of mongo database
    - db: database to use
    - auth_source: if present identify authentication source to use on connection.
    - port: port of mongo database.

* import functions: through mongo-migrator is possible define a function that is called for any row of the fetched data from defined query. This permit to retrieve data from others tables (on Oracle or Mongo) and customize row to store as Document.

In order to execute migration script it is needed define a query to execute and where is possible:

* identify target column (or key) of the mongo collection where a particular value of a column from Oracle database is stored. Note: it is important correct order from Oracle query and columns defined on migration script. For example if I want store value of COLUMN1 from Oracle to `column1` of mongo collection, configuration file will be write with "COLUMN1: column1".

* configure execution of custom function for any rows. For example for Oracle COLUMN1 it is possible elaborate data and store it on `column2` with an option like this:
        COLUMN2: column2 function myscript_function


Example of migration script
----------------------------

Oracle connection configuration:

````yaml
oracle_configuration:
   username: username_of_db
   password: password_of_db_user
   dsn: tnsname_alias
````

Mongo connection configuration:

````yaml
mongo_server:
   username: mongo_user
   password: mongo_pwd
   host: mongo_host
   db: mongo_db
   auth_source: mongo_auth_source
   port: mongo_port
````

Migration command:

````yaml
table_name: dbuser.table_name
    query: SELECT COLUMN1, COLUMN2 FROM dbuser.table_name
    collectionName: mongo_target_collection
    columns:
      COLUMN1: column1
      COLUMN2: column2
````

Migration command with custom Python function:

````yaml
table_name: dbuser.table_name2
    query: SELECT COLUMN1, COLUMN2 FROM dbuser.table_name2
    collectionName: mongo_target_collection2
    columns:
      COLUMN1: column1
      COLUMN2: column2 function parser_field
````

And python custom function:

````python
def parser_field(field, row=None, configuration=None,
                 mongo_column=None,
                 oracleConnection=None,
                 context=None,
                 mongoClient=None,
                 operator=None):

    print("Handle field", field, ' of column', mongo_column)

    # skip operator
    # if operator:
    #    operator.skip_column = True

    ans = '%s changed' % field

    # Store context field for retrieve it on
    # next row
    context['my_reusable_obj'] = 'XXXX'

    # Return value to store
    return ans
````
