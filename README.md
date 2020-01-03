# CQLish
This is a lightweight CQL shell with an embedded Cassandra.
It does not connect to an external Cassandra/DSE instance, like cqlsh does,
but rather spins up an embedded Cassandra instance.

This is good for learning, but not really appropriate for other uses.
Once you are famililar with CQL, you should move on to a real Cassandra/DSE
instance and develop your application.

## Overview
This application is built upon CassandraUnit to spin up an embedded
Cassandra instance.  It will run all Cassandra DML and DDL commands.

The interactive shell is implemented using jline2.  Mostly it will take
CQL DML and DDL operations and hand them off to the embedded Cassandra
and display the results.  If the command does not return data it will 
simply report 'OK'.  If data is returned it will be 'pretty-printed'.
Special cqlsh commands (like COPY) are not supported.  

#### Describe
`DESCRIBE` and `DESC`  have been implemented, though not exactly like cqlsh. 
Note that you need to end with a semicolon.
- `DESCRIBE keyspaces;` will list all keyspaces
- `DESCRIBE tables <keyspace>;` will list the tables in the specified keyspace
- `DESCRIBE tables;` will list the tables in the current working keyspace
- `DESCRIBE table <keyspace>.<table>;` will list the DDL for the specified table
- `DESCRIBE table <keyspace> <table>;` will list the DDL for the specified table
- `DESCRIBE table <table>;` will list the DDL for the table in the current working keyspace

#### Help
To get help, type `help`. To clear the buffer type CTRL-C. 
To exit type `exit` or `quit` (case insensitive).

#### Resetting the database
The application starts the first time with an empty database.
You can create keyspaces and tables and insert data, and the data 
will be preserved when you restart.  You can force a fresh database using 
the `-reset` option:
```
cqlish -reset true
```

#### CQL files
As a convenience, you can send in file of CQL commands (one per line)
to be run on startup.  This is handy for initializing the database
with some tables and data in the tables.  Do this via the `-f` option:
```
cqlish -f startup_commands.cql
```

You can also run the commands from the file after starting `cqlish` via the
`SOURCE` command:
```
SOURCE startup_commands.cql
```

#### Clear
`CLEAR` will clear the terminal window.

## Running
`cqlish` is both a Linux executable and a Java jar file.  You can execute
it simply by:
```
./cqlish
```

If you want to run with other Java operations you can execute via `java -jar`:
```
java -jar ./cqlish
```

### Windows
`cqlish` does work on Windows, but must be run using `java -jar`:
```
java -jar cqlish
```

The command-line switches `-reset` and `-j` also are supported:
```
java -jar cqlish -f path\to\commands.cql
```
