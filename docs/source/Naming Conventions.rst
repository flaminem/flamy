Naming Conventions
==================

Flamy uses the following terms and conventions:

Schema
""""""
A **schema** represents a set of tables.

It is uniquely identified by its name (eg: ``my_schema``)

Table
"""""
A **table** represents a Hive table.

Although Hive does not, Flamy requires users to always refer to a table with its fully qualified name (eg: ``my_schema.my_table``)

Item
""""
An **item** may represent either a Schema or a Table.
When a command requires ITEMS as arguments, the user can specify any space-separated list of table and/or schema names. 
Giving a schema name is equivalent to giving each table names inside this schema.


Partition
"""""""""
A **partition** represents one partition of a Hive Table.
If you are not familiar with partitioning in Hive, checkout
`this tutorial <http://blog.cloudera.com/blog/2014/08/improving-query-performance-using-partitioning-in-apache-hive/>`_ 
and try to use them, partitions are great! 
Just don't try to have too many for one table... 
As a rule of thumb, tables with more than a few thousands partitions may start causing issues.

A partition is identified by a string of the form: ``schema.table/part1=val1[/part2=val2...]``  
(eg: ``stats.daily_visitors/day=2014-10-12/campaign=shoes``)  
However, the ordering of the columns in the string do not matter for Flamy (even if it does for Hive and HDFS).
(eg: ``stats.daily_visitors/campaign=shoes/day=2014-10-12`` works too)


Special Characters
""""""""""""""""""
The characters ``.``, ``=``, and ``/`` being used as delimiters in the partitions names, they should not be used
as schema name, table name, partition key or value.



