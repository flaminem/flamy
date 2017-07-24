FAQ + Did You Know?
===================

In this section are gathered a few 'Frequently Asked Questions' and 'Did You Know' tricks
that are useful to learn when using flamy.

Don't hesitate to come back to this page often to refresh you memory or learn new things.

Frequently Asked Questions
--------------------------

Be the first to ask a question! 

https://groups.google.com/forum/#!forum/flamy  

*(But before you do, please make sure this section doesn't already answer it)*

Did you know?
-------------

Folder architecture
"""""""""""""""""""

When flamy scans the model folder, it looks recursively for folders ending in ``.db``
This means that you can regroup your schemas in subfolders if you want.
The only constraint are that the folders corresponding to the tables 
must be directly inside the schema (``.db``) folder.
The table folder may then contain ``CREATE.hql``, ``POPULATE.hql``, 
``VIEW.hql`` and ``META.properties`` files. 
You can safely add other type of files in theses directories, 
they will be ignored by flamy, but we plan to extend the set 
of files recognized by flamy in the future.
For instance, this folder structure is allowed::

  model
  ├── schema0.db
  │   └── table0
  │       ├── CREATE.hql
  │       └── comments.txt
  │       └── work_in_progress.hql
  ├── project_A
  │   ├── schemaA1.db
  │   │   └── tableA1a
  │   │       └── CREATE.hql
  └── project_B
      └── schemaB1.db
          └── tableA21
              └── CREATE.hql

The configuration ``flamy.model.dir.paths`` allows you to specify multiple folders, 
if you want to separate your projects even more.


Schema properties
"""""""""""""""""

If you want to create a schema with specific properties (location, comment), you can
add a ``CREATE_SCHEMA.hql`` inside the schema (.db) folder, which will contain the CREATE statement of your schema. 
Flamy will safely ignore the location when dry-running locally.

What about views?
"""""""""""""""""

Flamy supports views. To create a view with flamy, all you have to do is to write a ``VIEW.hql`` 
statement with the CREATE VIEW statement instead of the ``CREATE.hql``.

Views are treated as table when possible, which means that the ``show tables``, ``describe tables`` command will correctly list them,
and the ``push tables`` command will correctly push them, in the right order.

Multiple POPULATEs on the same table?
"""""""""""""""""""""""""""""""""""""

Flamy allows you to write multiple queries separated by semicolons ``;`` in the same ``POPULATE.hql``,
in such case, the queries will always be run together and sequentially. 
But you can also have multiple POPULATE files, by using a suffix of the form ``_suffix``.
In such case, when possible flamy will execute all the POPULATE files of a given table in parallel.

For instance a common pattern in Hive for a table aggregating data from two sources, 
is to partition it by source and to have one Hive query per source. 
In such case you could write a ``POPULATE_sourceA.hql`` and a ``POPULATE_sourceB.hql`` file to keep the two logics separated
and be able to execute both queries in parallel.

Hidden files
""""""""""""

Files and folder prefixed with a dot ``.`` or an underscore ``_`` will be ignore by flamy.
This follows the same convention as HDFS, and is especially useful when you are developing something
that is not fully ready yet, but you still want flamy to validate everything.

Presets files
"""""""""""""

For any environment ``myEnv`` you have configured (including the 'model' environment), 
you can set the configuration parameter ``flamy.env.<ENV>.hive.presets.path`` 
to make it point to a ``.hql`` file that may contains several commands that will be performed
before every query session on this environment.
For instance, if your cluster prevents dynamic partitioning by default, you can add
this line in your presets file to enable it for all your queries.

::

  SET hive.exec.dynamic.partition.mode = nonstrict ;

This file is also required to handle custom UDFs, as explained in the next paragraph.

Custom UDFs
"""""""""""

One of Hive's main advantages is that it is quite easy to create and use custom UDFs.
If you have custom UDFs, when using the ``check long`` or the ``run --dry`` command locally, 
you have to make sure that flamy has access to the custom UDF jar and that the functions
are correctly defined in the model presets.

This is how to proceed:

- Set the ``flamy.udf.classpath`` configuration parameter to point to the jar(s) containing your custom UDFs.
- Create a PRESETS_model.hql file and set ``flamy.env.model.hive.presets.path`` to point to it.
- In the presets file, add one line to create each function you want to use
  ``CREATE TEMPORARY FUNCTION my_function AS "com.example.hive.udf.GenericUDFMyFunction" ;``

What about non-Hive (e.g. Spark) jobs?
""""""""""""""""""""""""""""""""""""""

We all agree that SQL is great at performing some tasks, and very poor at others,
which is why our most complex jobs in our workflow are done with pure-Scala Spark jobs.
To handle these Spark dependencies between two tables, add a file called ``META.properties``
in the destination table folder and indicate the name of the source tables of your spark job like this::

  dependencies = schema.source_table_1, schema.source_table_2

When displaying the dependency graph with ``show graph``, flamy will now add blue arrows in the graph
to represent these external dependencies.

Unfortunately, for now, flamy is not capable of handling Spark job, and we usually used a regular scheduler
to populate all the tables required by the spark job with one ``flamy run`` command, then started 
the spark job, and finally populated all the tables downstream with another ``flamy run`` command.

Better handling for Spark jobs is part of the new features we would like to develop, although we know that
since Spark is much more permissive than the SQL syntax, some features, like the automatic dependency discovery
or the dry-run will be difficult to extend to Spark.

For jobs at the interface between the Hive cluster and other services, 
we used our regular scheduler, and flamy was no help here. 
However some of its feature like the graph and the dry-run 
could be a source of inspiration for designing similar features in a scheduler.



