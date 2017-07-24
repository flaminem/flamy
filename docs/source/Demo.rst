Tutorial
========

In this tutorial, we will use `a sample of server logs from the NASA (1995) <http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html>`_,
and show how we can use Hive, Spark and Flamy to build a small ETL pipeline.

Part 0. Starting the demo
-------------------------

First, download and install flamy :doc:`as explained here <Setup Guide>`, and make sure ``FLAMY_HOME`` is correctly set. ::

  export FLAMY_HOME=<path/to/flamy/installation/dir>

Once ready, checkout `the flamy_demo git repository <https://github.com/flaminem/flamy-demo>`_::

  git clone git@github.com:flaminem/flamy_demo.git
  cd flamy_demo

start the demo::
  
  ./demo.sh

and type your first flamy command::

  show tables

You should obtain the following result:

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/k2kMQOBx3PI7U0h2OQSKFjjP5.js" id="asciicast-k2kMQOBx3PI7U0h2OQSKFjjP5" async></script>

Once you are ready, we will start running some offline commands (no Hive cluster required).



Part 1. Local commands 
----------------------

Folder architecture
"""""""""""""""""""
First, let us have a look at what this repository contains:
We see a ``conf/`` folder containing the configuration file for flamy, a ``data/`` folder that contains sample data, 
and most importantly a ``model/`` folder that contains your first Hive project using flamy.

If you take a look at the conf, you see that flamy is configured via the ``conf/flamy.properties`` file,
in which the configuration line ``flamy.model.dir.paths = model`` indicates to flamy where the model folder is located.

::

  $ tree conf
  conf
  ├── flamy.properties
  └── log4j2.properties

As you can see, the ``model/`` follows the architecture pattern used by Hive to store data on hdfs: ``model/schema_name.db/table_name/...``

::

  $ tree model
  model
  ├── model_PRESETS.hql
  ├── nasa
  │   ├── facts.db
  │   │   └── http_status
  │   │       └── CREATE.hql
  │   ├── nasa_access.db
  │   │   ├── daily_logs
  │   │   │   ├── CREATE.hql
  │   │   │   └── POPULATE.hql
  │   │   ├── daily_url_error_rates
  │   │   │   ├── CREATE.hql
  │   │   │   └── POPULATE.hql
  │   │   ├── daily_urls
  │   │   │   ├── CREATE.hql
  │   │   │   └── POPULATE.hql
  │   │   └── daily_urls_with_error
  │   │       ├── CREATE.hql
  │   │       └── POPULATE.hql
  │   └── nasa_access_import.db
  │       ├── daily_logs
  │       │   ├── CREATE.hql
  │       │   └── POPULATE.hql
  │       └── raw_data
  │           └── CREATE.hql
  └── VARIABLES.properties

Each folder contains one or two .hql files called ``CREATE.hql`` and ``POPULATE.hql``.
If you open them, you will see that the CREATEs contain the definition of each table (CREATE TABLE ...)
and the POPULATEs contain INSERT statements to populate the tables with data.

As you can see, we have already written all the Hive queries that need, and we will demonstrate how flamy
can help us leverage that code efficiently and easily.

It's now time to try flamy's first feature: ``show graph`` !

show graph
""""""""""

::

  flamy> show graph
  INFO: Files analyzed: 18    Failures: 0
  graphs printed at :
     file:///tmp/flamy-user/result/graph%20.png
     file:///tmp/flamy-user/result/graph%20_light.png

Normally, a new window should automatically open and show you these images
(You should be able to see the second one by pressing the right arrow):

.. image:: https://github.com/flaminem/flamy-demo/raw/master/images/graph.png
   :alt: NASA graphs

If not, you can also try right-clicking or ctrl-clicking on the url (``file:///.../graph%20.png``) displayed by the shell to open the file.

This is one of flamy's main feature: flamy just parsed the whole ``model/`` folder, found and parsed all the ``CREATE.hql`` and ``POPULATE.hql`` files,
and build the dependency graph of the tables. 
Simply put, we have an arrow going from table A to table B if we insert data coming from table A into table B.
This is quite different from ORM design diagrams, where arrows symbolize foreign keys relationship, which do not exist in Hive.

As you can see, flamy always proposes two graphs, one 'light graph' to see the relationship between the tables, and another heavier graph that
also displays the columns and partitions of each table. The big dotted boxes each correspond to a schema.

Of course, for big projects, displaying a whole graph with hundred of tables is not practical, which is why the ``show graph`` command
take schema or table names as argument, to be able to concentrate on single tables.
For instance, let us type::

  flamy> show graph nasa_access.daily_logs
  INFO: Files analyzed: 18    Failures: 0
  graphs printed at :
     file:///tmp/flamy-user/result/graph%20nasa_access.png
     file:///tmp/flamy-user/result/graph%20nasa_access_light.png           

This opens a new window with two new graphs, where only the tables inside the schema nasa_access are displayed, 
and where the dependencies that do not belong to nasa_access are still summarized in blue.

This ``show graph`` feature gives you a whole new way to navigate through your Hive table and better understand the architecture
you or your fellow colleagues have created. 
This is very useful at every stage of the workflow's life: from early design and code review, to troubleshooting and production.

This command has a few options that are very useful to learn to master, you can get the list by typing ``help show graph`` or ``show graph --help``.
The most worth to be mentioned are: 

* ``-o --out`` allows you to see the downstream dependencies of a table, very useful when editing or removing a table to see what will be impacted
* ``-I --in-defs`` to see the complete definitions of the upstream dependencies
* ``-v --skip-views`` to see the tables behind the views

We will now see the next useful feature: ``check``

check
"""""

When designing or updating a Hive workflow, making sure everything works can be complicated.
Sometimes, even renaming a column is prohibited as it would cause too much hassle. 
However, keeping a data pipeline clean without ever renaming anything is like 
keeping a java project without ever refactoring: near to impossible.

The first reflex to acquire once you start using flamy is to always use the check command.
In a way, it is like compiling your SQL code.

Let us try it:: 

  check quick

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/xfBGK1LZ4rJ4Vu8xRm5LkO8N7.js" id="asciicast-xfBGK1LZ4rJ4Vu8xRm5LkO8N7" async></script>

As you can see, we voluntarily left a typo in one of the POPULATE.hql files.
This one is easy to solve, it is simply a comma that should not be there before the FROM.
You can open the file by right-clicking (MacOS) or ctrl-clicking (Linux) of the file where the typo is, and fix it.

Once the typo is fixed, rerun the ``check quick`` command to validate that all the queries are ok.
The ``check quick`` command is able to detect several kind of errors, including

* Syntax errors
* Wrong number of columns
* Unknown column references
* Table name not corresponding to the file path

The validation was made to be as close as possible as Hive's, but there might be some discrepancies,
for instance, flamy will complain if you insert two columns with the same alias into a table while Hive will not. 

Now that the quick check is valid, let us try the long check::

  check long

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/If488WHAlcgkxlbLEXBESXIeO.js" id="asciicast-If488WHAlcgkxlbLEXBESXIeO" async></script>

As you can see, the long check takes longer and found another error.
Basically, what the long check does is performing a quick check first, 
then creating all the tables in an empty build-in environment, and finally 
performing an EXPLAIN statement on every POPULATE.hql with Hive to ensure 
that they are all compiled by Hive.

The error we found this time is a Semantic error: we forgot to add the column file_extension to the GROUP BY statement.
Once you have fixed this error, run ``check long`` again to make sure all your Hive queries are correct.

This feature is extremely useful, and it is quite easy to use with a Continuous Integration
server to make sure that whenever someone commits a change to the code base, nothing is broken.

Once all our queries are valid, there is only one thing left to do: run them!

run
"""

Let us run the following command::

  run --dry --from nasa_access.daily_logs --to nasa_access.daily_url_error_rates

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/n2KQG0zd2ZkMBS7PsX2c2WY0e.js" id="asciicast-n2KQG0zd2ZkMBS7PsX2c2WY0e" async></script>

Since we haven't configured flamy for accessing a remote Hive database, the only thing we can
do for now is performing a local dry-run.
The dry-run mode is activated with the ``--dry`` option, and allows you to check what 
the command you type is going to do without actually running it. 
Any query-compile-time error will be spotted, which means that if the dry-run works, the only
type of error you might encounter when running the command for real will be runtime errors.

The next arguments are a ``--from`` and a ``--to`` which allows you to ask flamy to run every POPULATE.hql
related to the tables that are located in the graph between the ``--from`` tables and the ``--to`` tables.

You can also simply write the name of all the tables you want to run, or the name of a schema if you want
to run the queries for all the tables in that schema.  
For instance, if you run the command ``run --dry nasa_access`` you will see it has exactly the same behavior.

Flamy runs the query by following the dependency ordering of the tables, 
and runs them in parallel whenever possible (even if it too fast to see in this example).
The maximal number of queries run by flamy simultaneously is set 
by ``flamy.exec.parallelism`` which equals ``5`` by default.

Finally, as you might have noticed, next to each successful run is a ``{DAY -> "1995-08-08"}``, this inform you that
in the corresponding POPULATE, the variable ``${DAY}`` has been substituted with the value ``"1995-08-08"``.
This value is set inside the file ``model/VARIABLES.properties``, which is itself given to flamy via the ``flamy.variables.path`` 
configuration parameter.

You can try changing this value either by editing the ``VARIABLES.properties`` file, or simply with the ``--variables`` option, 
as in the following command::

  run --dry --from nasa_access.daily_logs --to nasa_access.daily_url_error_rates

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/nXt7mzQ9X84x5ioy8477rYAcD.js" id="asciicast-nXt7mzQ9X84x5ioy8477rYAcD" async></script>

Beware that this option should be given before the main ``run`` command.
Every variable used in a POPULATE should have a default value in the VARIABLES file, to allow flamy performing checks.

Now that you successfully ran your first dry-run, it is time to reach the interesting part: 
running commands against a remote cluster!

Summary
"""""""
We have seen how flamy helps us visualizing the dependency graph of the Hive queries that we wrote,
and validating all our queries like a compiler would do. 
Thanks to this, refactoring a Hive workflow, by renaming a table or a column for instance, has never been so easy.  
Finally, we saw how flamy is able to execute the query workflow in dry-run mode: this will help us in the next
part of this tutorial, where we will run queries against a real (sandbox) Hive environment.




Part 2. Remote commands 
-----------------------

Installing a sandbox Hive environment
"""""""""""""""""""""""""""""""""""""

**With docker**

If you have docker installed, simply run this command from inside this project's directory (this might require sudo rights)::

  docker run --rm -e USER=`id -u -n` -e USER_ID=`id -u` -it -v `pwd`/data:/data/hive -p 127.0.0.1:9083:9083 -p 127.0.0.1:4040:4040 -p 127.0.0.1:10000:10000 fpin/docker-hive-spark

It will start a docker that will automatically:

- expose a running Metastore on the port 9083
- expose a running Spark ThriftServer (similar to HiveServer2) on port 10000
- expose the Spark GUI to follow job progress on port 4040
- start a beeline terminal

In case the docker won't start correctly, please make sure the specified ports are not already used by a service on your machine.

**Without docker**

If you don't have docker, you can either 
`install it <https://store.docker.com/search?type=edition&offering=community>`_, 
or if you can't (e.g. you don't have sudo rights on your workstation) or don't want to install docker, 
you can try installing and configuring Hive and Spark by following the instruction 
from the Dockerfile in `this repository <https://github.com/flaminem/docker-hive-spark>`_, 
and running the file ``start.sh``.

You might also need to update your ``model/VARIABLES.properties`` file to have ``EXTERNAL_DATA_LOCATION``
point to the absolute location of the ``warehouse/`` folder.


diff/push
"""""""""

We now have a remote environment, which we will be able to query using the ``--on`` option::

  show tables --on local

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/H7uC2siZNiXhtjwC3V3bd3xBE.js" id="asciicast-H7uC2siZNiXhtjwC3V3bd3xBE" async></script>

As you can see, this new environment is empty for now.

We can use the ``push`` command to create all the schemas on the local environment::

  show schemas --on local
  push schemas --on local

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/hRaU1Z9p7RlEvXL76ioMAgMAG.js" id="asciicast-hRaU1Z9p7RlEvXL76ioMAgMAG" async></script>

``push`` can be used to create schemas and tables on the remote environment.
It will only create schemas and tables that do not already exists.
You can also use the ``diff`` commands to see the difference between your model and the remote environment.
Of course, you can specify the name of the schemas or tables that you want to interact with.

Let us push the tables on the local environment::

  diff tables --on local
  push tables --on local nasa_access_import facts.http_status
  diff tables --on local
  push tables --on local
  diff tables --on local

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/wWDCUPxZLesQuGqM2tSR3dkcA.js" id="asciicast-wWDCUPxZLesQuGqM2tSR3dkcA" async></script>

As you can see, the first ``push`` command uses arguments and will only push the table ``facts.http_status`` and all the
tables in ``nasa_access_import``. The second ``push``, without argument, will push all the remaining tables.

describe/repair
"""""""""""""""

Now that the tables are created, we can fetch some information about them::

  describe tables --on local

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/8i2TOOjFV8LElVq1qKViG6lyd.js" id="asciicast-8i2TOOjFV8LElVq1qKViG6lyd" async></script>

As you can see, the two input tables ``facts.http_status`` and ``nasa_access_import.raw_data`` are TEXTFILE,
but the latter doesn't seem to contain any data yet.

If you are familiar with Hive, you know that this is because the partition metadata have to be created first.
We can do this with the ``repair`` command, that will run the command ``MSCK REPAIR TABLE`` on the required table.
We can also use the command ``describe partitions`` to check that the partitions are now created::

  describe partitions --on local nasa_access_import.raw_data
  repair tables --on local nasa_access_import.raw_data 
  describe partitions --on local nasa_access_import.raw_data

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/bXyhqjUsflREnQ4VafvDkTOUq.js" id="asciicast-bXyhqjUsflREnQ4VafvDkTOUq" async></script>

As you can see, once the ``repair tables`` command has run, two new partitions have been added to the table ``nasa_access_import.raw_data``

run (again!)
""""""""""""

Let us now run our first real Hive queries with flamy::

  run --on local nasa_access_import.daily_logs

This query shall take a few minutes, while this run we will take a quick look at our pipeline.

The goal here was to create a simple data pipeline, taking 2 months of gzip raw web server logs, and turning them
into SQL tables where we could monitor the error rates per url and per day for this server. 

Let us look at the graph again:

.. image:: https://github.com/flaminem/flamy-demo/raw/master/images/graph%20.png
   :alt: NASA graph light
   :width: 300

As you can see, the first table ``nasa_access_import.raw_data`` is partitioned by month to ingest the two gzipped raw files,
while the rest of the tables are partitioned by day. For this reason the POPULATE action for ``nasa_access_import.daily_logs``
take a while because we must parse the whole 2 months of data.
The next queries that we will run will apply to one day at a time, so they won't take as long.

Once the previous query is done, we can now run our queries for the rest of the pipeline,
and check that the new partitions have been created::

  run --on local nasa_access
  describe partitions --on local nasa_access
  --variables "DAY='1995-08-09'" run --on local nasa_access
  describe partitions --on local nasa_access

.. raw:: html

  <script type="text/javascript" src="https://asciinema.org/a/qel0wqJ5t4drKsU9JS1ymajXZ.js" id="asciicast-qel0wqJ5t4drKsU9JS1ymajXZ" async></script>

Once we have done that, in a real use case, the only thing to do left would be to schedule a job to
run the command ``flamy --variables "DAY='$(date +%F)'" run nasa_access`` every day (sort of).

The ``--dry`` option is really useful for testing everything quickly when deploying a new workflow.

Here at Flaminem, we use our own home-made python scheduler, for which we developed a few plugins 
to have a good integration with flamy, and we encourage the community to try using it with their own schedulers.

Thanks to Flamy, we managed to develop, deploy and maintain tens of Hive+Spark workflows, 
running in production hundreds of Hive queries every day, with a very small team of 3 developers dedicated to it.

We also developed a command called ``regen``, that allows flamy to determine by itself when some 
partitions are outdated compared to their upstream dependencies. 
You can learn more about it here.

What next ?
-----------

Congratulations, you have completed this tutorial and are now free to try flamy for yourself.
From here, you can :

* Try writing new queries on this example, run and validate them with flamy
* Try to plug flamy to your own Hive cluster (see the :doc:`Configuration page <Configuring Flamy>`)
* Go to the :doc:`FAQ + Did You Know? <FAQ>` section to learn a few more tricks
* Check out :doc:`Flamy's regen <Regen>` section to learn about Flamy's most amazing feature
* Spread the word and star flamy's repo :-)


