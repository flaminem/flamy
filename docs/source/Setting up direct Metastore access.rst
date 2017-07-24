Setting up direct Metastore access
==================================

Flamy can perform some actions such as metadata retrieval from Hive's Metastore. 
It can perform many useful actions such as listing all the schemas, tables, or partitions with useful associated information.
For this, Flamy can use either the Thrift client (HiveMetastoreClient) provided by Hive or directly connect to the metastore database via JDBC. 
While the first method works out of the box, it is often much slower than the second.

In order to grant direct access to the Hive Metastore for Flamy, you need to ask your favorite administrator 
to create a flamy user and grant it a **read-only** access to the following tables of the metastore (beware, for names are case-sensitive) :

- PARTITIONS
- TBLS
- DBS
- SDS
- COLUMNS_V2
- TABLE_PARAMS
- PARTITION_PARAMS

Currently, flamy is only compatible with Metastore's backed by PostgreSQL, MySQL or MariaDB.
MySQL and MariaDB require the user to manually download the jdbc client jar, for license incompatibility reasons.

If you are not yourself an administrator, and want to perform the changes yourself on a development cluster, 
you may do so by following theses steps:

1. connect with ssh to the machine hosting the database's metastore 
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
(replace <MY_HOSTS> with the correct name)

::
  
  ssh <METASTORE_HOST>

2. connect to the database
""""""""""""""""""""""""""

If you have a Cloudera cluster with a **postgresql** database, 
you may retrieve the admin password with:: 

  sudo cat /var/lib/cloudera-scm-server-db/data/generated_password.txt

and then connect with::

  psql -h localhost -p 7432 hive cloudera-scm

Otherwise please refer to your SQL back end's documentation.


3. Once connected to the database, create a new user called flamy 
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

(replace <FlamyPassword> with the strong password of your choice)::

  CREATE USER flamy WITH PASSWORD '<FlamyPassword>';

4. And grant read-only access to flamy to the following tables
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
::

  GRANT SELECT ON TABLE "PARTITIONS", "TBLS", "DBS", "SDS", "COLUMNS_V2", "TABLE_PARAMS", "PARTITION_PARAMS" TO flamy ;

5. Change Flamy's configuration to enable direct access to the Metastore
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Edit conf/flamy.properties and add or change the line
::

  flamy.env.<ENVIRONMENT>.hive.meta.fetcher.type = direct

for the specific environment you want to connect to (eg: dev, prod, etc.)

You also need to fill the following properties::

  flamy.env.<ENVIRONMENT>.hive.metastore.jdbc.uri = ...
  flamy.env.<ENVIRONMENT>.hive.metastore.jdbc.user = flamy
  flamy.env.<ENVIRONMENT>.hive.metastore.jdbc.password = ...

For instance, if the metastore is using **postgresql**, the jdbc uri shall be of this form 

::
  jdbc:postgresql://<METASTORE_HOST>:<PORT>/hive

If using Cloudera, <PORT> might be 7432, but we recommend checking on your parameters.

6. Try connecting to the database
"""""""""""""""""""""""""""""""""

by running the following command::

  bin/flamy show schemas --on <ENVIRONMENT>

**7. (Optional) If the metastore is using *postgresql*, you might get this kind of error**

::

  FATAL: no pg_hba.conf entry for host "XXX.XXX.XXX.XXX", user "flamy", database "hive", SSL off
  "hive", SSL off

In such case, you will have to: 

**7.1. connect to the Metastore Host with a sudo user**

::

  ssh <METASTORE_HOST>

**7.2. edit postgresql's pg_hba.conf configuration file**

If using cloudera and the hive metastore uses the same database as Cloudera Manager::

  sudo vi /var/lib/cloudera-scm-server-db/data/pg_hba.conf

otherwise::

  sudo vi /etc/postgresql/<PG_VERSION>/main/pg_hba.conf

**7.3. Add the following line**

::

  # Grant access for user flamy to hive database
  host    hive        flamy        <IP/MASK>          md5

where <IP/MASK> describe the subnetwork that will require to connect to the database with flamy.
Check https://www.iplocation.net/subnet-mask for more informations.

**7.4. Finally, restart the database (schedule a maintenance for this)**

Be careful if the database is using **postgresql**, and especially if the same database is used by cloudera,
since the `stop` (and the `restart`) command will block every new connection to the database and wait for all currently open connections to be closed before stopping. 
This means that you will have to stop the HiveMetastore, HiveServer2, and all Cloudera monitoring
services first (the ActivityMonitor, cloudera-scm-server and all cloudera-scm-agents).

If you don't fear being careless, the `fast_stop` command should shut down the database immediately and drop the currently open connections.



