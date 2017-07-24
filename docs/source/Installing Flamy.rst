Installing Flamy
================


To install flamy, you can either download a pre-packaged version or build it from source.

Dependencies
------------

Flamy requires the program `dot <http://www.graphviz.org/>`_ to be able to print table dependency graphs.  

Debian-based
""""""""""""
::

  apt-get install graphviz libgraphviz-dev

Mac OS X
""""""""

Install brew if not already installed ::

  ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)" < /dev/null 2> /dev/null

Install graphviz ::
  
  brew install graphviz

Installation
------------

Download a pre-packaged version
"""""""""""""""""""""""""""""""
Download and untar the .tgz from `this url <https://oss.sonatype.org/service/local/artifact/maven/content?r=snapshots&g=com.flaminem&a=flamy&p=tgz&v=LATEST>`_::
  
  wget 'https://oss.sonatype.org/service/local/artifact/maven/content?r=snapshots&g=com.flaminem&a=flamy&p=tgz&v=LATEST' | gunzip | tar -x

*You still need to install the program `dot` as explained above to be able to display graphs.*

or Build from source
""""""""""""""""""""
Compilation requires `sbt <http://www.scala-sbt.org/>`_ to compile. ::

  git clone git@github.com:flaminem/flamy.git
  cd flamy
  sbt clean stage

The packaging directory will be found at `target/universal/stage`,  with the executable at `target/universal/stage/bin/flamy`
and the configuration file at `target/universal/stage/conf/flamy.properties` 
but bear in mind that recompiling the project will regenerate the `target/universal/stage/` folder.
You can use the `--config-file` to point the configuration file to an alternate location.

Starting a shell
----------------
Once packaged, you can start a shell with:: 

  target/universal/stage/bin/flamy shell

Once in the shell, the `help` command will list all the available commands and their options, and the `show conf` command
will help you troubleshoot any configuration issue.


(Optional) Running unit tests
-----------------------------
::

  sbt test

(Optional) Granting access for Flamy to the Hive Metastore
----------------------------------------------------------
Flamy can perform some actions such as metadata retrieval from Hive's Metastore.
It can perform many useful actions such as listing all the schemas, tables, or partitions with useful associated information.

For this Flamy can use either the Thrift client (HiveMetastoreClient) provided by Hive or directly connect to the metastore database via JDBC.
While the first method works out of the box, it is often much slower than the second.
On the other hand, the JDBC connection requires some configuration on the Hive Metastore's side, :doc:`as explained here <Setting up direct Metastore access>`.

(Optional) local install
------------------------
Ubuntu
""""""
In your .basrhc, add this and set FLAMY_HOME to the correct value::

  FLAMY_HOME=<PATH_TO_FLAMY_INSTALL_DIR> 
  alias flamy=$FLAMY_HOME/bin/flamy

Next steps
----------
- Try the :doc:`demo <Demo>`
- Configure flamy by editing ``target/universal/stage/conf/flamy.properties`` (See the :doc:`configuration guide <Configuring Flamy>`)
- Check out :doc:`the list of all commands available in Flamy <List of Commands>`.

