List of Flamy commands
======================

**Most flamy commands have the following form:**
::

  flamy [GLOBAL_OPTIONS] COMMAND [SUB_COMMAND] [COMMAND_OPTIONS] [ITEMS]

If you are already inside a ``flamy shell``, beginning the command with ``flamy`` is not necessary.

All the commands available in the shell can also be run as standalone commands.
For instance, ``flamy help`` and ``flamy show conf`` both work. 
This enables the user to easily write and execute scripts that calls sequence of flamy commands.  
The flamy shell handle quotes the same way as bash does, so there should be no worry when copy-pasting commands from the shell to standalone commands.



**The arguments** ``ITEMS`` **denotes a list of tables or schemas. 
Tables should always be referred to using their fully-qualified names (eg** ``my_database.my_table`` **).** 
**Giving a schema name is equivalent to giving the list of all tables in that schema.**

.. |br| raw:: html

   <br />

Global options
""""""""""""""

When specified before the ``shell`` command, the options will apply to all commands subsequently run inside the shell.
The following options are available:

``--help`` 
|br| *Display the help.*

``--version`` 
|br| *Show version information about the software.*

``--config-file PATH`` 
|br| *Make flamy use another configuration file than the default.*

``--conf KEY1=VALUE1 [KEY2=VALUE2 ...]`` 
|br| *Specify configuration parameters from the command line.*

``--variables NAME1=VALUE1 [NAME2=VALUE2 ...]``  
|br| *Declare variables in the command line. 
These declaration will override the ones made in the variable declaration file. 
Be careful when using this option, as bash automatically removes quotes. 
For instance if you want to declare a variable* ``DAY`` 
*whose value is* ``"2015-01-03"`` *(quotes included), 
you should write* ``--variables DAY='"2015-01-03"'``.

shell
"""""

``flamy shell``  
|br| *Starts an interactive shell with autocomplete and faster response time. 
Once in the shell, commands are run in the same way, without the first* ``flamy`` *keyword.
The shell handles quotes the same way bash does, which means commands run in flamy's shell can be copy-pasted, 
prefixed by the path of the flamy executable and directly run in a bash script.*  


help
""""
``flamy -h``

``flamy help [COMMAND]``

show
""""

``flamy show conf [--on ENV]``  
|br| *List all configuration properties applicable to the specified environment.*  

``flamy show schemas [--on ENV]``  
|br| *List all schemas in the specified environment.*  

``flamy show tables [--on ENV] [SCHEMA1 SCHEMA2 ...]``  
|br| *List all tables inside one or several schemas.*  

``flamy show partitions --on ENV TABLE``  
|br| *List all partitions in a table.*  

``flamy show graph [--from FROM_ITEMS --to TO_ITEMS | ITEMS]``  
|br| *Print the dependency graph of the specified items.*  

``flamy show select``  
|br| *Print a SELECT statement for the given table.*


describe
""""""""

*Similar to show commands, but display more information, and take more time to run.
We recommend to enable direct metastore access* :doc:`as explained here <Setting up direct Metastore access>`.

``flamy describe schemas --on ENV``  
|br| *List all schemas with their properties (size, last modification time).*  

``flamy describe tables --on ENV [ITEMS]``  
|br| *List all tables inside one or several schemas with their properties (size, last modification time).*  

``flamy describe partitions [--bytes] --on ENV TABLE``  
|br| *List all partitions in a table with their properties (size, last modification time).*  

diff 
""""
*Helpful to compare the differences between environments.*

``flamy diff schemas --on ENV``  
|br| *Show the schemas differences between the specified environment and the modeling environment.*  

``flamy diff tables --on ENV [ITEMS]``  
|br| *Show the table differences between the specified environment and the modeling environment.*  

``flamy diff columns --on ENV [ITEMS]``  
|br| *Show the column differences between the specified environment and the modeling environment.*  


push commands
"""""""""""""
*Helpful to propagate changes on from your model to another environment.*

``flamy push schemas --on ENV [--dry] [SCHEMA1 SCHEMA2 ...]``  
|br| *Create on the specified environment the schemas that are present in the model and missing in the environment.*  

``flamy push tables --on ENV [--dry] [ITEMS]``  
|br| *Create on the specified environment the tables that are present in the model and missing in the environment.*  


check
"""""

``flamy check quick ITEMS``  
|br| *Perform a quick check on the specified items.*  

``flamy check long ITEMS``  
|br| *Perform a long check on the specified items. This take more time than the quick check, but is more thorough.*  

``flamy check partitions ITEMS --on ENV [--from FROM_ITEMS --to TO_ITEMS | ITEMS]``  
|br| *Check the partitions dependencies on the specified items.*  
*This command will be much faster if run from the cluster rather than from remote.*  


run
"""

``flamy run [--dry] [--on ENV] [--from FROM_ITEMS --to TO_ITEMS | ITEMS]``  
|br| *Execute the POPULATE workflow on the specified environment for the specified items.*  
*If --dry flag is used, the queries will be checked by Hive but not run.*  
*If no environment is specified, run in standalone mode on empty tables 
(this requires to have *``SET hadoop.bin.path=...``* in your local presets.*  


Other commands
""""""""""""""

``flamy wait-for-partitions --on ENV [OPTIONS] PARTITIONS``  
|br| *Wait for the specified partitions to be created if they don't already exist.
Options are:*
|br| ``--after TIME`` *will wait for the partitions to be created or refreshed after the specified TIMESTAMP*
|br| ``--timeout DURATION`` *will make flamy return a failure after DURATION seconds*
|br| ``--retry-interval INTERVAL`` *will make flamy wait for INTERVAL seconds between every check*

``flamy gather-info --on ENV [ITEMS]``  
|br| *Gather all partitioning information on specified items (everything if no argument is given) 
and output this as csv on stdout.*  


