Configuring Flamy
=================

Flamy looks for its configuration file at ``$FLAMY_HOME/conf/flamy.properties``.
This location can be overridden when starting ``flamy`` with the ``-config-file`` option.

To get started configuring flamy, we recommend to rename `the file <https://github.com/flaminem/flamy/blob/master/conf/flamy.properties.template>`_ 
``$FLAMY_HOME/conf/flamy.properties.template`` to ``$FLAMY_HOME/conf/flamy.properties`` and start editing it.

Despite the file extension, flamy's configuration uses the `HOCON <https://github.com/typesafehub/config/blob/master/HOCON.md>`_ syntax,
which is a superset of both the JSON syntax and the Java properties syntax (`almost <https://github.com/typesafehub/config/blob/master/HOCON.md#java-properties-mapping>`_). 
It means you can write the properties either like a regular java.properties file, or choose any level of nesting for your parameters.

For troubleshooting, you can check your configuration with the command ``flamy config [--on ENV]``
This will display all the values that are active in your configuration for the specified environment.

.. |br| raw:: html

   <br />

Global properties
"""""""""""""""""
``flamy.model.dir.paths`` List\[String]    
|br| *Space-separated list of folder paths where flamy will look for the SQL files of your model.*

``flamy.variables.path`` Option\[String]    
|br| *Path to the file where the variables are defined.*

``flamy.udf.classpath`` Option\[String]    
|br| *List of jar paths (separated with `:`) where flamy will look for the custom Hive UDFs. Don't forget to also add them as CREATE TEMPORARY FUNCTION in the model's presets file.*

``flamy.exec.parallelism`` Int  (default: 5)  
|br| *Controls the maximum number of jobs that flamy is allowed to run simultaneously.*

Environment properties
""""""""""""""""""""""
These properties can be set for each environment you want to configure. Just replace ``<ENV>`` by the name of the correct environment

``flamy.env.<ENV>.hive.server.uri`` String    
|br| *URI of the Hive Server 2.*

``flamy.env.<ENV>.hive.server.login`` String  (default: "user")  
|br| *Login used to connect to the Hive Server 2.*

``flamy.env.<ENV>.hive.presets.path`` Option\[String]    
|br| *Path to the .hql presets file for this environment. These presets will be executed before every query run against this environment.*

``flamy.env.<ENV>.hive.meta.fetcher.type`` "direct" \| "client" \| "default"  (default: "default")  
|br| *The implementation used to retrieve metadata from Hive ('client' or 'direct').*

``flamy.env.<ENV>.hive.metastore.uri`` String    
|br| *Thrift URI of the Hive Metastore. Required in client mode of the meta.fetcher.*

``flamy.env.<ENV>.hive.metastore.jdbc.uri`` String    
|br| *JDBC URI of the Hive Metastore database. Required in direct mode of the meta.fetcher.*

``flamy.env.<ENV>.hive.metastore.jdbc.user`` String  (default: "flamy")  
|br| *JDBC user to use when connecting to the Hive Metastore database. Required in direct mode of the meta.fetcher.*

``flamy.env.<ENV>.hive.metastore.jdbc.password`` String  (default: "flamyPassword")  
|br| *JDBC password to use when connecting to the Hive Metastore database. Required in direct mode of the meta.fetcher.*

Other properties
""""""""""""""""
These are additional, less used, properties.

``flamy.run.dir.path`` String  (default: "/tmp/flamy-user")  
|br| *Set the directory in which all the temporary outputs will be written. By default this is a temporary directory created in /tmp/flamy-$USER.*

``flamy.run.dir.cleaning.delay`` Int  (default: 24)  
|br| *Set the number of hours for which all the run directories older than this time laps will be automatically removed. Automatic removal occurs during each flamy command startup.*

``flamy.regen.use.legacy`` Boolean  (default: false)  
|br| *Use the old version of the regen.*

``flamy.io.dynamic.output`` Boolean  (default: true)  
|br| *The run and regen commands will use a dynamic output, instead of a static output. Only work with terminals supporting ANSI escape codes.*

``flamy.io.use.hyperlinks`` Boolean  (default: true)  
|br| *Every file path that flamy prints will be formatted as a url. In some shells, this allows CTRL+clicking the link to open the file.*

``flamy.auto.open.command`` String  (default: "xdg-open" on Linux, "open" on Mac OSX)  
|br| *Some commands like 'show graph' generate a file and  automatically open it. Use this option to specify the command to use when opening the file,or set it to an empty string to disable the automatic opening of the files.*

``flamy.auto.open.multi`` Boolean  (default: false)  
|br| *In addition with auto.open.command, this boolean flag indicates if multiple files should be open simultaneously.*

``flamy.verbosity.level`` "DEBUG" \| "INFO" \| "WARN" \| "ERROR" \| "SILENT"  (default: "INFO")  
|br| *Controls the verbosity level of flamy.*

