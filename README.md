# FLAMY
### the database manager for Apache Hive

[![Build Status](https://api.travis-ci.org/flaminem/flamy.svg)](https://travis-ci.org/flaminem/flamy)


Flamy is a tool to help organizing, validating and running SQL queries and manage their dependencies.

By analyzing queries, Flamy can find dependencies between tables, draw the dependency graph, and run the queries in the right order.
It is also a great tool to quickly validate your hive queries without having to actually run them.
 
It is currently compatible with Hive and Spark-SQL, and is especially helpful when using Hive on Amazon's EMR. 

## Documentation

See the [wiki](https://github.com/flaminem/flamy/wiki) for installation, configuration, usage instructions, and a tutorial.


### Features
Flamy helps SQL developers to:

- easily and rapidly check the integrity of their queries, even against an evolving database
- better visualize and understand the workflows they created
- easily deploy and execute them on multiple environments
- efficiently gather metadata from the Metastore
 
[![10_run](https://github.com/flaminem/flamy-demo/raw/master/gifs/10_run.gif)](https://asciinema.org/a/qel0wqJ5t4drKsU9JS1ymajXZ)
*(you can click the image to get a video with better resolution)*

## Installation

To install flamy, you can either download a pre-packaged version or build it from source.

### Dependencies :

Printing dependency graphs requires the program `dot` that can be found in the following packages:

#### Debian-based

```
apt-get install graphviz libgraphviz-dev
```

#### Mac OS X

Install brew if not already installed
```
ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)" < /dev/null 2> /dev/null
```

Install graphviz
```
brew install graphviz
```

### Download a pre-packaged version :

Download and untar the .tgz from [this url](https://oss.sonatype.org/service/local/artifact/maven/content?r=snapshots&g=com.flaminem&a=flamy&p=tgz&v=LATEST):
```
wget 'https://oss.sonatype.org/service/local/artifact/maven/content?r=snapshots&g=com.flaminem&a=flamy&p=tgz&v=LATEST' | gunzip | tar -x
```

*You still need to install the program `dot` as explained above to be able to display graphs.*

## or Build from source

Compilation requires [`sbt`](http://www.scala-sbt.org/) to compile.

```
git clone git@github.com:flaminem/flamy.git
cd flamy
sbt clean stage
```

The packaging directory will be found at `target/universal/stage`,  with the executable at `target/universal/stage/bin/flamy`
and the configuration file at `target/universal/stage/conf/flamy.properties` 
but bear in mind that recompiling the project will regenerate the `target/universal/stage/` folder.
You can use the `--config-file` to point the configuration file to an alternate location.

### Starting a shell

Once packaged, you can start a shell with 

```
target/universal/stage/bin/flamy-ee shell
```

Once in the shell, the `help` command will list all the available commands and their options, and the `show conf` command
will help you troubleshoot any configuration issue.


### (Optional) Running unit tests :

Use the following command. In case you encounter PermGenSpace or Metaspace errors, 
increase the memory allocated to sbt.
```
sbt test
```

### Extra java options :

When running flamy, you can add extra java options to your environment, for example :
```
export FLAMY_EXTRA_JAVA_OPTIONS="-Xmx512m"
```

