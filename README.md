# FLAMY
### the database manager for Apache Hive

[![Build Status](https://api.travis-ci.org/flaminem/flamy.svg)](https://travis-ci.org/flaminem/flamy)


Flamy is a tool to help organising, validating and running SQL queries and manage their dependencies.

By analyzing queries, Flamy can find dependencies between tables, draw the dependency graph, and run the queries in the right order.

It is also a great tool to quickly validate your hive queries without having to actually run them.
 
It is currently compatible with Hive and Spark-SQL, and is especially helpful when using Hive on Amazon's EMR. 
 
## Installation

To install flamy, you can either build it from source or download a pre-packaged version.


### Install dependencies :

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

### Build From Source :
 
run the following command in the project's root folder of the project:
```
sbt clean stage
```

you can then run flamy from the terminal with the command:
```
target/universal/stage/bin/flamy
```

### (Optional) Run unit tests :

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

### Flamy shell

You think flamy takes too long to answer? Use the `flamy shell` command to start an interactive shell.
In addition to being much more responsive, the shell provides an autocomplete feature.
The shell is made to have the same behavior as single commands, in particular, quotes are handled 
in the exact same way in the flamy shell than in bash.
Thus, any command used in the flamy shell can be simply copy-pasted into a script to be automated. 
 

### Local install (Debian) :

In your .basrhc, add this and set FLAMY_HOME to the correct value :

```
FLAMY_HOME=<PATH_TO_FLAMY_INSTALL_DIR> 
alias flamy=$FLAMY_HOME/bin/flamy
```


