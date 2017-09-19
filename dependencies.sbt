
// this repo contains some of hive's exotic dependencies like eigenbase#eigenbase-properties and org.pentaho#pentaho-aggdesigner-algorithm
resolvers += "Concurrent Conjars repository" at "http://conjars.org/repo"

val hadoop_version = "2.6.0"

//val hive_jdbc_version = "1.1.0"
val hive_jdbc_version = "1.2.1"

val hive_version = "2.1.1"

val spark_version = "2.2.0"

// License: Apache 2.0
libraryDependencies += "org.apache.hive" % "hive-service" % hive_jdbc_version excludeAll ExclusionRule(organization = "log4j")
libraryDependencies += "org.apache.hive" % "hive-jdbc" % hive_jdbc_version excludeAll ExclusionRule(organization = "log4j")
libraryDependencies += "org.apache.hive" % "hive-hbase-handler" % hive_jdbc_version excludeAll ExclusionRule(organization = "log4j")

// License: Apache 2.0
libraryDependencies += "com.assembla.scala-incubator" %% "graph-core" % "1.9.0" withSources() withJavadoc() excludeAll ExclusionRule(organization = "log4j")
libraryDependencies += "com.assembla.scala-incubator" %% "graph-dot" % "1.9.0" withSources() withJavadoc() excludeAll ExclusionRule(organization = "log4j")

libraryDependencies += "postgresql" % "postgresql" % "9.1-901-1.jdbc4" excludeAll ExclusionRule(organization = "log4j")

// License: MIT
libraryDependencies += "org.rogach" %% "scallop" % "0.9.5"  withSources() withJavadoc() excludeAll ExclusionRule(organization = "log4j")

// License: Apache 2.0
libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.11" excludeAll ExclusionRule(organization = "log4j")

// License: BSD 3
libraryDependencies += "jline" % "jline" % "2.14.2" excludeAll ExclusionRule(organization = "log4j")


// License: Apache 2.0
libraryDependencies += "org.apache.spark" %% "spark-core" % spark_version withSources() withJavadoc() excludeAll ExclusionRule(organization = "log4j") exclude("org.spark-project.hive", "hive-exec")
libraryDependencies += "org.apache.spark" %% "spark-sql"  % spark_version withSources() withJavadoc() excludeAll ExclusionRule(organization = "log4j") exclude("org.spark-project.hive", "hive-exec")
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version excludeAll ExclusionRule(organization = "log4j") exclude("org.spark-project.hive", "hive-exec") exclude("org.spark-project.hive", "hive-metastore")

libraryDependencies += "org.apache.hive" % "hive-cli" % hive_version exclude("org.apache.hive", "hive-jdbc") exclude("org.apache.hive", "hive-service") excludeAll ExclusionRule(organization = "log4j")
libraryDependencies += "org.apache.hive" % "hive-beeline" % hive_version exclude("org.apache.hive", "hive-jdbc") exclude("org.apache.hive", "hive-service") excludeAll ExclusionRule(organization = "log4j")

// License: Apache 2.0
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.8.2"
libraryDependencies += "org.apache.logging.log4j" % "log4j-1.2-api" % "2.8.2"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.8.2"


// License: Apache 2.0
libraryDependencies += "com.typesafe" % "config" % "1.3.1"


/* Testing */



