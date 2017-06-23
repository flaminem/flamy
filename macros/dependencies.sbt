
name := name + "-macros"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.4" % "test"

libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _)


