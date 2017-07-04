import NativePackagerHelper._

lazy val commonSettings = Seq(
  organization := "com.flaminem",
  name := "flamy",
  version := "0.7.0-SNAPSHOT",
  scalaVersion := "2.11.8"
)

lazy val macros = 
  (project in file("macros"))
  .settings(commonSettings ++ Seq(publish := { }))

lazy val root = 
  (project in file("."))
  .dependsOn(macros)
  .aggregate(macros)
  .settings(commonSettings)

scalacOptions in Compile ++= Seq("-unchecked",  "-deprecation",  "-feature")

enablePlugins(JavaAppPackaging)

mappings in Universal ++= directory("conf")

mappings in Universal ++= directory("sbin")

mainClass in Compile := Some("com.flaminem.flamy.Launcher")

parallelExecution in Test := false

javaOptions in Test += "-XX:MaxPermSize=1G -XX:MaxMetaspaceSize=1G"

mappings in (Compile, packageDoc) := Seq()

fork in Test := true

