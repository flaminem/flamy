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

lazy val flamy = 
  (project in file("."))
  .dependsOn(macros)
  .aggregate(macros)
  .settings(commonSettings, Defaults.itSettings)
  .configs(IntegrationTest)

scalacOptions in Compile ++= Seq("-unchecked",  "-deprecation",  "-feature")

enablePlugins(JavaAppPackaging)

mappings in Universal ++= directory("conf")

mappings in Universal ++= directory("sbin")

mainClass in Compile := Some("com.flaminem.flamy.Launcher")

mappings in (Compile, packageDoc) := Seq()

