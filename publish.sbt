// Source: http://www.cakesolutions.net/teamblogs/publishing-artefacts-to-oss-sonatype-nexus-using-sbt-and-travis-ci

import aether.AetherPlugin._
import aether.AetherKeys._
import aether.MavenCoordinates

organizationName := "Flaminem"
organizationHomepage := Some(url("https://flaminem.com/"))
homepage := Some(url("https://github.com/flaminem/flamy"))
startYear := Some(2014)
description := "a database manager for Apache Hive"
licenses := Seq("Apache 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/flaminem/flamy"),
    "scm:git:git@github.com:flaminem/flamy.git"
  )
)
developers := List(
  Developer(
    id    = "fpin",
    name  = "Furcy Pin",
    email = "pin.furcy@gmail.com",
    url   = url("https://github.com/FurcyPin")
  )
)

// Credentials for Travis
credentials ++= (
    for {
      username <- Option(System.getenv().get("SONATYPE_USERNAME"))
      password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
    } yield {
      Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)
    }
  ).toSeq

enablePlugins(UniversalDeployPlugin)

publishMavenStyle := true

pomIncludeRepository := { _ => false }

publishArtifact in Test := false

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

overridePublishBothSettings

aetherArtifact <<= (organization, name in Universal, version, packageBin in Universal, makePom in Compile, packagedArtifacts in Universal) map {
  (organization, name, version, binary, pom, artifacts) =>
      val nameWithoutVersion = name.replace(s"-$version", "")
      createArtifact(artifacts, pom, MavenCoordinates(organization, nameWithoutVersion, version, None, "tgz"), binary)
}



