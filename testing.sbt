



// License: Apache 2.0
libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.4" % "it,test"




parallelExecution in Test := false

javaOptions in Test += "-XX:MaxPermSize=1G -XX:MaxMetaspaceSize=1G"

fork in Test := true




fork in IntegrationTest := true

parallelExecution in IntegrationTest := false

javaOptions in IntegrationTest += "-XX:MaxPermSize=1G -XX:MaxMetaspaceSize=1G"


testOptions in IntegrationTest += Tests.Setup( () => "src/it/bin/start-it-docker".run )

testOptions in IntegrationTest += Tests.Setup( () => "sleep 30".! )

testOptions in IntegrationTest += Tests.Cleanup( () => "docker kill flamy-it".! )



