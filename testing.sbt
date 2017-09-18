



// License: Apache 2.0
libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.4" % "it,test"




parallelExecution in Test := false

javaOptions in Test += "-XX:MaxPermSize=1G -XX:MaxMetaspaceSize=1G"

fork in Test := true




fork in IntegrationTest := true

parallelExecution in IntegrationTest := false

javaOptions in IntegrationTest += "-XX:MaxPermSize=1G -XX:MaxMetaspaceSize=1G"


//testOptions in IntegrationTest += Tests.Setup( () =>
//  Seq(
//    "docker", "run",
//    "--rm",
//    "--name", "flamy-it",
//    "-e", "USER=" + "id -u -n".!!.trim(),
//    "-e", "USER_ID=" + "id -u".!!.trim(),
//    "-v", "pwd".!!.trim() + "/tests/data:/data/hive",
//    "-p", "127.0.0.1:9083:9083",
//    "-p", "127.0.0.1:4050:4040",
//    "-p", "127.0.0.1:10000:10000",
//    "--entrypoint", "/start.sh",
//    "fpin/docker-hive-spark"
//  ).run
//)
//
//testOptions in IntegrationTest += Tests.Setup( () => "sleep 30".!)
//
//testOptions in IntegrationTest += Tests.Cleanup( () =>
//  "docker kill flamy-it".!
//)
//
//
//
