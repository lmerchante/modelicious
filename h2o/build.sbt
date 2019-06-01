import complete.DefaultParsers._

name := MyBuild.NamePrefix + "h2o"

// Para evitar que el assembly incluya la versión de scala
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  
jarName in assembly := "H2OApp.jar"
mainClass in assembly := Some("com.modelicious.in.h2o.H2OApp")  

// https://mvnrepository.com/artifact/ai.h2o/sparkling-water-core_2.10
libraryDependencies += "ai.h2o" %% "sparkling-water-core"  % "1.6.9"  % "provided"
libraryDependencies += "ai.h2o" %% "sparkling-water-core"  % "1.6.9"  % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

logBuffered in Test := false
parallelExecution in Test := false
fork in Test := true
testOptions in Test += Tests.Argument("-oFD")