name := MyBuild.NamePrefix + "core"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0" 

//libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.3"

// https://mvnrepository.com/artifact/ca.szc.configparser/java-configparser
libraryDependencies += "ca.szc.configparser" % "java-configparser" % "0.2"

// http://jesseeichar.github.io/scala-io-doc/0.4.3/#!/overview
libraryDependencies += "com.github.scala-incubator.io" % "scala-io-file_2.10" % "0.4.3"

logBuffered in Test := false
parallelExecution in Test := false
fork in Test := true
testOptions in Test += Tests.Argument("-oFD")
//javaOptions in Test += "-Xmx6G"
//javaOptions in Test += "-Xms1G"
//javaOptions in Test += "-Xss10M"

