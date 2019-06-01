name := MyBuild.NamePrefix + "lib"


import ScalateKeys._


//val ScalatraVersion = "2.4.1"


// https://mvnrepository.com/artifact/net.sf.saxon/Saxon-HE
// libraryDependencies += "net.sf.saxon" % "Saxon-HE" % "9.7.0-15"

//libraryDependencies += "org.scalatra" %% "scalatra-scalate" % ScalatraVersion
libraryDependencies += "org.scalatra.scalate" %% "scalate-core" % "1.8.0"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"