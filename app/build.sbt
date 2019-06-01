import complete.DefaultParsers._

name := MyBuild.NamePrefix + "app"

// Para evitar que el assembly incluya la versión de scala
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  
jarName in assembly := "Application.jar"
mainClass in assembly := Some("com.modelicious.in.MainApp")  

