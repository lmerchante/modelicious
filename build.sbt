import complete.DefaultParsers._

val ss = inputKey[Unit]("Spark submit task.")

ss := {
  // get the result of parsing
  val args: Seq[String] = spaceDelimited("<arg>").parsed
  val command = s"cmd /C spark-submit target\\scala-2.10\\Application.jar ${args.toArray.mkString(" ")} "
  
  println( "Running " + command )
  command! 
}

target in unidoc in ScalaUnidoc := baseDirectory.value / "api"

unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(spark2)