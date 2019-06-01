import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtunidoc.ScalaUnidocPlugin
import sbtunidoc.ScalaUnidocPlugin._
//import sbtunidoc.UnidocKeys._




import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys

import complete.DefaultParsers._


object MyBuild extends Build {

  lazy val sparkVersion = "1.6.3"
  
  lazy val NamePrefix = "Commander-"
  lazy val commonSettings = Seq(
    version := "1.0",
    organization := "com.modelicious.in",
    scalaVersion := "2.10.4",
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    libraryDependencies ++= Seq("org.fusesource.jansi" % "jansi" % "1.4",
      "org.scalatest" %% "scalatest" % "3.0.1" % "test"),
    autoAPIMappings := true,
    scalacOptions in (Compile, doc) := Seq("-groups", "-implicits", "-diagrams", "-diagrams-max-classes", "100", "-diagrams-max-implicits","100"),
    target in Compile in doc := baseDirectory.value / "api",
    // To force scalaVersion, add the following:
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
    test in assembly := {},
    parallelExecution in Test := false,
    fork in Test := true,
    testOptions in Test += Tests.Argument("-oFD"),
    EclipseKeys.skipParents in ThisBuild := false
  )

  //  lazy val customUnidocSettings = Seq (
  //    target in (ScalaUnidoc , unidoc) := baseDirectory.value / "api"
  //    )

  lazy val lib = Project("lib", file("lib"))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    .settings(commonSettings: _*)

  lazy val docgen = Project("docgen", file("docgen"))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    .settings(commonSettings: _*)

//  lazy val cg = Project("java-configparser", file("java-configparser"))
//    .disablePlugins(sbtassembly.AssemblyPlugin)
//    .enablePlugins(PomReaderPlugin)  
//    .settings(commonSettings: _*)

    
  lazy val spark2 = Project("spark2", file("spark2"))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    //.disablePlugins(ScalaUnidocPlugin )
    .settings(commonSettings: _*)
    
  lazy val web = Project("web", file("web"))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    .dependsOn(lib).aggregate(lib)
    .settings(commonSettings: _*)

  lazy val core = Project("core", file("core"))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    .dependsOn(lib).aggregate(lib)
	.dependsOn(docgen).aggregate(docgen)
    .dependsOn(spark2).aggregate(spark2)
    .settings(commonSettings: _*)

  lazy val app = Project("app", file("app"))
    .dependsOn(core).aggregate(core)
    .settings(commonSettings: _*)

  lazy val h2o = Project("h2o", file("h2o"))
    .dependsOn(core).aggregate(core)
    .settings(commonSettings: _*)

//  lazy val bigDL = Project("bigDL", file("bigDL"))
//    .dependsOn(core).aggregate(core)
//    .settings(commonSettings: _*)
//    
  lazy val root = Project("root", file("."))
    .disablePlugins(sbtassembly.AssemblyPlugin)
    .settings(aggregate in doc := false)
    //.settings (customUnidocSettings: _*)
    .settings(commonSettings: _*)
    .settings(parallelExecution in Test := false)
    
    //.dependsOn(core).aggregate(core)
    .dependsOn(app, h2o/*, bigDL*/).aggregate(app, h2o/*, bigDL*/)
    .enablePlugins(ScalaUnidocPlugin)

    
}


