//import sbtassembly.AssemblyPlugin.autoImport._

resolvers += Resolver.sbtPluginRepo("releases")
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"


addSbtPlugin("com.eed3si9n" %% "sbt-assembly" % "0.13.0")

addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2") //sbt dependencyTree, sbt dependencyBrowseGraph
