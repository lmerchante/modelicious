package com.modelicious.in.html

import org.fusesource.scalate.layout.DefaultLayoutStrategy
//import org.fusesource.scalate.servlet.ServletTemplateEngine
import org.fusesource.scalate.support.TemplateFinder
import org.fusesource.scalate.{ Binding, DefaultRenderContext, TemplateEngine }
import java.io.File

//import org.apache.log4j.Logger

//import com.modelicious.in.tools.Implicits._

// Taken from Scalatra github: https://github.com/scalatra/scalatra/blob/2.5.x/scalate/src/main/scala/org/scalatra/scalate/ScalateSupport.scala
// And refactored to a much more simple code

object ScalateSupport {
  //@transient lazy val log = Logger.getLogger(getClass.getName)
  
  val DefaultLayouts = Seq(
    "/WEB-INF/templates/layouts/default",
    "/WEB-INF/layouts/default",
    "/WEB-INF/scalate/layouts/default"
  )
  
  val defaultTemplatesPath = "WEB-INF/templates/views/"
    
  // Insance the only engine we will use  
  val engine = new TemplateEngine( )
  
  // engine.resourceLoader
  
  engine.workingDirectory = new java.io.File(engine.workingDirectory, "commander-web")
  
  setLayoutStrategy(engine)
  
  engine.bindings ::= Binding("context", "_root_." + classOf[DefaultRenderContext].getName, importMembers = true, isImplicit = true)
  //engine.importStatements ::= "import org.scalatra.servlet.ServletApiImplicits._"
  
  
  private def setLayoutStrategy(engine: TemplateEngine) = {
    val layouts = for {
      base <- ScalateSupport.DefaultLayouts
      extension <- TemplateEngine.templateTypes
    } yield ("%s.%s".format(base, extension))
    engine.layoutStrategy = new DefaultLayoutStrategy(engine, layouts: _*)
  }
  
  def render(path: String,  attributes: Map[String, Any] = Map()) = engine.layout( defaultTemplatesPath + path, attributes)

}
