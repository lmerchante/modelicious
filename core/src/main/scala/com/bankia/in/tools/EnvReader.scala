package com.modelicious.in.tools

import java.nio.file.Paths
//import java.util.Map

import ca.szc.configparser.Ini

import com.modelicious.in.tools.Implicits._

import org.apache.log4j.Logger
import org.apache.commons.lang3.text.StrSubstitutor

import scala.collection.JavaConverters._

import scala.xml.{Elem, XML}



/**
 * Reads a list of python formated config files and stores its values internally. 
 * It performs a flatten on the hierarchy so a [environment][tmpFile] entry is converted to [environment.tmpfile]
 * 
 * WARN: It does lowercase to vars, so to reference them in the files to substitute them, they must be uses with all lowercase letters
 * 
 *  @TODO: 1. Allow other sections and files than environment (Create with a Seq of files)   
 *      2. subsection indirections: eg. "environment.tmpFolder" would substitute ("environment")("tmpFolder")
 * Only if we consider that another custom conf file would be of use for us.
 * 
 **/ 
class EnvReader( uri: Seq[String] = Seq(), modelID: String = "modelID", sessionID: String = "sessionID" )  {
  
  def this( uri: String, modelID: String, sessionID: String ) = this( Seq( uri ), modelID, sessionID ) 
  
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  protected var sections: Map[String, Map[String, String]] = Map[String, Map[String, String]]() 
  for ( u <- uri ) {
    if( !u.isEmpty() ) {
      val file = Paths.get(u)
      val ini = new Ini().read(file);
      val s = ini.getSections().asScala.mapValues( _.asScala.toMap ); // asScala returns a mutable
      sections = sections ++ s
    }
  }    
  
  protected var flattened_sections = sections.map{ t =>
    t._2.map{ s =>
      (t._1 + "." + s._1, s._2)
    }
  }.flatten.toMap
 
  addMappings( Map( ("modelid"-> modelID), ( "sessionid" -> sessionID ) ) )
  
  log.file( "Environment Object created with values: " )  
  
  private var max_pad = -1
  for( section <- flattened_sections.toList.sortBy(_._1) ) {
    max_pad = math.max( section._1.length, max_pad )
  }
  
  for( section <- flattened_sections.toList.sortBy(_._1) ) {
    log.file( section._1.padTo(max_pad + 1 , ' ') + " -> "+ section._2)
  }

  /**
   * Perform substitution of the config values in the given String
   */
  def apply( s: String ): String = {
      val env_sub = new StrSubstitutor( flattened_sections.asJava )
      env_sub.setEnableSubstitutionInVariables(true)
      env_sub.replace( s )
  }
       
  /**
   * Perform substitution of the config values in the given scala.xml.Elem
   */
  def apply( x: Elem ): Elem = {    
     XML.loadString(apply( x.toString ))
  }
  
  def addMappings( map: Map[String, String] ): this.type = {
    flattened_sections ++= map
    return this
  }
  
  /**
   * Returns current map with flattened sections
   */
  def getCurrentEnvironment() = flattened_sections
}

