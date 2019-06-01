package com.modelicious.in.modelos

import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import com.modelicious.in.app.Application
import com.modelicious.in.tools.{FileUtils, SchemaTools}
import scala.xml._

import scala.language.postfixOps
import scala.language.reflectiveCalls

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.Metadata

import org.apache.log4j.Logger

import java.util.UUID

import com.modelicious.in.tools.Implicits._

/**
 * Base class for MLLIB and ML supervised and unsupervised predictors 
 * 
 * @see  [[com.modelicious.in.modelos.predictor]]
 * @see  [[com.modelicious.in.modelos.cluster]]
*/

trait Modelo extends Serializable{

  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  type T 
  var modelo: Option[T] = None
  var conf: scala.xml.Elem = _
  var transforms: scala.xml.Node = _
  var _columns: Option[String] = None
  val name: String 
  
  def save( path: String ) = {

    if( modelo.isEmpty )
      throw new RuntimeException("Train your model before trying to save it!") 
    
    FileUtils.createFolder( path )
    val prettyPrinter = new scala.xml.PrettyPrinter(120, 2)
    val meta = prettyPrinter.format( conf % Attribute(None, "model", Text(name), Null) )
    val transforms_string = prettyPrinter.format( transforms )
    
    FileUtils.writeStringToFile( path + "/metadata.xml", meta )
    FileUtils.writeStringToFile( path + "/transforms.xml", transforms_string )
    
    if( _columns.isDefined ) {
      FileUtils.writeStringToFile( path + "/ml_attr.txt", _columns.get )
    }
    doSave( path + "/modelo" )

  }
 
  protected def doSave( path: String )
  protected[modelos] def loadModel( path: String )
  
  protected def checkColumns( input: DataFrame ) = {
    
    if( _columns.isDefined ) {
      log.file("=======================")
      log.file("Input Dataset Columns are defined, so we will check that it is the same that should be. So the Vector Indexer Model works fine")

      val dfColumns = scala.util.Try(input.schema("features").metadata.getMetadata("ml_attr").toString).toOption
      if( dfColumns.isDefined ) {
        
        val fromDf = SchemaTools.MetadataToColumnList( dfColumns.get )
        val fromModel = SchemaTools.MetadataToColumnList( _columns.get )
        
        log.file(" From DataFrame Metadata:" + dfColumns.get)
        log.file(" From DataFrame Columns ordered: " + fromDf)
        
        log.file(" From Model Metadata:" + _columns.get)
        log.file(" From Model Columns ordered: " + fromModel)
        
        if( fromDf != fromModel ) {
          val msg = 
          "Input DataSet Columns is: \n" +  
          fromDf + 
            "\nBut expected Columns are: \n" +
          fromModel + 
          "\nPrediction won't be right."  
          log.file( msg )
          throw new RuntimeException( msg )
        }
      
      }
      else {
        log.file( "metadata from Dataframe dows not hold columns onfo, skipping check" )
      }
        
      log.file("=======================")
      
    } else {
        log.file( "metadata from Model dows not hold columns info, skipping check" )
      }
  
  }
  
  
  
  
}

abstract class OModel[T <: Modelo] {
 
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  type ModelCreator = (() => T)
  private val models = scala.collection.mutable.Map[String, ModelCreator] ()
  
  
  /**
   * Registers a new Model so it can be loaded/created though this objects methods
   * 
   * @param name The name of the model used as key for the factory
   * @param cc Functor of type [[com.modelicious.in.modelos.ModelCreator]] used to create  a new Model instance
   * 
   * For example, to add SVM to the factory:
   * 
   * {{{Classifier.register("SVM", (() => new SVM()))}}}
   * 
   * @see [[com.modelicious.in.modelos.ModelCreator]]
   * @see [[com.modelicious.in.Init.initialize]]
   */
  def register( name: String, cc: ModelCreator ) = {
    log.file( "Registering " + name )
    models.put( name, cc )
  }

  def apply(name: String): T = {
    return models(name)()
  }
  
  def load( model_url: String ): T = {
    
    val path = load_to_hdfs_if_needed( model_url )
    
    log.file( "Loading model from Path " + path )
    
    
    val s_xml = FileUtils.readFileToString(path + "/metadata.xml")
    val conf = XML.loadString( s_xml )
    
    val s_xforms = FileUtils.readFileToString(path + "/transforms.xml")
    val transforms = scala.xml.Utility.trim( XML.loadString( s_xforms ) )
    val columns = scala.util.Try(FileUtils.readFileToString(path + "/ml_attr.txt")).toOption
    
    log.file( "Metadata read from model: " )
    log.file( "Conf: \n " + s_xml )
    log.file( "Transforms: \n " + s_xforms )
    log.file( "Columns Metadata: \n " + columns.getOrElse(" None loaded, check if it is an error ") )
    
    
    println( s_xml )
    
    val model_name = conf \"@model" text
    val m = apply( model_name )
    m.loadModel( path + "/modelo" )
    m.conf = conf
    m.transforms = transforms
    m._columns = columns
    
    m
  }
  
  
  protected def load_to_hdfs_if_needed( url: String ) : String = url.split(':') match {
    case Array(a, l) if a.startsWith("local") => {
      val model_uuid = UUID.randomUUID().toString.takeRight(12)
      val base_uuid = Application.get.uuid
      val dest_in_remote = Application.get().getPath( base_uuid + "/tempModel/" + new java.io.File( url ).getName + "_" + model_uuid )
      log.file( "Loading local model " + url + " to remote location in HDFS " + dest_in_remote)
      FileUtils.copyFromLocal(l, dest_in_remote)
      dest_in_remote
    }
    case Array(a, r) if a.startsWith("remote") => Application.get().getPath(r)
    case Array(a, r) if !a.startsWith("remote") => Application.get().getPath(r) // Default to remote path if the prefix is wrong
    case Array(r) => Application.get().getPath(r)
  }
  
  
  
}