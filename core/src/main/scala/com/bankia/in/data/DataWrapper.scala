package com.modelicious.in.data

import org.apache.spark.sql.DataFrame

import com.modelicious.in.app.Application
import com.modelicious.in.tools.FileUtils

import scala.xml._
import scala.xml.transform.RuleTransformer
import scala.language.postfixOps
import scala.language.reflectiveCalls
import com.modelicious.in.tools.AddChildrenTo

import org.apache.log4j.Logger
import com.modelicious.in.tools.Implicits._


/**
 * Wrapper of a DataFrame that carries the metadata with all the transformations that are applied to it.
 * It is, like DataFrame, inmutable and a set of helper methods have been created to ease the use of the class.
 * 
 * Some are just shortcuts, like [[com.modelicious.in.data.DataWrapper.schema]] that returns wrapped DataFrame schema, or [[com.modelicious.in.data.DataWrapper.cache]].
 * 
 * Others are more complicated, allowing to pass blocks that allow to work with the wrapper objects.
 * 
 * For example, method [[com.modelicious.in.data.DataWrapper.withDF]] allows to pass a block or function that receives a DataFrame 
 * and returns another one after all the work on it is done. This method is overloaded by the ''apply'' method of the class:
 * 
 * {{{		
 * 		dw() { df =>
 * 			df.drop("myColumn") // drop returns a new DataFrame
 * 		}
 * }}}
 * 
 *  
 */
class DataWrapper( df: DataFrame, md: MetaData = <metadata></metadata>  ) {
  // making them vals to avoid side effects that taint the discretize flow.
  // should we need to return a kind of this.type is, we, e.g. subclass this ( prolly not ) Just use abstract self tyopes like in:
  // http://stackoverflow.com/questions/4313139/how-to-use-scalas-this-typing-abstract-types-etc-to-implement-a-self-type
  
  private val metadata = md // Or whatever, this is a test, right now 
  private val _data = df
  
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  def data = _data
  
  def schema = _data.schema
  
  def withDF( op: (DataFrame) => (DataFrame) ) = {
    val new_data = op(_data)
    new DataWrapper( new_data, this.metadata )
  }
  
  def apply( op: (DataFrame) => (DataFrame) ) = withDF(op) 
  
  def addToMetaData(conf: scala.xml.Node ): DataWrapper = {
    val new_md = new RuleTransformer(new AddChildrenTo(metadata,conf)).transform(metadata).head.asInstanceOf[MetaData]
    new DataWrapper( this.data, new_md )
  }
  
  def addToMetaData(conf: scala.xml.NodeSeq ): DataWrapper = addToMetaData( conf.asInstanceOf[scala.xml.Node] )
  def addToMetaData(conf: scala.xml.Elem ): DataWrapper = addToMetaData( conf.asInstanceOf[scala.xml.Node] )
  
  def addToMetaDataCommented(conf: scala.xml.Node ): DataWrapper = addToMetaData( Comment(conf.toString).asInstanceOf[scala.xml.Node] )
  def addToMetaDataCommented(conf: scala.xml.NodeSeq ): DataWrapper = addToMetaDataCommented( conf.asInstanceOf[scala.xml.Node] )
  def addToMetaDataCommented(conf: scala.xml.Elem ): DataWrapper = addToMetaDataCommented( conf.asInstanceOf[scala.xml.Node] )
  
  def unpersist = data.unpersist
  
  // cache is lazy
  def cache: DataWrapper = { new DataWrapper( this.data.cache, this.metadata ) }
  
  def repartition( partitions: Int = 10 ) = {
    new DataWrapper( this.data.repartition( partitions ), this.metadata ) 
  }
  
  /**
   * Allows, if the Application has a configured cache folder in HDFS, to create a checkpoint for the underlying RDD of the data object.
   * This breaks the lineage that sometimes, when a lot of transformations are chained, could lead to a OOM exception.
   * 
   * @see http://stackoverflow.com/questions/33424445/how-to-checkpoint-dataframes
   * 
   */
  @deprecated("On Spark 2.0+ this method is implemented for DataSets", "Spark 2.0")
  def checkPoint: DataWrapper = {
    
    com.modelicious.in.app.Application.sc.getCheckpointDir match {
      case Some(_) => {
        log.file( "Checkpoint for DataFrame called." )
        
        data.rdd.checkpoint
        data.rdd.count
        val new_data = com.modelicious.in.app.Application.sqlContext.createDataFrame(data.rdd, data.schema)
        
        return new DataWrapper( new_data, this.metadata ) 
        
      }
      case None => { 
        log.file( "Checkpoint called but no folder has been set." )
        return this  
      }
    }
  }
  
  def randomSplit(weights: Array[Double], seed: Long): Array[DataWrapper] = {
    this.data.randomSplit( weights, seed ).map( x => new DataWrapper( x , this.metadata ) )
  }
  
  def randomSplit(weights: Array[Double]): Array[DataWrapper] = {
    this.data.randomSplit( weights ).map( x => new DataWrapper( x , this.metadata ) )
  }
  
  def getMetaData() : MetaData = metadata
  
}

/**
 * Object that has several helper methods for the read/write of DataWrappers
 */
object DataWrapper {
  
  def readMetaData( uri: String ) : MetaData = {
    val XMLText = FileUtils.readFileToString( uri )
    XML.loadString(XMLText).asInstanceOf[MetaData]
  }
  
  def writeMetaData( uri: String, md: MetaData ) = {
    val p = new PrettyPrinter(80, 2)
    val toWrite = p.format(md) 
    FileUtils.writeStringToFile(uri, toWrite)
  }
  
}