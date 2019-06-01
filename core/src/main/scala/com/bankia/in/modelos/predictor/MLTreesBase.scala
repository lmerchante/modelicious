package com.modelicious.in.modelos.predictor

import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.classification.modelicious.{RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.feature.{VectorIndexerModel, BVectorIndexerModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.{Vector, Vectors,DenseVector}
import com.modelicious.in.app.Application
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.FileUtils
import com.modelicious.in.tools.SchemaTools
import scala.language.reflectiveCalls
import scala.reflect.runtime.{ universe => ru }
import scala.util.Try
import org.apache.log4j.Logger
import com.modelicious.in.transform._

abstract class MLTreesBase[S <: TransformerState, P <: PipelineStage] extends ModeloML with VectorIndexedModel{
  
  
  type T=PipelineModel//RandomForestClassificationModel
  val tag= ru.typeTag[PipelineModel]
  
  val name = "MLTreeBase"
  
  var labels: Array[String] = _

  def doTrain(input: DataFrame, conf: scala.xml.Elem ) = {

    
    log.file( "Selected categorical features: " + SchemaTools.getCategoricalFeatures( input.schema("features") ) )
    
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(input)
    log.file("SCHEMA ========================= " + input.schema.toString() )

    val labelIndexedData = labelIndexer.transform(input)

    labels = labelIndexer.labels
    
    log.file( " Fit " + name )
    
    val config = getConfig(conf)
    val trainer = getTrainer( config ) 
      
    log.file( " LabelConverter" )
      
    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    
    log.file( " Create pipeline" )
      
    // Chain indexers and tree in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array( trainer, labelConverter))
      
    log.file( " Run pipeline" )
      
    modelo = Some( pipeline.fit(labelIndexedData) )
    
  }
  
  def configuration(conf: scala.xml.Node):String = {
    conf.toString
  }

  override def predict( testData: DataFrame ) : RDD[(Double, Double)] = {
    
    var d = testData

    if( _vim.isDefined ) {
      log.file("=======================")
      log.file("Applying Vector Indexer Model")
      log.file("=======================")
      d = _vim.get.transform( testData ).drop("features").withColumnRenamed("indexedFeatures", "features")
    }
    
    log.file( "Categoric metadata from DataFrame: " )
    log.file( d.schema("features").metadata.json )
    
    super[ModeloML].predict(d)
  }
  
  override def doSave( path: String ) = {
    modelo.get.write.save( path )
    // Realmente se guardan en el label converter, y se podrían sacar de ahí
    FileUtils.writeStringToFile( path + "/labels.txt", labels.mkString(",") )  
    if( _vim.isDefined ) {
      _vim.get.write.save(path + "/vim")
    }
    
    //log.file(modelo.get.stages(0).asInstanceOf[org.apache.spark.ml.classification.modelicious.RandomForestClassificationModel].toDebugString)
  }
  
  def loadModel( path: String ) {
    val sc = com.modelicious.in.app.Application.sc
    log.file( "Loading model from " + path )
    try {
      modelo = Some( PipelineModel.load(path) )
      // Realmente se guardan en el label converter, y se podrían sacar de ahí. Está en el pipeline
      labels = FileUtils.readFileToString(path + "/labels.txt").split(',')
      //_columns = Try(FileUtils.readFileToString(path + "/columns.txt").split(',')).toOption
      
      _vim = VectorIndexedModel.load( path )
      
      log.file( "Vector Indexer Model is defined:  " + _vim.isDefined )
      
    } catch {
     case e: Exception => {
          val msg = e.getStackTraceString + "\n" + e.getMessage()
          log.file("Error:" + msg)
          modelo = None
     }
    }
    
    //log.file(modelo.get.stages(0).asInstanceOf[org.apache.spark.ml.classification.modelicious.RandomForestClassificationModel].toDebugString)
    
    // modelo = Try( PipelineModel.load(path) ).toOption 
  }

  def getConfig( conf: scala.xml.Elem ) : S

  def getTrainer( config: S ) :  P
}