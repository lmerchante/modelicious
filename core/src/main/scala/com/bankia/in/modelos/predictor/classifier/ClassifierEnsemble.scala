package com.modelicious.in.modelos.predictor.classifier

import com.modelicious.in.modelos.OModel

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.sql.{DataFrame, Row}

//import org.apache.spark.ml.util.MLWritable

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.language.postfixOps
import scala.language.reflectiveCalls
import scala.util.Try
import scala.xml._

import org.apache.log4j.Logger

import com.modelicious.in.data.DataWrapper
import com.modelicious.in.modelos.predictor._
import com.modelicious.in.tools.FileUtils
import com.modelicious.in.tools.SchemaTools
import com.modelicious.in.tools.Implicits._

import scala.reflect.runtime.{ universe => ru }
 
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DoubleType


object ClassifierEnsemble extends OEnsemble[Classifier] 

// Composite
final class ClassifierEnsemble extends Ensemble[Classifier] with Classifier {
  
 
  def train( input: DataFrame, conf: scala.xml.Elem ) = {
    this.conf = conf
    
    val models = conf \\ "conf" \ "models" \ "model"
    for( m <-  models ) {
      val w = (m \"@weight" text).toDouble
      val name = (m \"@name" text)
      
      val theModel = (m \ "@url").lift(0) match {
        case Some(path) => Classifier.load(path.text)
        case None => ClassifierEnsemble(name)
      }
      
      if( theModel.isInstanceOf[VectorIndexedModel]) {
        val theVimModel = theModel.asInstanceOf[VectorIndexedModel] 
        val v = theVimModel.getModelIndexer()
        theVimModel.setModelIndexer(None)
        setModelIndexer( v )
      }

      addModel(w, theModel)
    }
  }
  
  //////////////////////////////////
  // TODO: We should simplify this. And make it more performant. 
  // If models did a per row prediction, and not only whole dataset, this would be much much simpler.
  // The problem is that while training, we dont have info to do the join, so meanwhile we create a row_number column
  // We are using the same predict method for training and execution ( Duplicate it would make the run more performant, but at the cost of
  // making other parts of the code more complex ie: iferent method for training prediction, 2 methods for testing ...
  // Better to keep all the complexity inside this class....
  
  def predict( testData: DataFrame ): RDD[(Double, Double)] = {
    
    var prediction: Option[ RDD[(Double, Double)] ] = None

    // This is to keep the labels in training 
    // Is either this, or adding a new method by row, for all models ----
    // find a way to distinguish train prediction from real prediction (we then just join by client)
    val testData_with_rowNumber = testData.withColumn("rowNumber", (row_number() over Window.orderBy("label")).cast(DoubleType)  ).cache
    
    val data_to_predict = testData_with_rowNumber.drop("label").withColumnRenamed("rowNumber", "label")
    
    
    val d = data_to_predict.cache()
    
    val tw = total_weight.get
    com.modelicious.in.app.Application.sc.broadcast(tw)
    val models = modelo.get
    log.file("Predicting ensemble with " + models.size + " models ")
    for( m <- models ) {
      val w = m._1
      val mm = m._2
      log.file( "Predicting with ensemble submodel " + mm.name )
      log.file( "Data to predict" )
      for( r <- d.take(10) ) {
        log.file( r.toSeq.mkString("[",",","]") )
      }
      prediction match {
        case None => prediction = Some( mm.predict( d ).map{ case(p, c) => ( c, p*w) } )
        case Some( previous ) => {          
          val pred_for_m = mm.predict( d ).map{ case(p, c) => (c, p*w) }
          Some( previous.join( pred_for_m ).map{ x => ( x._1, x._2._1 + x._2._2 ) } )
        }
      }
    }

    prediction.get.map{ x => ( x._1, x._2/tw ) }
    
    for( r <- prediction.get.take(10) ) {
        log.file( r._1 + "-" + r._2 )
      }

    val result2 = testData_with_rowNumber.select("rowNumber","label").rdd.map{ r => (r.getDouble( 0 ), r.getDouble( 1 )) }.join( prediction.get )
          for( r <- result2.take(10) ) {
        log.file( r._1 + " (" + r._2._1 + "," + r._2._2 + ")" )
      }
    
    
    val result= result2.map{ case(a,b) =>( b._2, b._1 ) }
    
    d.unpersist
    testData_with_rowNumber.unpersist
    
    log.file( "Result from ensemble submodel " )
      
      for( r <- result.take(10) ) {
        log.file( r._1 + " " + r._2 )
      }
    
    result
    
  }
  
}

