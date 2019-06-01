package com.modelicious.in.modelos.classifier.h2o

import com.modelicious.in.modelos.predictor.classifier.Classifier
import com.modelicious.in.modelos.predictor.Transformable

import com.modelicious.in.tools.SchemaTools
import com.modelicious.in.app.Application

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

import com.modelicious.in.h2o._

import water.fvec.H2OFrame

import scala.language.implicitConversions
import scala.language.reflectiveCalls


import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.Logs
import org.apache.log4j.Logger


/**
 * Base class for H2O predictors 
*/

abstract class H2OClassifier extends Classifier {
  
  type T <: hex.tree.SharedTreeModel[_,_,_]
  
  val h2oContext = Application.get.h2oc
  
  import h2oContext._
  import h2oContext.implicits._
  
  def train(input: DataFrame, conf: scala.xml.Elem ) = {
    this.conf = conf
   
    val newDF = h2oContext.asH2OFrame(input)
    
    doTrain(newDF, conf)
  }
  
  def doTrain(input: H2OFrame, conf: scala.xml.Elem )
  
  def predict( testData: DataFrame ) : RDD[(Double, Double)] = {
    
    val threshold = 0.5
    
    // No me convence demasiado, pero se supone que los h2oFrame están indexados
    // Tampoco creo que esto se paralelice bien, hay q reunir los datos para crear el RDD
    val h2oDF = h2oContext.asH2OFrame( testData )
    
    val m = modelo.getOrElse(throw new RuntimeException("Train you model before predict!") )
    
    val h2oPrediction = m.score( h2oDF )

    log.file( " columnas entrada: " + h2oDF.names.mkString("[",",","]") )
    log.file( " columnas predict: " + h2oPrediction.names.mkString("[",",","]") )
    
     for (i <- 0 to 9){ 
       log.file( "P: " + h2oPrediction.vec("predict").at(i) + " L: " + h2oDF.vec("label").at(i) )
       } 
    
    Application.sc.parallelize(for (i <- 0L to (h2oDF.numRows()-1)) 
      yield ( categorize(h2oPrediction.vec("predict").at(i), threshold ), h2oDF.vec("label").at(i) ) )
    
  }
  
  private def categorize( value: Double, t: Double ) : Double = {
    if( value <= t ) {return 0.0} else { return 1.0 }
     
  }
  
}
