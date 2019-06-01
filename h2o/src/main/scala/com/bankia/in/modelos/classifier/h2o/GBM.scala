package com.modelicious.in.modelos.classifier.h2o

import com.modelicious.in.app.Application 
import org.apache.spark.sql.DataFrame


import scala.language.reflectiveCalls
import scala.xml._

import com.modelicious.in.h2o._
import com.modelicious.in.tools.Implicits._

import hex.tree.gbm.{GBM => H2OGBM}
import hex.tree.gbm.GBMModel
import hex.tree.gbm.GBMModel.GBMParameters

import water.fvec.H2OFrame

/**
 * Gradient Boosting Machine for Regression and Classification:
 * This is a forward learning ensemble method. The guiding heuristic is that good predictive results can be obtained through increasingly refined approximations. 
 * H2O’s GBM sequentially builds regression trees on all the features of the dataset in a fully distributed way. 
 * Each tree is built in parallel.
 * 
 * @see  [H2O GMB](https://h2o-release.s3.amazonaws.com/h2o/rel-vajda/2/docs-website/h2o-docs/data-science/gbm.html). 
*/

class GBM extends H2OClassifier{ 

  type T = GBMModel
  
  def configuration(conf: scala.xml.Node): String = conf.toString()

  def doTrain(input: H2OFrame, conf: scala.xml.Elem ) = {
    
    import h2oContext.implicits._ 
    implicit val sqlContext = Application.sqlContext
    import sqlContext.implicits._
    
    val numTrees = (conf \\ "numTrees" ).textAsIntOrElse( 10 )
    val maxDepth = (conf \\ "maxDepth" ).textAsIntOrElse( 15 )
    val nBins = (conf \\ "nBins" ).textAsIntOrElse( 32 )
    
    log.file( "======================================================= Columnas input " + input.names.mkString("[",",","]") )
  
    val gbmParams = new GBMParameters()
    gbmParams._train = input
    gbmParams._response_column = "label"
    gbmParams._ntrees = numTrees
    gbmParams._max_depth = maxDepth
    gbmParams._nbins = nBins 
    gbmParams._seed = Application.getNewSeed
    val gbmModel = new H2OGBM(gbmParams).trainModel.get
    modelo = Some( gbmModel )
   
    log.file( "======================================================= Modelo creado: " + gbmModel._output  )
    
  }

  
  def loadModel(path: String): Unit =  {} //water.support.ModelSerializationSupport.loadH2OModel( new java.net.URI(path) )
  def doSave(path: String): Unit = {} //water.support.ModelSerializationSupport.exportH2OModel( modelo.get, new java.net.URI(path) )
  
  val name: String = "GBM"
  
  
}