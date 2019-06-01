package com.modelicious.in.modelos.predictor.classifier

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{ RandomForest => SparkRandomForest }
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, IntegerType, MetadataBuilder}
import org.apache.spark.SparkException
import org.apache.log4j.Logger

import com.modelicious.in.app.Application
import com.modelicious.in.tools.SchemaTools
import com.modelicious.in.tools.Implicits._

import scala.language.reflectiveCalls

import scala.reflect.runtime.{ universe => ru }

/**
 * MLLIB Implementation of Random Forest
 * 
 * @see  [MLLIB RF](https://spark.apache.org/docs/1.6.0/mllib-ensembles.html#random-forests)
*/

class RandomForest extends Strategist {

  type T = RandomForestModel
  val tag = ru.typeTag[RandomForestModel]
  val name = "RandomForest"

  def doTrain( input: RDD[LabeledPoint], vector_schema: StructType, conf: scala.xml.Elem) = {
    val numTrees = (conf \\ "numTrees").textAsIntOrElse(20)
    val featureSubsetStrategy = (conf \\ "featureSubsetStrategy").textOrElse("auto")
    
    var strategy =  makeStrategy(conf)
    
    strategy.categoricalFeaturesInfo=SchemaTools.getCategoricalFeatures( vector_schema("features") )
    log.file(s"  * Build categoricalFeaturesInfo: " + strategy.categoricalFeaturesInfo.toString)
    
    modelo = Some(SparkRandomForest.trainClassifier(input, strategy, numTrees, featureSubsetStrategy, Application.getNewSeed()))
    
  }

  def configuration(conf: scala.xml.Node): String = {
    val numTrees = (conf \\ "numTrees").textAsIntOrElse(20)
    val featureSubsetStrategy = (conf \\ "featureSubsetStrategy").textOrElse("auto")
    val strategy = makeStrategy(conf).strategyToString
    return "numTrees: " + numTrees + "; featureSubsetStrategy: " + featureSubsetStrategy + strategy

  }

}