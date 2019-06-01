package com.modelicious.in.modelos.predictor.classifier

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree => SparkDecisionTree}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, IntegerType, MetadataBuilder}
import org.apache.spark.SparkException
import org.apache.log4j.Logger

import com.modelicious.in.tools.SchemaTools
import com.modelicious.in.tools.Implicits._
import scala.language.reflectiveCalls
import scala.reflect.runtime.{ universe => ru }

/**
 * MLLIB Implementation of Decision Trees
 * 
 * @see  [MLLIB DT](https://spark.apache.org/docs/1.6.0/mllib-decision-tree.html)
*/

class DecisionTree extends Strategist{
  
  type T= DecisionTreeModel
  val tag = ru.typeTag[DecisionTreeModel]
  val name = "DecisionTree"
  
  def doTrain( input: RDD[LabeledPoint], vector_schema: StructType, conf: scala.xml.Elem) = {
    
    var strategy =  makeStrategy(conf)
    strategy.categoricalFeaturesInfo=SchemaTools.getCategoricalFeatures( vector_schema("features") )
    log.file(s"  * Build categoricalFeaturesInfo: " + strategy.categoricalFeaturesInfo.toString)
    
    modelo = Some( SparkDecisionTree.train(input, strategy))
    
  }
  
  def configuration(conf: scala.xml.Node):String = {
    return makeStrategy(conf).strategyToString
  }
  
}