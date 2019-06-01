package com.modelicious.in.modelos.predictor.classifier

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{ LogisticRegression, OneVsRest, OneVsRestModel }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import com.modelicious.in.modelos.predictor.ModeloML
import com.modelicious.in.tools.Implicits._

import scala.language.reflectiveCalls

import scala.reflect.runtime.{ universe => ru }
import scala.util.Try

/**
 * ML Implementation of Multilayer Perceptron Networks
 * 
 * @see  [ML OneVsRest](https://spark.apache.org/docs/1.6.0/ml-classification-regression.html#one-vs-rest-classifier-aka-one-vs-all)
*/

class OneVsAllLogistic extends MLClassifier{

  type T = OneVsRestModel
  val tag = ru.typeTag[OneVsRestModel]

  val name = "OneVsAllLogistic"

  def doTrain(input: DataFrame, conf: scala.xml.Elem ) = {
    val numClasses = (conf \\ "numClasses").textAsIntOrElse(-1)
    if (numClasses == -1) throw new RuntimeException("Select the appropiate number of classes")
    val trainer = new OneVsRest().setClassifier(new LogisticRegression)
    modelo = Some(trainer.fit(input))
  }

  def configuration(conf: scala.xml.Node): String = {
    val numClasses = (conf \\ "numClasses").textAsIntOrElse(-1)
    if (numClasses == -1) throw new RuntimeException("Select the appropiate number of classes")
    return "numClasses: " + numClasses
  }

  def loadModel(path: String) {
    // modelo = Try( OneVsRestModel.load(path) ).toOption 
  }
}