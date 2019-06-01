package com.modelicious.in.modelos.predictor.classifier

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{BMultilayerPerceptronClassifier => MultilayerPerceptronClassifier,
          BMultilayerPerceptronClassificationModel => MultilayerPerceptronClassificationModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.{Vector, Vectors,DenseVector}

import com.modelicious.in.app.Application
import com.modelicious.in.tools.Implicits._

import scala.language.reflectiveCalls

import scala.reflect.runtime.{ universe => ru }
import scala.util.Try

/**
 * ML Implementation of Multilayer Perceptron Networks
 * 
 * @see  [ML NN](https://spark.apache.org/docs/1.6.0/ml-classification-regression.html#multilayer-perceptron-classifier)
*/
class MultilayerPerceptron extends SaveableMLClassifier{
  
  type T=MultilayerPerceptronClassificationModel
  val tag= ru.typeTag[MultilayerPerceptronClassificationModel]
  
  val name = "MultilayerPerceptron"
  
  def doTrain(input: DataFrame, conf: scala.xml.Elem ) = {

    val numClasses = (conf \\ "numClasses").textAsIntOrElse( 2 )
    val layout = (conf \\ "layers").textOrElse("unknown")
    if (layout == "unknown") throw new RuntimeException("Select the appropiate layout considering the dimension of the data and the number of classes")
    val inputSize= input.first().get(1).asInstanceOf[DenseVector].toArray.size
    val layers= Array(inputSize) ++ layout.split(",").map(_.toInt) ++ Array(numClasses )
    val blockSize = (conf \\ "blockSize" ).textAsIntOrElse( 128 )
    val maxIter = (conf \\ "maxIter" ).textAsIntOrElse( 100 )
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(blockSize)
      .setSeed(Application.getNewSeed())
      .setMaxIter(maxIter)
    modelo = Some( trainer.fit(input) )
  }
  
  def configuration(conf: scala.xml.Node):String = {
    val numClasses = (conf \\ "numClasses").textAsIntOrElse( 2 )
    val layout = (conf \\ "layers").textOrElse("unknown")
    if (layout == "unknown") throw new RuntimeException("Select the appropiate layout considering the dimension of the data and the number of classes")
    val layers= layout.split(",").map(_.toInt)
    val blockSize = (conf \\ "blockSize" ).textAsIntOrElse( 128 )
    val maxIter = (conf \\ "maxIter" ).textAsIntOrElse( 100 )
    return "numClasses: " + numClasses + "layout: "+layout+"; blockSize: "+blockSize+"; maxIter: "+maxIter
  }

  def loadModel( path: String ) {
    val sc = com.modelicious.in.app.Application.sc
    modelo = Try( MultilayerPerceptronClassificationModel.load(sc, path) ).toOption 
  }
    
}