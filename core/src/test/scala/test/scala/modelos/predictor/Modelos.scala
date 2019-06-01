package test.scala.modelos.predictor

import collection.mutable.Stack
import org.scalatest._

import scala.language.reflectiveCalls
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Row}

//import org.apache.spark.sql.hive.test.TestHiveContext
//import com.holdenkarau.spark.testing.DataFrameSuiteBase

import com.modelicious.in.app.{Application, TestAppConfig}
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.{Statistics, SchemaTools}
import com.modelicious.in.transform._


class TestModelos extends TestModelosBase {
 
 
 "Model Multilayer Perceptron WITHOUT feature selection " should " provide this acuracy 1.0" in {
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
     
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "MultilayerPerceptron" )
    val model_conf = <conf>
                     <numClasses>2</numClasses>
                     <layers>6</layers>
                     <blockSize>128</blockSize>
                     <maxIter>100</maxIter>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, VolCampanha = 4.0, 
        PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia=2.5)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  } 
  
  
  "Model OneVsAllLogistic WITHOUT feature selection " should " provide this acuracy 1.0" in {
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
     
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "OneVsAllLogistic" )
    val model_conf = <conf>
                     <numClasses>2</numClasses >
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, VolCampanha = 4.0, 
        PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia=2.5)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
  
}

