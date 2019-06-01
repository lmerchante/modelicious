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
import com.modelicious.in.tools.{Statistics, ScorerStatistics, ScorerStats, SchemaTools}
import com.modelicious.in.transform._


class NaiveBayesCL extends TestModelosBase {
      
   "Model Naive Bayes WITHOUT feature selection " should " provide this acuracy 0.50" in {
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
     
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "NaiveBayes" )
    val model_conf = <conf>
                     <lambda>10</lambda>
                     <modelType></modelType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.66, FNR = 0.25, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 5.0, VolCampanha = 5.0, 
        PrecisionMinima = 0.4, Precision = 0.6, Accuracy = 0.7, TPR = 0.75, P = 4.0, FPR = 0.33, Tiempo = 0.0, PrecisionExclu = 0.8, Ganancia=1.49)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
   
  "Model Naive Bayes WITH feature selection " should " provide this acuracy 0.60" in {
    val training_rate = 0.75 

    val splits =  getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "NaiveBayes" )
    val model_conf = <conf>
                     <lambda>1</lambda>
                     <modelType></modelType>
                     </conf> 
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.66, FNR = 0.5, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, VolCampanha = 4.0, 
        PrecisionMinima = 0.4, Precision = 0.5, Accuracy = 0.6, TPR = 0.5, P = 4.0, FPR = 0.33, Tiempo = 0.0, PrecisionExclu = 0.66, Ganancia=1.25)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }

}

class NaiveBayesSC extends TestModelosBase {
  
   "Scorer Naive Bayes WITHOUT feature selection " should " provide this Gain  2.50" in {
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
     
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "NaiveBayesSC" )
    val model_conf = <conf>
                     <lambda>10</lambda>
                     <modelType></modelType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 0.9993150307955683, decil2 = 0.9456534894404304, decil3 = 0.7288651167433969, decil4 = 0.6821195998604926, decil5 = 0.41335958678218326,
 decil6 = 0.38668894400203707, decil7 = 0.07278203832739703, decil8 = 0.06283312178644004, decil9 = 0.008060798819628111)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
  }
   
  "Scorer Naive Bayes WITH feature selection " should " provide this Gain 2.50" in {
    val training_rate = 0.75 

    val splits =  getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "NaiveBayesSC" )
    val model_conf = <conf>
                     <lambda>1</lambda>
                     <modelType></modelType>
                     </conf> 
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 0.6009427175879345, decil2 = 0.5726530306543421, decil3 = 0.5582446461450999, decil4 = 0.4565697542552178, decil5 = 0.4189309689763359,
decil6 = 0.36341206328819187, decil7 = 0.3318906858079594, decil8 = 0.17995941712584787, decil9 = 0.15559160565282967)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
  }
    
}


class NaiveBayesSave extends TestModelosBase {
  
   "Model Naive Bayes WITHOUT feature selection " should " give same values after save/load" in {
    val f = fixture

    val training_rate = 0.75
    val mock_DW = f.mock_DW
    val splits =  mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
     
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "NaiveBayes" )
    val model_conf = <conf>
                     <lambda>10</lambda>
                     <modelType></modelType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.66, FNR = 0.25, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 5.0, VolCampanha = 5.0, 
        PrecisionMinima = 0.4, Precision = 0.6, Accuracy = 0.7, TPR = 0.75, P = 4.0, FPR = 0.33, Tiempo = 0.0, PrecisionExclu = 0.8, Ganancia=1.49)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
    
    model.transforms = mock_DW.getMetaData()
    model.save("test/saveNB01/")
    
    val model2 = com.modelicious.in.modelos.predictor.classifier.Classifier.load("test/saveNB01/")
    
    val predictionAndLabel2 = model2.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel2.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats2 = com.modelicious.in.tools.Stats.compute( predictionAndLabel2, testData.count() )
    println(s"    - Stats: "+stats2.toStringWithFields)
    compareStats(stats2, stats)
    
  }
   
  "Scorer Naive Bayes WITH feature selection " should " give same values after save/load" in {
    val training_rate = 0.75 

    val fsDW = getFeatureSelectedDW
    
    val splits =  fsDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "NaiveBayesSC" )
    val model_conf = <conf>
                     <lambda>1</lambda>
                     <modelType></modelType>
                     </conf> 
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 0.6009427175879345, decil2 = 0.5726530306543421, decil3 = 0.5582446461450999, decil4 = 0.4565697542552178, decil5 = 0.4189309689763359,
decil6 = 0.36341206328819187, decil7 = 0.3318906858079594, decil8 = 0.17995941712584787, decil9 = 0.15559160565282967)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
    
    model.transforms = fsDW.getMetaData()
    model.save("test/saveNB02/")
    
    val model2 = com.modelicious.in.modelos.predictor.scorers.Scorer.load("test/saveNB02/")
    
    val predictionAndLabel2 = model2.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel2.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats2 = ScorerStats.compute( predictionAndLabel2)
    println(s"    - Stats: "+stats2.toStringWithFields)
    compareStats(stats2, stats)

  }
    
}



