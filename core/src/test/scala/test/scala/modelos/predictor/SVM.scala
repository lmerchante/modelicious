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
import com.modelicious.in.tools.{Statistics, ScorerStatistics, SchemaTools, ScorerStats}
import com.modelicious.in.transform._


class TestSVMCL extends TestModelosBase {
 
  
  ////////////////////////////////// MLLIB SVM Classifier
  
  "MLLIB Model SVM L1 WITHOUT feature selection " should " provide this acuracy 0.9" in {
     val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "SVM" )
    val model_conf = <conf>
                     <numIterations>100</numIterations>
                     <regParam>1</regParam>
                     <regType>L1</regType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.66, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 4.0, VolCampanha = 6.0, 
        PrecisionMinima = 0.4, Precision = 0.66, Accuracy = 0.8, TPR = 1.0, P = 4.0, FPR = 0.33, Tiempo = 0.0, PrecisionExclu = 1.0,
        Ganancia=1.66)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
    "MLLIB Model SVM L1 WITH feature selection " should " provide this acuracy 0.4" in {
    val f = fixture

    val training_rate = 0.75 
        
    val splits =  getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "SVM" )
    val model_conf = <conf>
                     <numIterations>100</numIterations>
                     <regParam>1</regParam>
                     <regType>L1</regType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 1.0, FNR = 1.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 10.0, VolCampanha = 0.0, 
        PrecisionMinima = 0.4, Precision = 0.0, Accuracy = 0.6, TPR = 0.0, P = 4.0, FPR = 0.0, Tiempo = 0.0, PrecisionExclu = 0.6, Ganancia=0.0)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }

    "MLLIB Model SVM L2 WITHOUT feature selection " should " provide this acuracy 1.0" in {

    val f = fixture
      
    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
     
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "SVM" )
    val model_conf = <conf>
                     <numIterations>100</numIterations>
                     <regParam>1</regParam>
                     <regType>L2</regType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.83, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 5.0, VolCampanha = 5.0, 
        PrecisionMinima = 0.4, Precision = 0.8, Accuracy = 0.9, TPR = 1.0, P = 4.0, FPR = 0.16, Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia=2.0)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
   "MLLIB Model SVM L2 WITH feature selection " should " provide this acuracy 1.0" in {
    val f = fixture

        val training_rate = 0.75 
        
    val splits =  getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "SVM" )
    val model_conf = <conf>
                     <numIterations>100</numIterations>
                     <regParam>1</regParam>
                     <regType>L2</regType>
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

class TestSVMSC extends TestModelosBase {
   ////////////////////////////////// MLLIB SVM Scorer
  
  "MLLIB SCorer Model SVM L1 WITHOUT feature selection " should " provide this Gain 2.5" in {
     val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "SVM_SC" )
    val model_conf = <conf>
                     <numIterations>100</numIterations>
                     <regParam>1</regParam>
                     <regType>L1</regType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 0.66, decil2 = 0.46, decil3 = 0.44, decil4 = 0.40,
        decil5 = 0.13, decil6 = -0.45, decil7 = -0.46, decil8 = -0.83, decil9 = -1.00)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
    
  }
  
    "MLLIB SCorer Model SVM L1 WITH feature selection " should " provide this Gain 0.0" in {
    val f = fixture

    val training_rate = 0.75 
        
    val splits =  getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "SVM_SC" )
    val model_conf = <conf>
                     <numIterations>100</numIterations>
                     <regParam>1</regParam>
                     <regType>L1</regType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
     val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 0.0, NumUnos = 4.0, decil1 = 0.0, decil2 = 0.0, decil3 = 0.0, 
        decil4 = 0.0, decil5 = 0.0, decil6 = 0.0, decil7 = 0.0, decil8 = 0.0, decil9 = 0.0)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
    
    
    println( ScorerStats.compute( predictionAndLabel ).toStringWithFields )
    
  }

    "MLLIB SCorer Model SVM L2 WITHOUT feature selection " should " provide this gain 2.5" in {

    val f = fixture
      
    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
     
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "SVM_SC" )
    val model_conf = <conf>
                     <numIterations>100</numIterations>
                     <regParam>1</regParam>
                     <regType>L2</regType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 1.1379248991598465, decil2 = 1.0740651248829716, decil3 = 0.39136032333552206, 
        decil4 = 0.1231956835524685, decil5 = -0.08608308534172394, decil6 = -1.0265225161099067, decil7 = -1.0721021157019617, decil8 = -1.235874115514168,
        decil9 = -1.4230091419477728)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
    
  }
  
   "MLLIB SCorer Model SVM L2 WITH feature selection " should " provide this Gain 2.5" in {
    val f = fixture

        val training_rate = 0.75 
        
    val splits =  getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "SVM_SC" )
    val model_conf = <conf>
                     <numIterations>100</numIterations>
                     <regParam>1</regParam>
                     <regType>L2</regType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 1.05, decil2 = 0.48, decil3 = 0.22, decil4 = -0.5, decil5 = -0.94,
        decil6 = -1.21, decil7 = -1.25, decil8 = -1.30, decil9 = -1.34)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
    
  } 
  
}

class TestSVMSave extends TestModelosBase {

  
  "MLLIB Model SVM L1 WITHOUT feature selection " should " give same values after save/load" in {
     val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "SVM" )
    val model_conf = <conf>
                     <numIterations>100</numIterations>
                     <regParam>1</regParam>
                     <regType>L1</regType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.66, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 4.0, VolCampanha = 6.0, 
        PrecisionMinima = 0.4, Precision = 0.66, Accuracy = 0.8, TPR = 1.0, P = 4.0, FPR = 0.33, Tiempo = 0.0, PrecisionExclu = 1.0,
        Ganancia=1.66)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
    
    model.transforms = f.mock_DW.getMetaData()
    model.save("test/saveSVM01/")
    
    val model2 = com.modelicious.in.modelos.predictor.classifier.Classifier.load("test/saveSVM01/")
    
    val predictionAndLabel2 = model2.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel2.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats2 = com.modelicious.in.tools.Stats.compute( predictionAndLabel2, testData.count() )
    println(s"    - Stats: "+stats2.toStringWithFields)
    compareStats(stats2, stats)
    
  }
  
 
   "MLLIB SCorer Model SVM L2 WITH feature selection " should " give same values after save/load" in {
    val f = fixture

        val training_rate = 0.75 
        
    val splits =  getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "SVM_SC" )
    val model_conf = <conf>
                     <numIterations>100</numIterations>
                     <regParam>1</regParam>
                     <regType>L2</regType>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 1.05, decil2 = 0.48, decil3 = 0.22, decil4 = -0.5, decil5 = -0.94,
        decil6 = -1.21, decil7 = -1.25, decil8 = -1.30, decil9 = -1.34)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
    
    
    model.transforms = f.mock_DW.getMetaData()
    model.save("test/saveSVM02/")
    
    val model2 = com.modelicious.in.modelos.predictor.scorers.Scorer.load("test/saveSVM02/")
    
    val predictionAndLabel2 = model2.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel2.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats2 = ScorerStats.compute( predictionAndLabel2 )
    println(s"    - Stats: "+stats2.toStringWithFields)
    compareStats(stats2, stats)
  } 
  
}

