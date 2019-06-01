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


class DecisionTreeMLLIBCL extends TestModelosBase {
 
    "Model DecisionTrees WITHOUT feature selection " should " provide this acuracy 1.0" in {
    
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1) .data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "DecisionTree" )
    val model_conf = <conf>
                     <algo>Classification</algo>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>false</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, VolCampanha = 4.0,
        PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 2.5
        )
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
   "Model DecisionTrees WITH feature selection " should " provide this acuracy 1.0" in {

    val training_rate = 0.75 
        
    val splits =  getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "DecisionTree" )
    val model_conf = <conf>
                     <algo>Classification</algo>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>false</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, 
        VolCampanha = 4.0, PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, 
        Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 2.5)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
  "Model DecisionTrees WITH categorical varaibles WITHOUT feature selection " should " provide this acuracy 0.6" in {
    
    val f = fixtureCAT

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "DecisionTree" )
    val model_conf = <conf>
                     <algo>Classification</algo>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>false</useNodeIdCache>
                     <!--categoricalColumns>(f2;4),(f5;4)</categoricalColumns-->
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.83, FNR = 0.75, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 8.0, VolCampanha = 2.0,
        PrecisionMinima = 0.4, Precision = 0.5, Accuracy = 0.6, TPR = 0.25, P = 4.0, FPR = 0.16, Tiempo = 0.0, PrecisionExclu = 0.62
        , Ganancia = 1.25)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
}

class DecisionTreeMLCL extends TestModelosBase {
 
    "ML Model DecisionTree Classifier WITHOUT feature selection " should " provide this acuracy 1.0" in {
    
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1) .data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "MLDecisionTreeC" )
    val model_conf = <conf>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>false</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, VolCampanha = 4.0,
        PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 2.5
        )
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
   "ML Model DecisionTree Classifier WITH feature selection " should " provide this acuracy 1.0" in {

    val training_rate = 0.75 
        
    val splits =  getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "MLDecisionTreeC" )
    val model_conf = <conf>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>false</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, 
        VolCampanha = 4.0, PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, 
        Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 2.5)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
  "ML Model DecisionTree Classifier WITH categorical varaibles WITHOUT feature selection " should " provide this acuracy 0.6" in {
    
    val f = fixtureCAT

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "MLDecisionTreeC" )
    val model_conf = <conf>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>false</useNodeIdCache>
                     <!--categoricalColumns>(f2;4),(f5;4)</categoricalColumns-->
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.83, FNR = 0.75, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 8.0, VolCampanha = 2.0,
        PrecisionMinima = 0.4, Precision = 0.5, Accuracy = 0.6, TPR = 0.25, P = 4.0, FPR = 0.16, Tiempo = 0.0, PrecisionExclu = 0.62
        , Ganancia = 1.25)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
}


class DecisionTreeMLSC extends TestModelosBase {
 
    "ML Model DecisionTree Scorer WITHOUT feature selection " should " provide this Gain 2.5" in {
    
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1) .data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "MLDecisionTreeS" )
    val model_conf = <conf>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>false</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 1.0, decil2 = 1.0, decil3 = 1.0, decil4 = 0.0, 
        decil5 = 0.0, decil6 = 0.0, decil7 = 0.0, decil8 = 0.0, decil9 = 0.0)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
   "ML Model DecisionTree Scorer WITH feature selection " should " provide this Gain 2.5" in {

    val training_rate = 0.75 
        
    val splits =  getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "MLDecisionTreeS" )
    val model_conf = <conf>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>false</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 1.0, decil2 = 1.0, decil3 = 1.0, decil4 = 0.0, decil5 = 0.0, decil6 = 0.0, decil7 = 0.0, decil8 = 0.0, decil9 = 0.0)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
  "ML Model DecisionTree Scorer WITH categorical varaibles WITHOUT feature selection " should " provide this Gain 0" in {
    
    val f = fixtureCAT

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "MLDecisionTreeS" )
    val model_conf = <conf>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>false</useNodeIdCache>
                     <!--categoricalColumns>(f2;4),(f5;4)</categoricalColumns-->
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.ScorerStats.compute( predictionAndLabel )
    val desired =  ScorerStatistics(Ganancia = 0.0, NumUnos = 4.0, decil1 = 1.0, decil2 = 0.0, decil3 = 0.0, decil4 = 0.0, decil5 = 0.0, decil6 = 0.0,
        decil7 = 0.0, decil8 = 0.0, decil9 = 0.0)
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
  }
  
}



