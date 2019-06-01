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


class RandomForestCL extends TestModelosBase {
   
    "Model Random Forest WITHOUT feature selection"  should " provide this acuracy 0.8" in {
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "RandomForest" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <algo>Classification</algo>
                     <featureSubsetStrategy>auto</featureSubsetStrategy>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf/*, seed=11*/)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.66, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 4.0, VolCampanha = 6.0,
      PrecisionMinima = 0.4, Precision = 0.66, Accuracy = 0.8, TPR = 1.0, P = 4.0, FPR = 0.33, 
      Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 1.66)
    
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
    
  }

  "Model Random Forest WITH feature selection " should " provide this acuracy 1.0" in {

    val training_rate = 0.75 
        
    val splits = getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "RandomForest" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <algo>Classification</algo>
                     <featureSubsetStrategy>auto</featureSubsetStrategy>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )

    //val desired = Statistics(TNR = 0.66, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 4.0, 
    //    VolCampanha = 6.0, PrecisionMinima = 0.4, Precision = 0.66, Accuracy = 0.8, TPR = 1.0, P = 4.0, FPR = 0.33,
    //    Tiempo = 0.0, PrecisionExclu = 1.0)
        
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, 
        VolCampanha = 4.0, PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, 
        Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 2.5)

    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)

  } 
  "Model Random Forest WITH categorical columns WITHOUT feature selection " should " provide this acuracy 1.0" in {
    val f = fixtureCAT

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "RandomForest" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <algo>Classification</algo>
                     <featureSubsetStrategy>auto</featureSubsetStrategy>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>true</useNodeIdCache>
                     <!--categoricalColumns>(f2;4),(f5;4)</categoricalColumns-->
                     </conf>
    model.train(trainingData, model_conf/*, seed=11*/)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, VolCampanha = 4.0,
      PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, 
      Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 2.5)
    
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
    
  }
    ////////////////////////////////// ML RANDOM FOREST
  
    "Classifier Random Forest from ML WITHOUT feature selection"  should " provide this acuracy 0.8" in {
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "MLRandomForestC" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <featureSubsetStrategy>auto</featureSubsetStrategy>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf/*, seed=11*/)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.66, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 4.0, VolCampanha = 6.0,
      PrecisionMinima = 0.4, Precision = 0.66, Accuracy = 0.8, TPR = 1.0, P = 4.0, FPR = 0.33, 
      Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 1.66)
    
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
    
  }

  "Classifier Random Forest from ML WITH feature selection " should " provide this acuracy 1.0" in {

    val training_rate = 0.75 
        
    val splits = getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "MLRandomForestC" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )

    //val desired = Statistics(TNR = 0.66, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 4.0, 
    //    VolCampanha = 6.0, PrecisionMinima = 0.4, Precision = 0.66, Accuracy = 0.8, TPR = 1.0, P = 4.0, FPR = 0.33,
    //    Tiempo = 0.0, PrecisionExclu = 1.0)
        
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, 
        VolCampanha = 4.0, PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, 
        Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 2.5)

    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)

  } 
  
  // https://issues.apache.org/jira/browse/SPARK-19449
  // Not the same value in this case, but not considered an error by Spark O.O
  "Classifier Random Forest from ML WITH categorical columns WITHOUT feature selection " should " provide this acuracy 1.0" in {
    val f = fixtureCAT

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "MLRandomForestC" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <featureSubsetStrategy>auto</featureSubsetStrategy>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf/*, seed=11*/)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 0.83, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 5.0, Ganancia = 2.0,
        VolCampanha = 5.0, PrecisionMinima = 0.4, Precision = 0.8, Accuracy = 0.9, TPR = 1.0, P = 4.0, FPR = 0.16, 
        Tiempo = 0.0, PrecisionExclu = 1.0)
    
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
    
  }
  
}

class RandomForestSC extends TestModelosBase {
  ////////////////////////////////// ML RANDOM FOREST SCORER
  
    "Scorer Random Forest from ML WITHOUT feature selection"  should " provide this Gain  2.5" in {
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "MLRandomForestS" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <featureSubsetStrategy>auto</featureSubsetStrategy>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf/*, seed=11*/)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 1.0, decil2 = 1.0, decil3 = 0.8333333333333334, decil4 = 0.5, 
decil5 = 0.5, decil6 = 0.3333333333333333, decil7 = 0.0, decil8 = 0.0, decil9 = 0.0)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
    
  }

  "Scorer Random Forest from ML WITH feature selection " should " provide this Gain " in {

    val training_rate = 0.75 
        
    val splits = getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "MLRandomForestS" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 1.0, decil2 = 1.0, decil3 = 1.0, decil4 = 0.0, decil5 = 0.0, decil6 = 0.0, decil7 = 0.0, decil8 = 0.0, decil9 = 0.0)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
  } 
  
  // https://issues.apache.org/jira/browse/SPARK-19449
  // Not the same value in this case, but not considered an error by Spark O.O
  "Scorer Random Forest from ML WITH categorical columns WITHOUT feature selection " should " provide this acuracy 1.0" in {
    val f = fixtureCAT

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "MLRandomForestS" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <featureSubsetStrategy>auto</featureSubsetStrategy>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf/*, seed=11*/)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 0.8333333333333334, decil2 = 0.8333333333333334, decil3 = 0.8333333333333334, decil4 = 0.6666666666666666, decil5 = 0.16666666666666666,
 decil6 = 0.0, decil7 = 0.0, decil8 = 0.0, decil9 = 0.0)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
    
  }
}


class RandomForestSave extends TestModelosBase {
 
  
  "MLLIB Random Forest WITH feature selection " should " give same values after save/load" in {

    val training_rate = 0.75 
        
    val splits = getFeatureSelectedDW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    
    val trainingData= splits(0).data
    val testData = splits(1).data 
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    
    val model = com.modelicious.in.modelos.predictor.classifier.Classifier( "RandomForest" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <algo>Classification</algo>
                     <featureSubsetStrategy>auto</featureSubsetStrategy>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <numClasses>2</numClasses>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )

    //val desired = Statistics(TNR = 0.66, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 4.0, 
    //    VolCampanha = 6.0, PrecisionMinima = 0.4, Precision = 0.66, Accuracy = 0.8, TPR = 1.0, P = 4.0, FPR = 0.33,
    //    Tiempo = 0.0, PrecisionExclu = 1.0)
        
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, 
        VolCampanha = 4.0, PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, 
        Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 2.5)

    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)

    model.transforms = getFeatureSelectedDW.getMetaData()
    model.save("test/saveRFMLLIB/")
    
    val model2 = com.modelicious.in.modelos.predictor.classifier.Classifier.load("test/saveRFMLLIB/")
    
    val predictionAndLabel2 = model2.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel2.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats2 = com.modelicious.in.tools.Stats.compute( predictionAndLabel2, testData.count() )
    println(s"    - Stats: "+stats2.toStringWithFields)
    compareStats(stats2, stats)
    
  } 
  
    "Scorer Random Forest from ML WITH categorical columns WITHOUT feature selection " should " give same values after save/load" in {
    val f = fixtureCAT

    val training_rate = 0.75 
    val splits =  f.mock_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "MLRandomForestS" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <featureSubsetStrategy>auto</featureSubsetStrategy>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf/*, seed=11*/)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    val desired = ScorerStatistics(Ganancia = 2.5, NumUnos = 4.0, decil1 = 0.8333333333333334, decil2 = 0.8333333333333334, decil3 = 0.8333333333333334, decil4 = 0.6666666666666666, decil5 = 0.16666666666666666,
 decil6 = 0.0, decil7 = 0.0, decil8 = 0.0, decil9 = 0.0)
    println( stats.toStringWithFields )
    compareStats(stats, desired)
   
        model.transforms = f.mock_DW.getMetaData()
    model.save("test/saveRFML01/")
    
    val model2 = com.modelicious.in.modelos.predictor.scorers.Scorer.load("test/saveRFML01/")
    
    val predictionAndLabel2 = model2.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel2.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats2 = com.modelicious.in.tools.Stats.compute( predictionAndLabel2, testData.count() )
    println(s"    - Stats: "+stats2.toStringWithFields)
    compareStats(stats2, stats)
    
  }
    
  "Scorer Random Forest after some transforms " should " give same values after save/load" in {
    val f = fixtureCAT

    var mock_DW = f.raw_mock_DW
    
    // Discretize
    val dtz_conf= <discretize><limit>6</limit><nCategories>3</nCategories><exclude></exclude></discretize>
    val dtz = new DiscretizeTransform 
    mock_DW = dtz.configure(dtz_conf).fit( mock_DW ).transform( mock_DW )
    
    // Standardize
    val std_conf= <standarize><withMean>true</withMean><withSTD>true</withSTD></standarize>
    val std = new StandarTransform 
    mock_DW = std.configure(std_conf).fit( mock_DW ).transform(mock_DW)
    
    var final_DW = new DataWrapper(SchemaTools.convertToVectorOfFeatures( mock_DW.data ), mock_DW.getMetaData())
    
    final_DW = new DataWrapper ( SchemaTools.createCategoricMetadata(final_DW.data, 3)._1,  final_DW.getMetaData() )
    
    val training_rate = 0.75 
    val splits = final_DW.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0).data
    val testData =  splits(1).data  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = com.modelicious.in.modelos.predictor.scorers.Scorer( "MLRandomForestS" )
    val model_conf = <conf>
                     <numTrees>6</numTrees>
                     <featureSubsetStrategy>auto</featureSubsetStrategy>
                     <impurity>gini</impurity>
                     <maxDepth>4</maxDepth>
                     <maxBins>32</maxBins>
                     <useNodeIdCache>true</useNodeIdCache>
                     </conf>
    model.train(trainingData, model_conf/*, seed=11*/)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    
    val stats = ScorerStats.compute( predictionAndLabel )
    println( stats.toStringWithFields )

   
    model.transforms = final_DW.getMetaData()
    model.save("test/saveRFML01/")
    
    val model2 = com.modelicious.in.modelos.predictor.scorers.Scorer.load("test/saveRFML01/")
    
    val predictionAndLabel2 = model2.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel2.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats2 = ScorerStats.compute( predictionAndLabel2 )
    println(s"    - Stats: "+stats2.toStringWithFields)
    compareStats(stats2, stats)
    
  }
  
}



