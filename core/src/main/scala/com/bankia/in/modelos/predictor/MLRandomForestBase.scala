package com.modelicious.in.modelos.predictor


import org.apache.spark.ml.classification.modelicious.RandomForestClassifier

import com.modelicious.in.app.Application
import com.modelicious.in.transform._

case class MLRandomForestConfig() extends TransformerState {
  var numTrees: Int = 10
  var maxDepth: Int = 5
  var maxBins: Int = 32
  var impurity: String = "gini"
  var useNodeIdCache: Boolean = true
  var checkpointInterval: Int = 10
  var featureSubsetStrategy: String = "auto"
}


abstract class MLRandomForestBase extends MLTreesBase[MLRandomForestConfig, RandomForestClassifier] {
  
  override val name = "MLRandomForestBase"
  
  def getConfig( conf: scala.xml.Elem ) : MLRandomForestConfig = {
     TransformerState.fromXML[MLRandomForestConfig](conf)
  }

  def getTrainer( config: MLRandomForestConfig ) : RandomForestClassifier  = {
   new RandomForestClassifier()
      .setNumTrees( config.numTrees )
      .setMaxDepth( config.maxDepth )
      .setMaxBins( config.maxBins )
      .setImpurity( config.impurity )
      .setCacheNodeIds( config.useNodeIdCache )
      .setFeatureSubsetStrategy( config.featureSubsetStrategy )
      .setCheckpointInterval( config.checkpointInterval )
      .setSeed(Application.getNewSeed())
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
  }

}