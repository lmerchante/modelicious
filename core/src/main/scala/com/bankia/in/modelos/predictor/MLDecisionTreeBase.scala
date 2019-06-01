package com.modelicious.in.modelos.predictor


import org.apache.spark.ml.classification.modelicious.DecisionTreeClassifier
import com.modelicious.in.app.Application

import com.modelicious.in.transform._

case class MLDecisionTreeConfig() extends TransformerState {
  var maxDepth: Int = 5
  var maxBins: Int = 32
  var impurity: String = "gini"
  var useNodeIdCache: Boolean = true
  var checkpointInterval: Int = 10
}


abstract class MLDecisionTreeBase extends MLTreesBase[MLDecisionTreeConfig, DecisionTreeClassifier] {
  
  override val name = "MLDecisionTreeBase"
  
  def getConfig( conf: scala.xml.Elem ) : MLDecisionTreeConfig = {
     TransformerState.fromXML[MLDecisionTreeConfig](conf)
  }

  def getTrainer( config: MLDecisionTreeConfig ) : DecisionTreeClassifier  = {
   new DecisionTreeClassifier()
      .setMaxDepth( config.maxDepth )
      .setMaxBins( config.maxBins )
      .setImpurity( config.impurity )
      .setCacheNodeIds( config.useNodeIdCache )
      .setCheckpointInterval( config.checkpointInterval )
      .setSeed(Application.getNewSeed())
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
  }

}