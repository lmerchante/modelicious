package com.modelicious.in.modelos.predictor.classifier

import com.modelicious.in.modelos.predictor.MLDecisionTreeBase

import org.apache.spark.sql.Row

/**
 * ML Implementation of Decision Trees
 * 
 * @see  [ML DT](https://spark.apache.org/docs/1.6.0/ml-classification-regression.html#decision-tree-classifier)
*/
class MLDecisionTreeC extends MLDecisionTreeBase with MLTreeClassifier {
  override val name = "MLDecisionTreeC"
}