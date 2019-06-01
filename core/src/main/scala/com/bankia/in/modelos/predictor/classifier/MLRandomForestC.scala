package com.modelicious.in.modelos.predictor.classifier

import com.modelicious.in.modelos.predictor.MLRandomForestBase

import org.apache.spark.sql.Row

/**
 * ML Implementation of Random Forest
 * 
 * @see  [ML RF](https://spark.apache.org/docs/1.6.0/ml-classification-regression.html#random-forest-classifier)
*/
class MLRandomForestC extends MLRandomForestBase with MLTreeClassifier {
  override val name = "MLRandomForestC"
}