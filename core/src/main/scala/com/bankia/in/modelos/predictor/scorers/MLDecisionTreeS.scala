package com.modelicious.in.modelos.predictor.scorers

import com.modelicious.in.modelos.predictor.MLDecisionTreeBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.RDD

import com.modelicious.in.tools.Implicits._

/**
 * ML Implementation of Decision Trees Regressor
 * 
 * @see  [ML DT Scorer](https://spark.apache.org/docs/1.6.0/ml-classification-regression.html#decision-tree-regression)
*/
class MLDecisionTreeS extends MLDecisionTreeBase with MLScorer {

  override val name = "MLDecisionTreeS"
  
  def getPrediction: ( DataFrame ) => RDD[(Double, Double)] =  { data =>
    
    log.file("labels for prediction: " + labels.mkString("[",",","]") )
    
    val cat_one_index = labels.toList.indexOf( "1.0" )
    
    data.map{ x =>  (x.getAs[DenseVector](x.fieldIndex("probability"))(cat_one_index), x.getDouble(x.fieldIndex("label")) ) }
  }
  
}