package com.modelicious.in.modelos.predictor.scorers

import com.modelicious.in.modelos.predictor.MLRandomForestBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.RDD

import com.modelicious.in.tools.Implicits._

/**
 * ML Implementation of Random Forest Regressor
 * 
 * @see  [ML RF Scorer](https://spark.apache.org/docs/1.6.0/ml-classification-regression.html#random-forest-regression)
*/

class MLRandomForestS extends MLRandomForestBase with MLScorer {

  override val name = "MLRandomForestS"
  
  def getPrediction: ( DataFrame ) => RDD[(Double, Double)] =  { data =>
    
    log.file("labels for prediction: " + labels.mkString("[",",","]") )
    
    val cat_one_index = labels.toList.indexOf( "1.0" )
    
    data.map{ x =>  (x.getAs[DenseVector](x.fieldIndex("probability"))(cat_one_index), x.getDouble(x.fieldIndex("label")) ) }
  }
  
}