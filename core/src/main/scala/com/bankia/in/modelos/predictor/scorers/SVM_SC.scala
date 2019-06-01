package com.modelicious.in.modelos.predictor.scorers

import com.modelicious.in.modelos.predictor.SVMBase

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.StatCounter


//import scala.reflect.runtime.{ universe => ru }

/**
 * MLLIB Implementation of Suport Vector Machines Regressor
 * 
 * Proxy class to the classifiers.SVM that, by clearing the Threshold of the 
 * underlying MLLIB SVM, allows it to output a scoring instead of a classification
 * 
 * @see  [MLLIB SVM Scorer](https://spark.apache.org/docs/1.6.0/mllib-linear-methods.html#linear-support-vector-machines-svms)
*/
class SVM_SC extends SVMBase with Scorer {

 
  override val name = "SVM_SC"
  
  override def doTrain(input: RDD[LabeledPoint], vector_schema: StructType, conf: scala.xml.Elem ) = {
    super[SVMBase].doTrain(input, vector_schema, conf)
    modelo.get.clearThreshold
  }

  
// Normalizes the output to be betewwn 0 and 1, but the middle point would not be 0.5, so we prefer the default output ( distance )
//  override def predict( testData: DataFrame ) : RDD[(Double, Double)] = {
//    val predictions = super.predict(testData)
//    
//    val stats = predictions.map{ x => x._1 }.aggregate(StatCounter())( (sc, d) => sc.merge(d) , (sc1, sc2) => sc1.merge(sc2) )
//    val max = stats.max
//    val min = stats.min
//    
//    println("Max: "+ max + " Min: " + min)
//    
//    if( (max - min ) == 0.0 ) {
//      predictions
//    }
//    else
//    {
//      predictions.map{ case ( pred: Double, client: Double) => { val x = ( (pred - min)/(max - min), client ); println(pred + ":" +x); x } }
//    }
//    
//  }
  
}