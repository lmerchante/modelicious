package com.modelicious.in.modelos.cluster

import com.modelicious.in.modelos.Modelo
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Base class for MLLIB and ML unsupervised predictors 
 * 
 * TO DO
*/


abstract class Cluster extends Modelo {

  def train(input: RDD[Vector], conf: scala.xml.Elem , sqlContext: org.apache.spark.sql.SQLContext) = {
    this.conf = conf
    
    doTrain(input, conf, sqlContext)
  }
  
  protected def doTrain(input: RDD[Vector], conf: scala.xml.Elem , sqlContext: org.apache.spark.sql.SQLContext)
   
}