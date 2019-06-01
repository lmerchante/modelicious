package com.modelicious.in.modelos.predictor.classifier

import org.apache.spark.ml.feature.{ BQuantileDiscretizer => QuantileDiscretizer, Bucketizer }
import org.apache.spark.mllib.classification.{ NaiveBayes => SparkNaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import org.apache.spark.sql.{ DataFrame, Row, Column }
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType, IntegerType }

import java.{ util => ju }

import com.modelicious.in.modelos.predictor.{NaiveBayesBase, NBDiscretizer}
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.SchemaTools

import scala.language.reflectiveCalls

import scala.reflect.runtime.{ universe => ru }

// Probably this should be a class and a transform by its own



private class NBClassifier(modelo: NaiveBayesModel, discretizer: NBDiscretizer) extends Serializable {
  
  def predict(testData: RDD[LabeledPoint] ): RDD[(Double, Double)] = {
    testData.map { p =>
      val testvector = p.features
      val label = p.label
      var output_vector = Array[Double]()
      for (i <- 1 to testvector.size) {
        output_vector = output_vector :+ discretizer.binarySearchForBuckets(discretizer.quantizer(s"val_${i+1}").getSplits, testvector(i - 1))
      }
      val predicted = modelo.predict(Vectors.dense(output_vector))
      (predicted, label)
    }
  }
}

/**
 * MLLIB Implementation of Naive Bayes Classifier
 * 
 * @see  [MLLIB NB](https://spark.apache.org/docs/1.6.0/mllib-naive-bayes.html)
*/
class NaiveBayes extends NaiveBayesBase with Classifier {

  override val name = "NaiveBayes"

  // Our classes with type tag generics are not serializable ( but do in 2.11 )
  // a method of such class isnt ( as the whole class is passed)
  // To avoid it, we create a new method that wraps the parallelized method
  override def predict( testData: DataFrame ) : RDD[(Double, Double)] = {
    val sqlContext = com.modelicious.in.app.Application.sqlContext
    import sqlContext.implicits._

    checkColumns( testData )
    
    val my_RDD = SchemaTools.DFWithVectorToLabeledRDD(testData)
    new NBClassifier( modelo.getOrElse(throw new RuntimeException("Train you model before predict!")), discretizer).predict(my_RDD )
  }
  

}
