package com.modelicious.in.modelos.predictor.scorers

import com.modelicious.in.modelos.OModel
import com.modelicious.in.modelos.predictor.{Predictor, ModeloML, SaveableModeloML, Transformable, Ensemble, OEnsemble, VectorIndexedModel}

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd.RDD

import com.modelicious.in.tools.Implicits._

import scala.language.postfixOps

/**
 * Base class for all scorers 
 * For a classifier to be instantiable, it must implement the following methods:
 * 
 * 		- train
 * 		- predict
 * 		- configuration 
 * 
 * @todo With a bit of reflection like the used in transformer, the configurarion method could be generic for all children
 * 
 */

trait Scorer extends Predictor 

object Scorer extends OModel[Scorer] 


trait MLScorer extends ModeloML with Scorer


// Hack for the Perceptron, that does not have save at Spark 1.6.* version. Trying to have it at the lowest level possible, it is at Classifier and not at predictor 
trait SaveableMLScorer extends MLScorer with SaveableModeloML {
   type T <: Saveable with Transformable
   
   override def doSave(path: String): Unit = {
     val sc = com.modelicious.in.app.Application.sc
     modelo.getOrElse(throw new RuntimeException("Train your model before trying to save it!") ).save(sc, path)
   }
}

