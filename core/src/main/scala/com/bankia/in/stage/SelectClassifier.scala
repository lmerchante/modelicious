package com.modelicious.in.stage

import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools._

import org.apache.spark.rdd.RDD

import com.modelicious.in.modelos.predictor.{Predictor, Ensemble}
import com.modelicious.in.modelos.predictor.classifier.{Classifier, ClassifierEnsemble}

import scala.language.postfixOps



class SelectClassifier extends SelectPredictor[Statistics] {

  def getModel( id: String ) : Predictor = {
    Classifier(id)
  }
  
  def getCV : CrossValidator[Statistics] = {
    new CVStats()
  }
  
  def computeStats(predictionAndLabel:RDD[(Double,Double)], n:Long, tiempo: Double= 0.0) : Statistics = {
    Stats.compute(predictionAndLabel, n, tiempo)
  }
  
  def addModelToEnsembleObject( id: String, model: Predictor ): Unit = {
    ClassifierEnsemble.add(id, model.asInstanceOf[Classifier])
  }
  
  
  
}