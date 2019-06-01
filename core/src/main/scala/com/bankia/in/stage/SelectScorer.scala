package com.modelicious.in.stage

import com.modelicious.in.Constants
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools._

import org.apache.spark.rdd.RDD

import com.modelicious.in.modelos.predictor.{Predictor, Ensemble}
import com.modelicious.in.modelos.predictor.scorers.{Scorer, ScorerEnsemble}

import scala.language.postfixOps

class SelectScorer extends SelectPredictor[ScorerStatistics] {

  def getModel( id: String ) : Predictor = {
    Scorer(id)
  }
  
  def getCV : CrossValidator[ScorerStatistics] = {
    new CVScorerStats()
  }
  
  def computeStats(predictionAndLabel:RDD[(Double,Double)], n:Long, tiempo: Double= 0.0) : ScorerStatistics = {
    ScorerStats.compute(predictionAndLabel, tiempo)
  }
  
  def addModelToEnsembleObject( id: String, model: Predictor ): Unit = {
    ScorerEnsemble.add(id, model.asInstanceOf[Scorer])
  }
  

}