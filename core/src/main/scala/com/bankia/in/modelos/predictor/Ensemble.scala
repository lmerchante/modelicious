package com.modelicious.in.modelos.predictor

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd.RDD

class OEnsemble[T <: Predictor] {
  
  protected val models: scala.collection.mutable.Map[String, T] = scala.collection.mutable.Map()
  
  def add( id: String, model: T ) = {
    models += ( id -> model)
  }
  
  def apply( id: String ): T = { models(id) }
  
}

trait Ensemble[P <: Predictor] extends Predictor with VectorIndexedModel {
    
  import scala.collection.mutable.ListBuffer
  
  protected var total_weight: Option[Double] = None

  // weight, model  
  type T = ListBuffer[(Double, P)]
  modelo = Some( new ListBuffer[(Double, P)]() )

  def train( input: DataFrame, conf: scala.xml.Elem )
  
  def addModel( w: Double, m: P ): Unit = { 
    // Save the _vim at root level. We dont want to index the data multiple times
    // So we also unset it on the children
    if( m.isInstanceOf[VectorIndexedModel] ){
      val m_as_vim = m.asInstanceOf[VectorIndexedModel] 
      _vim = m_as_vim.getModelIndexer
      m_as_vim.setModelIndexer(None)
    }
    modelo = Some( modelo.get += ((w,m)) )
    total_weight = total_weight match {
      case None => Some(w)
      case Some(x) => Some(x + w) 
    }
  }
  private def addModel( m: (Double, P) ): Unit = addModel( m._1, m._2 )
  
  def predict( testData: DataFrame ): RDD[(Double, Double)] 
  
   // TODO
 /** As seen from class ScorerEnsemble, the missing signatures are as follows.
  *  For convenience, these are usable as stub implementations.
  */
   def doSave(path: String): Unit = {}
   def loadModel(path: String): Unit = {}
   val name: String = "ScorerEnsemble"
   def configuration(conf: scala.xml.Node):String = "" // TODO
  
}
