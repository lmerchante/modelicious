package com.modelicious.in.tools

import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import com.modelicious.in.tools.Implicits._

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import com.modelicious.in.modelos.predictor.Predictor
import com.modelicious.in.app.Application
import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls



case class CurvePoint( model: Node, porcentage: Double, ECM_train: Double,ECM_test:Double)
case class CurveGraph( porcentages: Array[Double], ECM_train: Array[Double], ECM_test: Array[Double], ECM_train_std: Array[Double], ECM_test_std:Array[Double] )

object LearningCurve { 
  
  def compute(modelConf: Node, model: Predictor, trainingData: DataFrame,testData: DataFrame,overfitting_splits: Array[Double]) : Array[CurvePoint] = {
    implicit val sqlContext = Application.sqlContext
    @transient lazy val log = Logger.getLogger(getClass.getName)
    val id = modelConf \"@id" text
    val name = modelConf \"@name" text  
    val model_conf = (modelConf \\ "conf")(0).asInstanceOf[Elem]
    for (porcentaje <- overfitting_splits.filter(_!=1)) yield {
        val sampleTrainingData=trainingData.sample(false,porcentaje,Application.getNewSeed())
        log.file( "OVERFITTING Train porcentaje: "+porcentaje*100+"%. Count: "+sampleTrainingData.count())
        model.train(sampleTrainingData, model_conf)
        val ECM_train = 1.0 * model.predict(sampleTrainingData).map(x => (x._1 - x._2)*(x._1 - x._2)).sum() / sampleTrainingData.count()
        val ECM_test = 1.0 * model.predict(testData).map(x => (x._1 - x._2)*(x._1 - x._2)).sum() / testData.count()
        new CurvePoint(modelConf,porcentaje,ECM_train,ECM_test)
    }
  }
  
}


class OverfittingTest {
  // store and average the stats computed for every KFold
  
  var array_stats = Array[CurvePoint]()
  
  def append( stats: CurvePoint) {
    array_stats=array_stats :+ stats
  }
  
  def append(modelo: Node,porcentaje: Double, ECM_train:Double, ECM_test:Double) {
    array_stats=array_stats :+ CurvePoint(modelo,porcentaje,ECM_train,ECM_test)
  }
  
  def round( n: Double ) : String = {
    if( java.lang.Double.isNaN(n) )
    {
      "NaN"
    }
    else
    {
      BigDecimal(n).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString()
    }
  }
  
  def getCurveForModel(modelo: Node): CurveGraph = {
    val points=array_stats.filter(x=> (x.model==modelo)).map(x=>(x.porcentage,x.ECM_train,x.ECM_test)).sortWith(_._1 < _._1)
    val index=points.map(_._1)
    val train_error=points.map(_._2)
    val test_error=points.map(_._3)
    CurveGraph(index,train_error,test_error, Array(),Array())
  }
  
  def mean(vector: Array[Double]): Double = vector.sum / vector.size
  def variance(vector: Array[Double]): Double = {
      val avg = mean(vector)
      vector.map(x => math.pow(x - avg, 2)).sum / vector.size
  }
  def stdDev(vector: Array[Double]): Double = math.sqrt(variance(vector))
  
  def getAverageCurveForModel(modelo: Node): CurveGraph = {
    val points=array_stats.filter(x=> (x.model==modelo))
    val lista_porcentajes=points.map(_.porcentage).distinct.sortWith(_< _)
    val avgCurve=lista_porcentajes.map{x=> 
      val puntos=array_stats.filter(y=> (y.porcentage==x))
      val ecmtrain= mean(puntos.map(y=> y.ECM_train))
      val ecmtest= mean(puntos.map(y=> y.ECM_test))
      val ecmtrain_std = stdDev(puntos.map(y=> y.ECM_train))
      val ecmtest_std = stdDev(puntos.map(y=> y.ECM_test))
      (x,ecmtrain,ecmtest,ecmtrain_std,ecmtest_std)
    }
    val index=avgCurve.map(_._1)
    val train_error=avgCurve.map(_._2)
    val test_error=avgCurve.map(_._3)
    val train_error_std=avgCurve.map(_._4)
    val test_error_std=avgCurve.map(_._5)
    CurveGraph(index,train_error,test_error,train_error_std,test_error_std)
  }

} //OverfittingTest


