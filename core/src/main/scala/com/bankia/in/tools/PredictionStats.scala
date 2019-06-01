package com.modelicious.in.tools


import org.apache.spark.rdd.RDD

import com.modelicious.in.app.Application
import org.apache.log4j.Logger

import com.modelicious.in.tools.Implicits._

import scala.reflect._
import scala.reflect.runtime.{universe => ru}
import com.modelicious.in.app.Application

private case class KPIObject( kpiName: String, kpiType: String, kpiValue: String )
  
private case class JSONKPIObject( KPI: Array[KPIObject], sessionID: String, modelID: String  )  

case class PredictionStatistics( num_clientes: Long, decil1: Double, decil2: Double, decil3: Double, 
    decil4: Double, decil5: Double, decil6: Double, decil7: Double, decil8: Double, decil9: Double ) {
  
    def toJson: String = {  
    
   val tt = ru.typeTag[PredictionStatistics]
    
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val im = m.reflect(this)
      val fields: scala.collection.mutable.ArrayBuffer[(String, String, String)] = scala.collection.mutable.ArrayBuffer()

      // Get all vals from case class, and create a tuple of name, type, value that is appended to fields
      tt.tpe.declarations.filter(_.asTerm.isVal).foreach { f =>
        val termfield = ru.typeOf[PredictionStatistics].member(f.name).asTerm
        val fieldMirror = im.reflectField(termfield)
        val field_tuple = (f.name.decoded.capitalize, termfield.typeSignature.toString.split('.').last, fieldMirror.get.toString ) 
        fields += field_tuple
      }
    
    val KPIArray = fields.map{ f => KPIObject( f._1, f._2, f._3 ) }.toArray
    
    val theJSONObject = JSONKPIObject( KPIArray, Application.get().SessionID, Application.get().ModelID )
    
    return JsonUtil.toJson( theJSONObject )
  }
    
   def toMap: Map[String,(String,String)] = {  
     /* returns prediction statistics as a Map[String,(String,String)] with
      * _.1 = KPI Name
      * _.2._1 = KPI Type
      * _.2._2 = KPI_Value
      */
    
      val tt = ru.typeTag[PredictionStatistics]
    
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val im = m.reflect(this)
      val fields: scala.collection.mutable.ArrayBuffer[(String, String, String)] = scala.collection.mutable.ArrayBuffer()

      // Get all vals from case class, and create a tuple of name, type, value that is appended to fields
      tt.tpe.declarations.filter(_.asTerm.isVal).foreach { f =>
        val termfield = ru.typeOf[PredictionStatistics].member(f.name).asTerm
        val fieldMirror = im.reflectField(termfield)
        val field_tuple = (f.name.decoded.capitalize, termfield.typeSignature.toString.split('.').last, fieldMirror.get.toString ) 
        fields += field_tuple
      }
    
      val KPIMap = fields.map{ f => Map( f._1 ->( f._2, f._3 )) }.reduce(_ ++ _)
      KPIMap + ("ModelID" -> ("String",Application.get().ModelID)) + ("SessionID" -> ("String",Application.get().SessionID))
  }
  
}


object PredictionStats {
  
    def compute( rdd:RDD[(Double,Double)] ) : PredictionStatistics = {
    
      val n = rdd.count
      
      val rdd_sorted = rdd.repartition(1).sortBy( _._1, false ).zipWithIndex()
   
      val deciles = (1 until 10).map{ x => math.floor( (x*n)/10.0 ).toLong }
      
      val deciles_values = rdd_sorted.filter( x => deciles.contains( x._2 ) ).map( _._1 ).map( _._1 ).collect
      
      val ones_in_first_decil = rdd_sorted.take( deciles(0).toInt ).filter( _._1._2 == 1.0 ).size

      PredictionStatistics(  
          n,
          deciles_values(0),
          deciles_values(1),
          deciles_values(2),
          deciles_values(3),
          deciles_values(4),
          deciles_values(5),
          deciles_values(6),
          deciles_values(7),
          deciles_values(8))
  }
}

