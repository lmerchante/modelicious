package com.modelicious.in.tools

import org.apache.spark.rdd.RDD
import scala.reflect._
import scala.reflect.runtime.{universe => ru}
import com.modelicious.in.app.Application

case class ScorerStatistics( NumUnos: String, decil1: String, decil2: String, decil3: String, 
    decil4: String, decil5: String, decil6: String, decil7: String, decil8: String, decil9: String, Ganancia: String, Time: String )
    extends StatsBase {
  
  def this(NumUnos: Double, decil1: Double, decil2: Double, decil3: Double, 
    decil4: Double, decil5: Double, decil6: Double, decil7: Double, decil8: Double, decil9: Double, Ganancia: Double, Time: Double) 
  = this(NumUnos.toString, decil1.toString, decil2.toString, decil3.toString, 
    decil4.toString, decil5.toString, decil6.toString, decil7.toString, decil8.toString, decil9.toString, Ganancia.toString, Time.toString)
  
  def this(ss: Map[String,String])=
       this(ss("NumUnos"), ss("decil1"), ss("decil2"), ss("decil3"), ss("decil4"), ss("decil5"),
           ss("decil6"), ss("decil7"), ss("decil8"), ss("decil9"), ss("Ganancia"), ss("Time"))
  
  override def toMap: Map[String,(String,String)] = {  
     /* returns prediction statistics as a Map[String,(String,String)] with
      * _.1 = KPI Name
      * _.2._1 = KPI Type
      * _.2._2 = KPI_Value
      */
    
      val tt = ru.typeTag[ScorerStatistics]
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val im = m.reflect(this)
      val fields: scala.collection.mutable.ArrayBuffer[(String, String, String)] = scala.collection.mutable.ArrayBuffer()

      // Get all vals from case class, and create a tuple of name, type, value that is appended to fields
      tt.tpe.declarations.filter(_.asTerm.isVal).foreach { f =>
        val termfield = ru.typeOf[ScorerStatistics].member(f.name).asTerm
        val fieldMirror = im.reflectField(termfield)
        val field_tuple = (f.name.decoded.capitalize, termfield.typeSignature.toString.split('.').last, fieldMirror.get.toString ) 
        fields += field_tuple
      }
    
      val KPIMap = fields.map{ f => Map( f._1 ->( f._2, f._3 )) }.reduce(_ ++ _)
      KPIMap + ("ModelID" -> ("String",Application.get().ModelID)) + ("SessionID" -> ("String",Application.get().SessionID))
  }
 }


object ScorerStatistics {
    def apply( NumUnos: Double, decil1: Double, decil2: Double, decil3: Double, 
    decil4: Double, decil5: Double, decil6: Double, decil7: Double, decil8: Double, decil9: Double, Ganancia: Double, Time: Double) =
        new ScorerStatistics( NumUnos, decil1, decil2, decil3, decil4, decil5, decil6, decil7, decil8, decil9, Ganancia, Time )
}
   
 
object ScorerStats {
  
    def compute( rdd:RDD[(Double,Double)],  tiempo: Double= 0.0 ) : ScorerStatistics = {
    
      val n = rdd.count
      
      val total_ones = rdd.filter( _._2 == 1.0 ).count()
      val rdd_sorted = rdd.repartition(1).sortBy( _._1, false ).zipWithIndex()
   
      val deciles = (1 until 10).map{ x => math.floor( (x*n)/10.0 ).toLong }
      
      val deciles_values = rdd_sorted.filter( x => deciles.contains( x._2 ) ).map( _._1 ).map( _._1 ).collect
      
      val ones_in_first_decil = rdd_sorted.take( deciles(0).toInt ).filter( _._1._2 == 1.0 ).size

      val Ganancia = ones_in_first_decil/ ( total_ones/ 10.0 )
      
      val LiftValues = deciles.map(x=> rdd_sorted.take( x.toInt ).filter( _._1._2 == 1.0 ).size).zipWithIndex.map(x=> x._1/((x._2+1)*total_ones/ 10.0))
      
      println( "n: " + n )
      println( "total_ones: " + total_ones )
      println( "deciles: " + deciles.mkString( "[", ",", "]" ) )
      println( "deciles_values: " + deciles_values.mkString( "[", ",", "]" ) )
      println( "Ganancia: " + Ganancia )
      println( "Lift curve: "+LiftValues.mkString(","))
      

      // TODO: Add LiftValues to class
      
      ScorerStatistics( total_ones.toDouble, 
          deciles_values(0),
          deciles_values(1),
          deciles_values(2),
          deciles_values(3),
          deciles_values(4),
          deciles_values(5),
          deciles_values(6),
          deciles_values(7),
          deciles_values(8),
          Ganancia, 
          tiempo)
      

  }
}

class CVScorerStats extends CrossValidator[ScorerStatistics] {
  implicit val ct =  scala.reflect.classTag[ScorerStatistics]
  
  def getStats = new ScorerStatistics( meanANDstd )
}