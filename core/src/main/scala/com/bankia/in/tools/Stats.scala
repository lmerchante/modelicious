package com.modelicious.in.tools

import org.apache.spark.rdd.RDD
import scala.reflect._
import scala.reflect.runtime.{universe => ru}
import com.modelicious.in.app.Application

trait StatsBase {
  
  def toMap: Map[String,(String,String)] 
}

 case class Statistics( Accuracy: String, TPR: String, TNR: String, FPR: String, FNR: String, P: String, N: String, VolCampanha: String, 
     Precision: String, PrecisionMinima: String, VolCampaniaExclu: String, PrecisionExclu: String, PrecisionMinimaExclu: String, Ganancia:String, Tiempo: String)
     extends StatsBase {
  
  def this( Accuracy: Double, TPR: Double, TNR: Double, FPR: Double, FNR: Double, P: Double, N: Double, VolCampanha: Double, 
      Precision: Double, PrecisionMinima: Double, VolCampaniaExclu: Double, PrecisionExclu: Double, PrecisionMinimaExclu: Double, 
      Ganancia: Double, Tiempo: Double ) = 
  this( Accuracy.toString, TPR.toString, TNR.toString, FPR.toString, FNR.toString, P.toString, N.toString, 
      VolCampanha.toString, Precision.toString, PrecisionMinima.toString, VolCampaniaExclu.toString, PrecisionExclu.toString, 
      PrecisionMinimaExclu.toString, Ganancia.toString, Tiempo.toString )  
          
  def this(ss: Map[String,String]) = 
       this(ss("Accuracy"), ss("TPR"), ss("TNR"), ss("FPR"), ss("FNR"), ss("P"),
           ss("N"), ss("VolCampanha"), ss("Precision"), ss("PrecisionMinima"), ss("VolCampaniaExclu"), 
           ss("PrecisionExclu"), ss("PrecisionMinimaExclu"), ss("Ganancia"), ss("Tiempo"))
  
  override def toMap: Map[String,(String,String)] = {  
     /* returns prediction statistics as a Map[String,(String,String)] with
      * _.1 = KPI Name
      * _.2._1 = KPI Type
      * _.2._2 = KPI_Value
      */
    
      val tt = ru.typeTag[Statistics]
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val im = m.reflect(this)
      val fields: scala.collection.mutable.ArrayBuffer[(String, String, String)] = scala.collection.mutable.ArrayBuffer()

      // Get all vals from case class, and create a tuple of name, type, value that is appended to fields
      tt.tpe.declarations.filter(_.asTerm.isVal).foreach { f =>
        val termfield = ru.typeOf[Statistics].member(f.name).asTerm
        val fieldMirror = im.reflectField(termfield)
        val field_tuple = (f.name.decoded.capitalize, termfield.typeSignature.toString.split('.').last, fieldMirror.get.toString ) 
        fields += field_tuple
      }
    
      val KPIMap = fields.map{ f => Map( f._1 ->( f._2, f._3 )) }.reduce(_ ++ _)
      KPIMap + ("ModelID" -> ("String",Application.get().ModelID)) + ("SessionID" -> ("String",Application.get().SessionID))
  }
  
}
 
object Statistics {
    def apply( Accuracy: Double, TPR: Double, TNR: Double, FPR: Double, FNR: Double, P: Double, N: Double, VolCampanha: Double, 
        Precision: Double, PrecisionMinima: Double, VolCampaniaExclu: Double, PrecisionExclu: Double, PrecisionMinimaExclu: Double,
        Ganancia: Double, Tiempo: Double ) =
        new Statistics( Accuracy, TPR, TNR, FPR, FNR, P, N, 
      VolCampanha, Precision, PrecisionMinima, VolCampaniaExclu, PrecisionExclu, PrecisionMinimaExclu, Ganancia, Tiempo )
}

object Stats {
  // To add new stats, simply:
  //     1. include delcaration in case class and
  //     2. computations in "compute" method
  //     3. include in convertMapToStatistics()
  
  // Define the case class as String to be able to add later the "mean(std)" using the same class    
  
  def compute( rdd:RDD[(Double,Double)], n:Long, tiempo: Double= 0.0 ) : Statistics = {
    
      val Accuracy = 1.0 * rdd.filter(x => x._1 == x._2).count() / n
      val TN = 1.0 * rdd.filter(x => x._1 == 0.0 && x._2 == 0.0).count()
      val TP = 1.0 * rdd.filter(x => x._1 == 1.0 && x._2 == 1.0).count()
      val FN = 1.0 * rdd.filter(x => x._1 == 0.0 && x._2 == 1.0).count()
      val FP = 1.0 * rdd.filter(x => x._1 == 1.0 && x._2 == 0.0).count()
      val TPR = if (TP+FN==0) 0.0 else TP/(TP+FN)
      val TNR = if (TN+FP==0) 0.0 else TN/(TN+FP)
      val FPR = if (FP+TN==0) 0.0 else FP/(FP+TN)
      val FNR = if (TP+FN==0) 0.0 else FN/(TP+FN)
      val P=TP+FN
      val N=TN+FP
      val VolCampania = TP+FP
      val Precision = if (TP+FP==0) 0.0 else TP/(TP+FP)
      val PrecisionMinima = P/(N+P) // si digo que todo mi universo es propenso, cuanto acierto
                                    // o si cojo cualquier subconjunto aleatorio como campana
      val VolCampaniaExclu = TN+FN
      val PrecisionExclu = if (TN+FN==0) 0.0 else TN/(TN+FN)
      val PrecisionMinimaExclu = N/(N+P) // si digo que todo mi universo no es propenso, cuanto acierto
                                    // o si cojo cualquier subconjunto aleatorio como campana					
      
      val Ganancia = Precision/PrecisionMinima
      Statistics(Accuracy.toString,TPR.toString,TNR.toString,FPR.toString,FNR.toString,P.toString,N.toString, VolCampania.toString,Precision.toString,
          PrecisionMinima.toString,VolCampaniaExclu.toString, PrecisionExclu.toString, PrecisionMinimaExclu.toString, Ganancia.toString, tiempo.toString())
  }
  
}

trait CrossValidator[ T <: StatsBase ] {
   // store and average the stats computed for every KFold
 
  import scala.reflect.ClassTag
  
  implicit val ct: ClassTag[T]
  
  var array_stats: List[T] = List()
  
  def append( stats: T) {
    array_stats=array_stats :+ stats
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
  
  def meanANDstd(): Map[String,String] = {
    // have to read from String and convert to Double to be able to compute mean and std
    var array=Array[(String,Double)]()
    var stats_names=Array[String]()
    val Kfolds=array_stats.length
    for (index <- 0 to Kfolds-1) {
      array_stats(index).getClass.getDeclaredFields.foreach{x=>
        x.setAccessible(true)
        val name=x.getName
        val value=x.get(array_stats(index)).asInstanceOf[String].toDouble
        array=array :+ (name,value)
        stats_names=stats_names :+ name
      }
    }
     val mapDeStats=stats_names.distinct.map{x=>
       val mean=array.filter(y=> y._1==x).map(_._2).sum/Kfolds
       val std=math.sqrt(array.filter(y=> y._1==x).map(z=> math.pow(z._2-mean,2)).sum/Kfolds)
       (x ,round(mean) +"("+round(std)+")")
      }.toMap
    return mapDeStats
  }
  
  def getStats: T
  
}

class CVStats extends CrossValidator[Statistics] {
  implicit val ct =  scala.reflect.classTag[Statistics]
  
  def getStats = new Statistics( meanANDstd )
}
