package com.modelicious.in.transform


import com.modelicious.in.app.Application
import com.modelicious.in.Constants
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.SchemaTools
import com.modelicious.in.data.DataWrapper

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Row,Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, IntegerType}

import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls
import scala.math.abs

case class purge_invariant() extends TransformerState {
  var min_relative_std: Double = 0.0
  var columns_purged: Array[String] = Array()
  var auto: Boolean = false
  var exclude: List[String]= List()
}

class PurgeInvariantTransformModel( theState: TransformerState ) extends DataTransformerModel[purge_invariant](theState) {

   def doTransform( dw: DataWrapper): DataWrapper = {
    dw { data =>
    
        var oldSchema=data.schema
        var df_temp=data
        val columns= state.columns_purged
        
        log.file(s"Remove invariant columns from: "+ columns.toList.toString())
        for (column<- columns) {
            //log.file(s"Remove invariant column: "+column+" with relative standard deviation: "+rel_std)
            df_temp=df_temp.drop(column)
        }
        var newSchema = df_temp.schema
        
        log.file(s"Final SCHEMA:"+newSchema.toList.toString())
        rslt = <columns_purged> {state.columns_purged.toList.toString} </columns_purged>
        df_temp
        
    }.addToMetaData( state.toXML )
  } 
  override def useForPrediction = false
}

class PurgeInvariantTransform extends DataTransformer[purge_invariant] {
  
  override def fit( dw: DataWrapper ): PurgeInvariantTransformModel = {

    val columns_purged = scala.collection.mutable.ArrayBuffer[String]()
    val data = dw.data
    val columns_org = dw.data.columns
    log.file( s"Remove invariant columns from: "+columns_org.toList.toString() )
    log.file( s"Exclude columns: "+state.exclude.toString() )
    val columns=columns_org.filter(x=> !state.exclude.contains(x))
    
    
    val stats = get_columns_statistics( data, columns )
    
    log.file( s"Resulting columns to remove invariant: "+columns.toList.toString() )
    if (state.auto==true) {
          state.min_relative_std = compute_relative_std("label",stats)/10
          log.file(s"AUTO selected. Override min_relative_std to: "+state.min_relative_std)
    }
    for (column<- columns) {
      val rel_std=compute_relative_std(column,stats)
      log.file(s"Column "+column+" min_relative_std: "+rel_std)
      if ( rel_std < state.min_relative_std) {
        log.file(s"Column "+column+" marked for removal")
        columns_purged += column
        }
     }
    state.columns_purged = columns_purged.toArray
    
    new PurgeInvariantTransformModel( state )
  }
  
  private def get_columns_statistics(data: DataFrame, cols: Array[String]): MapOutliersStats = {
    val stats = data.describe(cols: _*).map { r => r.toSeq.toList }.collect().map { r => r.drop(1).map { n => n.asInstanceOf[String] } }
    val count = 0
    val mean = 1
    val std = 2

    val cols_names_index = cols.zipWithIndex.toMap
    val reversed_stats = Map[String, List[Double]]()
    data.schema
      .filter(x => cols.contains(x.name))
      .map { x =>
        (x.name -> OutliersStats(
          stats(count)(cols_names_index(x.name)).toLong,
          stats(mean)(cols_names_index(x.name)).toDouble,
          stats(std)(cols_names_index(x.name)).toDouble))
      }.toMap.asInstanceOf[MapOutliersStats] // Evita q el compilador de eclipse de error
  }

  private def compute_relative_std(column: String, stats: MapOutliersStats): Double = {
    
    val count=stats(column).count
    val mean= stats(column).mean
    val std = stats(column).std
    if (std==0 & mean==0) 0.0 else if (mean==0) std/abs(mean+0.0000001) else std/abs(mean)
  }
  
  
//  private def compute_relative_std(column: String, data: DataFrame): Double = {
//    val stats_column=data.describe(column).collect()
//    val count=stats_column(0).getString(1).toLong
//    val mean= stats_column(1).getString(1).toDouble
//    val std = stats_column(2).getString(1).toDouble
//    if (std==0 & mean==0) 0.0 else if (mean==0) std/abs(mean+0.0000001) else std/abs(mean)
//  }
    
  
  
}