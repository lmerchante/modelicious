package com.modelicious.in.transform

import org.apache.spark.sql.DataFrame
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.SchemaTools
import com.modelicious.in.data.{DataWrapper, DataFrameExtender}
import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, Column }
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType, IntegerType, LongType }
import org.apache.spark.sql.types.MetadataBuilder
import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls
import java.util.NoSuchElementException
import javax.naming.Name

import org.fusesource.jansi.Ansi._
import org.fusesource.jansi.Ansi.Color._


case class noNumericColumn(text: String) extends Exception(text)

case class remove_outliers() extends TransformerState {
  var columns: Array[String] = Array("all")
  var times: Double = 3.0
  // var list_outliers_count: Boolean = false  // Deprecate
  var stats: MapOutliersStats = _
  var max_percentage_of_outliers: Double = 0.75
  var bulk: Boolean = false
}

class RemoveOutliersModel(theState: TransformerState) extends DataTransformerModel[remove_outliers](theState) {

  override def useForPrediction = false
  
  def doTransform(dw: DataWrapper): DataWrapper = {
    dw { data =>

      val sqlcontext = com.modelicious.in.app.Application.sqlContext
      var tmpdata = data

      log.file("RemoveOutliers: BEFORE")
      tmpdata.take(10).foreach(l => log.file(l.mkString(", ")))
      
      if (!((state.columns.size==1) && (state.columns(0)=="_none_"))) {
        if( state.bulk ) {
          tmpdata = bulkRemoveOutliers(tmpdata, state.columns.toList, state.stats)
        }
        else {
          for (columna <- state.columns) { tmpdata = removeOutliers(tmpdata, columna, state.stats(columna)) } 
        }
      }

      val lines_removed = state.stats.toList(0)._2.count - tmpdata.count()
      val newSchema = tmpdata.schema
      rslt = <params> <columns> { state.columns.mkString(";") } </columns> <lines_removed> { lines_removed } </lines_removed> </params>

      log.file("RemoveOutliers: AFTER")
      tmpdata.take(10).foreach(l => log.file(l.mkString(", ")))
      log.file("RemoveOutliers: SCHEMA DEF: " + newSchema.map(x => "(" + x.name + "," + x.dataType + "," + x.nullable + "," + x.metadata + ")").mkString(";"))


      tmpdata
    }.addToMetaData(state.toXML).checkPoint
  }

  private def removeOutliers(data: DataFrame, columna: String, stats: OutliersStats): DataFrame = {

    try {
      val tipo = data.dtypes.toMap.apply(columna)
      if (tipo == "StringType") { throw new noNumericColumn("Error: columna " + columna + " is not numerical") }
      filter_by_std_multiplier(state.times, columna, data, stats)
    } catch {
      case e: NoSuchElementException => {
        log.file("Error: columna " + columna + " does not exist in dataframe")
        log.file("Error: only columns: " + data.columns.toList.toString())
        log.file("Error:" + e.getStackTraceString + "\n" + e.getMessage())
        rslt = <result> Error: { e.getMessage() } </result>
        data
      }
      case e: noNumericColumn => {
        log.file("Error: columna " + columna + " is not numerical")
        rslt = <result> Error: { e.getMessage() } </result>
        data
      }
    }

  }

  private def filter_by_std_multiplier(std_multiplier: Double, column: String, data: DataFrame, stats: OutliersStats): DataFrame = {

    log.file("Column "+column+" count: " + stats.count)
    log.file("Column "+column+" mean: " + stats.mean)
    log.file("Column "+column+" std: " + stats.std)

    val df = data.filter(abs(data(column) - stats.mean) < (stats.std * std_multiplier + 0.0000000001))
    val colums = df.columns

    val newSchema = StructType(df.schema.filter( _.name == column ).map { x =>
        val mb = new MetadataBuilder()
        val mb_filled = mb.withMetadata(x.metadata)
          .putDouble("outliers_std_multiplier", state.times)
          .build()
        StructField(x.name, x.dataType, x.nullable, mb_filled)
    })
    df.updateWithMetaData(newSchema)
  } 
  
  private def bulkRemoveOutliers(data: DataFrame, columns: List[String], stats: MapOutliersStats): DataFrame = {

    try {
      for( c <- columns ) {
        val tipo = data.dtypes.toMap.apply(c)
        if (tipo == "StringType") { throw new noNumericColumn("Error: columna " + c + " is not numerical") }
      }
      bulk_filter_by_std_multiplier(state.times, columns, data, stats)
    } catch {
      case e: NoSuchElementException => {
        log.file("Error: Come column does not exist in dataframe [Remove Outliers Bulk Mode]\n" + e.getMessage)
        log.file("Error: only columns: " + data.columns.toList.toString())
        log.file("Error:" + e.getStackTraceString + "\n" + e.getMessage())
        rslt = <result> Error: { e.getMessage() } </result>
        data
      }
      case e: noNumericColumn => {
        log.file("Error: Some column is not numerical [Remove Outliers Bulk Mode]\n" + e.getMessage)
        rslt = <result> Error: { e.getMessage() } </result>
        data
      }
    }

  }
  
  private def bulk_filter_by_std_multiplier(std_multiplier: Double, columns: List[String], data: DataFrame, stats: MapOutliersStats): DataFrame = {

    log.file("Bulk mode outliers removal:")
    
    
    val bulk_expr_array = scala.collection.mutable.ArrayBuffer.empty[Column] 
    
    
    for( c <- columns ) {
      log.file("Column "+c+" count: " + stats(c).count)
      log.file("Column "+c+" mean: " + stats(c).mean)
      log.file("Column "+c+" std: " + stats(c).std)
      
      val newColumn = abs(col(c) - stats(c).mean) < (stats(c).std * std_multiplier + 0.0000000001)
      
      bulk_expr_array += newColumn
    }
    
    implicit val ct = scala.reflect.classTag[Column]
    
    val first = bulk_expr_array.toArray[Column].apply(0)
    val others = bulk_expr_array.toArray.drop(1)
    
    val bulk_condition = others.foldLeft( (first) ) ( (b,a) => b && (a))
    
    log.file(" Bulk condition to apply: " + bulk_condition.toString )
    
    val newSchema = StructType(data.schema.filter( columns.contains( _ )  ).map { x =>
        val mb = new MetadataBuilder()
        val mb_filled = mb.withMetadata(x.metadata)
          .putDouble("outliers_std_multiplier", state.times)
          .build()
        StructField(x.name, x.dataType, x.nullable, mb_filled)
    })
    data.filter( bulk_condition ).updateWithMetaData(newSchema)
  } 
  

}

class RemoveOutliers extends DataTransformer[remove_outliers] {

  override def fit(dw: DataWrapper): RemoveOutliersModel = {
    // Cant go in configure as we dont have schema info there yet
    state.columns = if (state.columns(0) == "all") { dw.data.columns.filter( _ != "label") } else { state.columns }
    state.stats = get_columns_statistics(dw.data, state.columns)
    
    val data = dw.data
          // Logging
    data.cache()
    val columns_to_process = scala.collection.mutable.ArrayBuffer[String]()
    for (columna <- state.columns) {
      val count = state.stats(columna).count
      val outliers = RemoveOutliers.countOutliers(data, columna, state )
      val percent: Double = outliers*100.0/count
      log.file("Outliers in column " + columna + ": " + outliers)
      log.file("Percent of outliers over total = " + f"$percent%1.5f" + "%")
      if( state.max_percentage_of_outliers > 0.0 ) {
        if( percent <= state.max_percentage_of_outliers && outliers > 0) {
          columns_to_process += columna
          log.file( "Column " + columna + " selected for outliers removal" )
        }
        else
        {
          log.file( "Column " + columna +"  will " + 
              ansi.fg(RED).a("NOT").reset +" be selected for outliers removal" )
        }
      }
    }
    
    if( state.max_percentage_of_outliers > 0.0 ) {
      state.columns = if (columns_to_process.size!=0) columns_to_process.toArray else Array("_none_")
      log.file( "The columns to be processed by remove outliers are:\n" + state.columns.mkString("{",",","}") )
    }
    
    data.unpersist
    new RemoveOutliersModel(state)
    
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
}

protected object RemoveOutliers {
  
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  private def countOutliers(data: DataFrame, columna: String, state: remove_outliers): Long = {
    try {
      val stats = state.stats(columna)
      val times = state.times
      
      val tipo = data.dtypes.toMap.apply(columna)
      if (tipo == "StringType") { throw new noNumericColumn("Error: columna " + columna + " is not numerical") }
      stats.count - data.filter(abs(data(columna) - stats.mean) < (stats.std * times + 0.0000000001)).count()
    } catch {
      case e: NoSuchElementException => {
        log.file("Error: columna " + columna + " does not exist in dataframe")
        log.file("Error: only columns: " + data.columns.toList.toString())
        log.file("Error:" + e.getStackTraceString + "\n" + e.getMessage())
        0L
      }
      case e: noNumericColumn => {
        log.file("Error: columna " + columna + " is not numerical")
        0L
      }
    }
  }
}
