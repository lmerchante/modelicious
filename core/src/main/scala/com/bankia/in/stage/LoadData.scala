package com.modelicious.in.stage

import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls

import org.apache.log4j.Logger

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

import com.modelicious.in.app.Application
import com.modelicious.in.data.{DataManager, DataWrapper}
import com.modelicious.in.tools.AddChildrenTo
import com.modelicious.in.tools.Implicits._

import scala.xml.transform.RuleTransformer

class LoadData extends Stage{
  
  def execute( conf: scala.xml.Elem ): scala.xml.Elem = {

    val out_uri = conf \ "data" \ "output" \ "@url" text

    //val datos = calcula_datos_entrada()
    
    val sql = conf \ "sql" 
    val table = (sql \ "table" text).trim
    val label = (sql \ "label" text).trim
    val excluded = (sql \ "columns" \ "exclude" \ "column" ).map( _.text.trim  )
    var include_only = (sql \ "columns" \ "include_only" text ).trim.split(",").map( _.trim  )
    if (include_only.toList==List("")) include_only=Array[String]()
    if ((include_only.size != 0) && (!include_only.contains(label))) { include_only= label +: include_only }
    
    val limit = (sql\"limit").textAsIntOrElse(0) 
    var drop_table = (conf\"drop_table").textAsBoolOrElse(false)
    
    log.file("Label name: " +  label )
    log.file("Excluded: " +  excluded.mkString("[", ", ", "]") )
    log.file("Include only: "+ include_only.mkString("[", ", ", "]"))
    if( limit > 0 ) {log.file("Data limited to : "+ limit+ " rows")}
    if( drop_table == true ) {
      log.file("Table  : "+ table+ " will be deleted after using data")
      Application.stagesChannel+=("drop_table"->"true")
      Application.stagesChannel+=("table"->table)
      }
    else {Application.stagesChannel+=("drop_table"->"false")}
    
    val table_description = get_table_description( table )
    
    var non_present: List[String] = List()
    val columns = if (include_only.size!=0) {
      non_present = include_only.filterNot( x => excluded.contains( x ) ).filterNot( x => table_description.map( y => y._1 ).toList.contains(x) ).toList
      if( non_present.size > 0 ) {
        log.file( "Columns requested from configuration but not present in table: " + non_present.mkString("[",",","]") )
      }
      table_description.filter(x => include_only.contains(x._1)).filter( x => !excluded.contains( x._1 )  ) 
    } else {
      table_description.filter( x => !excluded.contains( x._1 )  )
    }
    
    log.file("Columns: " +  columns.mkString("[", ", ", "]") )
    
    val columns_SQL = build_column_list( columns, label )
    var the_SQL = s"SELECT " + columns_SQL + " FROM " + table 
    if( limit > 0 ) { the_SQL += " LIMIT " + limit.toString }
    
    
    
    log.file("SQL to be run: " + the_SQL)
    
    // Todo las SQL deberían ser gestionadas tb por un DataHandle
    val datos = new DataWrapper( Application.sqlContext.sql( the_SQL ))
    
    Application.dataManager.write(out_uri, datos)
    
    if( non_present.size > 0 ) {
      new RuleTransformer(new AddChildrenTo(conf, <result><missing_columns>non_present.mkString("[", ", ", "]")</missing_columns><columns_ordered>{columns.mkString("[",",","]")}</columns_ordered></result>))
        .transform(conf).head.asInstanceOf[scala.xml.Elem]
    }
    else {
        new RuleTransformer(new AddChildrenTo(conf, <result><columns_ordered>{columns.mkString("[",",","]")}</columns_ordered></result>))
        .transform(conf).head.asInstanceOf[scala.xml.Elem]
    }
    
    
  }
  
  private def get_table_description(table_name : String) :  Array[(Any, Any)] = {
    val describe = "DESCRIBE " + table_name 
    val columns = Application.sqlContext.sql( describe )
    
    columns.collect.map( {x => (x(0), x(1)) } )
  }
  
  private def build_column_list( columns: Array[(Any, Any)], label: String ) : String = {
    // Queremos siempre la etiqueta de primera, por comodida al trabajar con dataframes
    s" cast($label as double) as label, " +
    columns.filter(x => x._1!=label ).map{
       //case (`label`, _ ) => 
       case (a, "int") => s" cast($a as double) as $a "
       case (a, "bigint") => s" cast($a as double) as $a "
       case (a, "tinyint") => s" cast($a as double) as $a "
       case (a, b) if b.asInstanceOf[String].startsWith("decimal") => s" cast($a as double) as $a "
       case (a, b) => s" $a "
     }
//      columns.filter(x => x._1!=label ).map{ a =>  s" cast(${a._1} as double) as ${a._1} " }
     .mkString( ", " )
     
  }
  
}