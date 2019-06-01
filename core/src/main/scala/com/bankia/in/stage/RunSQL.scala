package com.modelicious.in.stage

import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls

import org.apache.log4j.Logger


import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions.col
import com.modelicious.in.app.Application
import com.modelicious.in.data.{DataManager, DataWrapper}
import com.modelicious.in.tools.Implicits._

class RunSQL extends Stage{
  
  def execute( conf: scala.xml.Elem ): scala.xml.Elem = {

    val out_uri = conf \ "data" \ "output" \ "@url" text

    //val datos = calcula_datos_entrada()
    
    val settings = (conf \ "settings")
    
    if ( !settings.isEmpty ) {
      for (set <- (settings\"set")) {
        val s = set.text
        log.file("Setting " + s)
        Application.sqlContext.sql( s )
      }
    }
    
    val sql = (conf \ "sql" ).text
    val label = (conf \ "label").text
    log.file("SQL to be run: " + sql)
       
    val my_df = Application.sqlContext.sql( sql )
    Application.document.addSubSection("Query Executed: ","Final query with parametrized variables resolved")
    Application.document.addQuery(sql,1)
    
    val cols = my_df.columns
    
    // renombramos la columna que sea la etiqueta ( labrel en train, cliente en predicion  ) a label, que es lo q esperan todas
    // nuestras fucniones, además, de primera. Esta es la convención definida por la aplicación
    val with_label = my_df.withColumnRenamed(label, "label").select( "label", cols.filter( _ != label ):_* )
    
    // Y ahora hacemos un cast de todas las columnas a double type
    
    val ordered_double_data = with_label.select(with_label.columns.map(name => toDouble(col(name))): _*)
    
    log.file( "SQL result: " )
    for(r <- ordered_double_data.take(10)) {
      log.file( r.toString() )
    }
    
    val datos = new DataWrapper( ordered_double_data ) 
    
    Application.dataManager.write(out_uri, datos)
    
    conf
  }
    
  def toDouble(column: Column) = column.cast(DoubleType)
}
