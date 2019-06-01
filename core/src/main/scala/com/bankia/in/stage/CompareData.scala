package com.modelicious.in.stage

import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools._

import com.modelicious.in.app.Application

import com.modelicious.in.data.DataManager

import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls

import org.apache.spark.sql.DataFrame

import com.modelicious.in.tools.AddChildrenTo
import scala.xml.transform.RuleTransformer

class CompareData extends Stage {

  def execute( conf: scala.xml.Elem ): scala.xml.Elem = {
    
    val in1_uri = conf \ "data" \ "input1" \ "@url" text
    val in2_uri = conf \ "data" \ "input2" \ "@url" text
    
    val input_data1 = Application.dataManager.read(in1_uri)
    val input_data2 = Application.dataManager.read(in2_uri)
    
    val columns1 = input_data1.data.columns
    val columns2 = input_data2.data.columns
    
    val sameSchema = columns1.sameElements( columns2 )
    
    val df1 = renameColumns( input_data1.data, "1" )
    val df2 = renameColumns( input_data2.data, "2" )
    val dups = df1.intersect( df2 )
    
    val ndups = dups.count
    val ndf1 = df1.count
    val ndf2 = df2.count

    log.file(" Counts: ")
    log.file(" DF1: " + ndf1)
    log.file(" DF2: " + ndf2)
    log.file(" Intersect: " + ndups)
    log.file(" DataSets are equal: " + (ndups == ndf1 && ndups == ndf2 && sameSchema) )
    Application.document.add(" Counts: ")
    Application.document.add(" DF1: " + ndf1)
    Application.document.add(" DF2: " + ndf2)
    Application.document.add(" Intersect: " + ndups)
    Application.document.add(" DataSets are equal: " + (ndups == ndf1 && ndups == ndf2 && sameSchema) )
    

    try {
      if (Application.dataManager.deleteLocal(in2_uri)){
        log.file(" Deleted test product: " + in2_uri)
      } else {
        log.file(" Could not delet test product: " + in2_uri)
      }
    }
    catch {
      case _:Throwable => log.file(" Could not delete test product: " + in2_uri)
    }

    try {
      if (Application.stagesChannel("drop_table")=="true"){
        val table=Application.stagesChannel("table")
        val table_exists = scala.util.Try(Application.sqlContext.table(table)).toOption.isDefined
        if ( table_exists ) {
          val setInternal = "ALTER TABLE " + table + " SET TBLPROPERTIES('EXTERNAL'='FALSE')" 
          val drop = "Drop table if exists " + table 
          log.file("Set Internal Table Query = " + setInternal)
          Application.sqlContext.sql(setInternal)
          log.file("Drop Table Query = " + drop)
          Application.sqlContext.sql(drop)
          log.file("Deleted table  : "+ table)
        }
      } 
    }
    catch {
      case _:Throwable => log.file(" Could not delete test table: " + Application.stagesChannel("table"))
    }
    
    _error_code = if(ndups == ndf1 && ndups == ndf2 && sameSchema) {0} else {1}
    
    val result = <result><SameSchema>{sameSchema}</SameSchema><SameData>{ ndups == ndf1 && ndups == ndf2 }</SameData></result>
    
    new RuleTransformer(new AddChildrenTo(conf, {result})).transform(conf).head.asInstanceOf[scala.xml.Elem]

  }

  
  private def renameColumns( df: DataFrame, suffix: String ): DataFrame = {
    val renamed_cols = df.columns.filter( _ != "label").map( c => ( c, c+suffix ) )
    renamed_cols.foldLeft( df ) ( (d, n) => d.withColumnRenamed(n._1, n._2) )
  }

}