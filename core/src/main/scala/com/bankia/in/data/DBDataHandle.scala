package com.modelicious.in.data

import org.apache.hadoop.fs.Path
import com.modelicious.in.app.Application
import com.modelicious.in.data._
import com.modelicious.in.tools.{PredictionStats, FileUtils,JsonUtil}
import com.modelicious.in.tools.Implicits._

import org.apache.spark.sql.{ SQLContext, Row }
import org.apache.spark.sql.types.{ StructType, StructField }

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.parsing.json.JSON.parseRaw
// 
class OutputDBDataHandle(uri: String, opts: Map[String, String]) extends OutputDataHandle(uri, opts) {
  def doWrite(data: DataWrapper) = {

    val dyn_part = Application.sqlContext.getConf("hive.exec.dynamic.partition")
    val part_mode = Application.sqlContext.getConf("hive.exec.dynamic.partition.mode")
    val concurrency = Application.sqlContext.getConf("hive.support.concurrency")

    val table = uri.toLowerCase()
    // Convert to mutable, so we can drop as we use them( For the partition By opts)
    val myOpts = collection.mutable.Map(opts.toSeq: _*)

    // Partitions
    val partitions: Option[Array[String]] = if (myOpts.contains("partitionBy")) {
      val part = myOpts("partitionBy").split(',').map { _.trim }.map { _.toLowerCase }
      myOpts -= "partitionBy"
      Some(part)
    } else {
      None
    }
    val data_to_write = data.data
      .withColumnRenamed("prediction", "probabilidad")
      .withColumnRenamed("efficiency", "eficacia")
      .withColumnRenamed("client", "co_cliente")
  
    if( myOpts.contains("dropFirst") && myOpts("dropFirst") == "true") {
      dropTable( table )
      myOpts -= "dropFirst"
    }
      
    // TODO: Put on their own traits (separated)
    // Allow to use local/remote urls...
    //////////////////////////////////////////////
    // Inventory Control TODO: Move out!
    if (myOpts.contains("ic")) {
      val parts = partitions.getOrElse(Array.ofDim(3))
      val icPath = myOpts("ic")
//      val sqlcontext = Application.sqlContext
//      import sqlcontext.implicits._
        val icTuple = Seq(table, parts.lift(0).getOrElse(""), parts.lift(1).getOrElse(""), parts.lift(2).getOrElse(""),
        new SimpleDateFormat("YYYYMMDD").format(Calendar.getInstance.getTime) // The logic time si, right now, the generation time )
        )
//      
//      icTuple.toDF("name", "partition1", "partition2", "partition3", "logicTime").repartition(1).write
//        .format("com.databricks.spark.csv")
//        .option("header", "true").save(icPath)

      val csv = "name,partition1,partition2,partition3,logicTime\n" + icTuple.toList.mkString("",",","")
      
      FileUtils.writeStringToLocalFile(icPath, csv)
      Application.document.addSubSection("Output Files: Inventory Control","Filename: \n\n"+icPath+"\n\n"+"Content: \n\n"+csv.replace("\n","\n\n"))
      myOpts -= "ic"
    }
    
    // JSON Stats TODO: Move out!
    if (myOpts.contains("json")) {
      
      val jsonPath = myOpts("json")
      val rdd_data = data_to_write.select( "probabilidad", "co_cliente" ).rdd.map{ r => ( r.getString(0).toDouble, r.getString(1).toDouble ) }
      val stats = PredictionStats.compute( rdd_data ).toJson
      
      FileUtils.writeStringToLocalFile(jsonPath, stats)
      Application.document.addSubSection("Output Files: KPIs Json File","Filename: \n\n"+jsonPath+"\n\n"+"Content: \n\n")
      Application.document.addJSON(JsonUtil.format(parseRaw(stats).get),1)
      myOpts -= "json"
    }
    /////////////////////////////////////////////////////

    // Safety checks!
    // Check there exists this table, else create it with our schema
    // But if it exists, we must ensure that we insert with the very same column order!
    // https://issues.apache.org/jira/browse/SPARK-19742
    // Working as intended
    val tableDF = scala.util.Try(Application.sqlContext.table(table)).toOption
    val columns = tableDF match {
      case Some(df) => df.columns
      case None => {
        // If table does not exists, we must create it via SQL, else, it will create a table with an invalid Array<cols> schema.
        // https://stackoverflow.com/questions/39765737/spark-sql-dataframe-save-with-partitionby-is-creating-an-array-column
        // https://stackoverflow.com/questions/31482798/save-spark-dataframe-to-hive-table-not-readable-because-parquet-not-a-sequence
        // In production this shouldnt happen, anywhere, as tables should be precreated ...
        createDB(table, data_to_write.schema, partitions)
        data_to_write.columns
      }
    }

    log.file("OutputDBDataHandle to table: " + table)
    log.file("Column order to insert:" + columns.mkString("[", ",", "]"))

    // Code to write
    val df_to_write = data_to_write.select(columns.head, columns.drop(1): _*)
    log.file(df_to_write.columns.mkString("{", ",", "}"))
    df_to_write.take(20).foreach { x => log.file(x.toSeq.mkString("[", ",", "]")) }

    var writer = df_to_write.write
    if (partitions.isDefined) {
      Application.sqlContext.setConf("hive.exec.dynamic.partition", "true")
      Application.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      Application.sqlContext.setConf("hive.support.concurrency", "false")
      writer.partitionBy(partitions.get: _*)
    }
    // Other options?    
    writer = myOpts.foldLeft(writer) { (w, f) => w.option(f._1, f._2) }

    // We created it if it was empty, so always insert
    writer.insertInto(table)

    Application.sqlContext.setConf("hive.exec.dynamic.partition", dyn_part)
    Application.sqlContext.setConf("hive.exec.dynamic.partition.mode", part_mode)
    Application.sqlContext.setConf("hive.support.concurrency", concurrency)

    // metadata is lost, cause this is only useful to store in DB
    //DataWrapper.writeMetaData(uri_md, data.getMetaData)
  }

  protected def dropTable( tableName: String ) = {
    
    
      
    // If its not internal, it wont drop the data, and when recreating, it will reuse it
    // ALTER TABLE doesnt allow if exist, so we ust check that the table exists
    
    val table_exists = scala.util.Try(Application.sqlContext.table(tableName)).toOption.isDefined
    if( table_exists ) { 
      val setInternal = "ALTER TABLE " + tableName + " SET TBLPROPERTIES('EXTERNAL'='FALSE')" 
      val drop = "Drop table if exists " + tableName 
          
      log.file("Set Internal Table Query = " + setInternal)
      Application.sqlContext.sql(setInternal)
      log.file("Drop Table Query = " + drop)
      Application.sqlContext.sql(drop)
    }
    
    
  }
  
  protected def createDB(tableName: String, columns: StructType, partitions: Option[Array[String]] = None) = {

    val schema = columns
    val (non_partitioned_schema, partitions_schema) = partitions match {
      case Some(a) => (new StructType(schema.filterNot { f => a.contains(f.name) }.toArray), new StructType(schema.filter { f => a.contains(f.name) }.toArray))
      case None    => (schema, new StructType())
    }

    //var query = "create external table if not exists " + tableName + list_of_fields_for_query(non_partitioned_schema)
    var query = "create external table if not exists " + tableName + list_of_fields_for_query(non_partitioned_schema)

    if (partitions.isDefined) {
      query += " partitioned by " + list_of_fields_for_query(partitions_schema)
    }

    log.file("Create Table Query = " + query)

    Application.sqlContext.sql(query)
  }

  private def list_of_fields_for_query(schema: StructType): String = {
    schema.map { f =>
      f.name + " " + f.dataType.simpleString
    }.mkString("(", ",", ")")
  }

}

class OutputScoreDBDataHandle(uri: String, opts: Map[String, String]) extends OutputDBDataHandle(uri, opts + (("partitionBy", "modelid,sessionid"))) {
  override def doWrite(data: DataWrapper) = {
    import org.apache.spark.sql.functions.lit
    val partitioned_df = data.data.withColumn("modelid", lit(Application.get().ModelID))
      .withColumn("sessionid", lit(Application.get().SessionID))

    super[OutputDBDataHandle].doWrite(new DataWrapper(partitioned_df, data.getMetaData()))

  }
}


