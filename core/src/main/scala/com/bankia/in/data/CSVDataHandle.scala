package com.modelicious.in.data

import org.apache.hadoop.fs.Path
import com.modelicious.in.app.Application
import com.modelicious.in.data._
import org.apache.spark.sql.SQLContext

// all posible options:
// https://github.com/databricks/spark-csv#features
// Importantes:
// delimiter: Por defecto , | delimiter=^. Por ejemplo


class InputCSVDataHandle(uri: String, opts: Map[String, String]) extends InputDataHandle(uri, opts) {
  def doRead(): DataWrapper = {
    val sqlContext = Application.sqlContext
    
    var reader = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("inferSchema", "true")
    .option("header", "true")
    
    reader = opts.foldLeft(reader){ (w,f) => w.option(f._1, f._2)}
    
    new DataWrapper(reader.load(uri_df), DataWrapper.readMetaData(uri_md))
    
  }
}

class InputCSVGZDataHandle(uri: String, opts: Map[String, String])
  extends InputCSVDataHandle(uri, opts + (("codec", "gzip")) ) {}

// all posible options:
// https://github.com/databricks/spark-csv#features
// Importantes:
// delimiter: Por defecto , | delimiter=^. Por ejemplo
// 
// codec: compression codec to use when saving to file. Should be the fully qualified name of a class 
// implementing org.apache.hadoop.io.compress.CompressionCodec or one of case-insensitive shorten names (bzip2, gzip, lz4, and snappy). 
// Defaults to no compression when a codec is not specified.
//  Ej: codec=org.apache.hadoop.io.compress.GzipCodec
class OutputCSVDataHandle(uri: String, opts: Map[String, String]) extends OutputDataHandle(uri, opts) {
  def doWrite(data: DataWrapper) = {
    val path = new Path(uri_df)
    com.modelicious.in.tools.FileUtils.createOrCleanFolder( path )
    var writer = data.data.repartition(1).write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    
    writer = opts.foldLeft(writer){ (w,f) => w.option(f._1, f._2)}
    
    writer.save(uri_df)
    DataWrapper.writeMetaData(uri_md, data.getMetaData)
  }
}

// Por comodidad
class OutputCSVGZDataHandle(uri: String, opts: Map[String, String])
  extends OutputCSVDataHandle(uri, opts + (("codec", "gzip")) ) {}
