package com.modelicious.in.data

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import com.modelicious.in.app.Application
import com.modelicious.in.data._

class InputHDFSDataHandle(uri: String, opts: Map[String, String]) extends InputDataHandle(uri, opts) {
  def doRead(): DataWrapper = {
    val sqlContext = Application.sqlContext

    val df = sqlContext.read.parquet(uri_df) /*.rdd.map(row => LabeledPoint(
       row.getAs[Double]("label"),   
       row.getAs[org.apache.spark.mllib.linalg.Vector]("features")
    )) */
    val md = DataWrapper.readMetaData(uri_md)
    
    new DataWrapper( df, md )
  }
}

class OutputHDFSDataHandle(uri: String, opts: Map[String, String]) extends OutputDataHandle(uri, opts) {
  def doWrite(data: DataWrapper) = {

    val path = new Path(uri_df)
    com.modelicious.in.tools.FileUtils.createOrCleanFolder( path )
    data.data.write.parquet(path.toString()) // write to parquet
    DataWrapper.writeMetaData(uri_md, data.getMetaData)
    
  }
}

