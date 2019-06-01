package com.modelicious.in.transform

import com.modelicious.in.app.Application
import com.modelicious.in.Constants
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.SchemaTools
import com.modelicious.in.data.DataWrapper

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{ DataFrame, Row, Column }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType, IntegerType }

import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls
import scala.math.abs

case class purge_correlated() extends TransformerState {
  var max_correlation: Double = 0.95
  var columns_purged: Array[String] = Array()
}

class PurgeCorrelatedTransformModel(theState: TransformerState) extends DataTransformerModel[purge_correlated](theState) {

  def doTransform(dw: DataWrapper): DataWrapper = {
    dw { data =>
      var df_out = data.select(data.columns.filter(colName => !state.columns_purged.contains(colName)).map(colName => new Column(colName)): _*)
      log.file(s"Final SCHEMA:" + SchemaTools.schemaToXMLString(df_out.schema))
      rslt = <columns_purged> { state.columns_purged.toList.toString } </columns_purged>
      df_out
    }.addToMetaData(state.toXML)
  }

  override def useForPrediction = false
}

class PurgeCorrelatedTransform extends DataTransformer[purge_correlated] {

  override def fit(dw: DataWrapper): PurgeCorrelatedTransformModel = {
    var df_temp = dw.data.drop("label")
    val columns = df_temp.columns

    log.file(s"Remove correlated columns from: " + columns.toList.toString())

    val rdd = df_temp.rdd.map(x => Vectors.dense(x.toSeq.toArray.map(x => x.asInstanceOf[java.lang.Number].doubleValue())))
    val correlMatrix = Statistics.corr(rdd, "pearson")

    log.file(">>>> CORRELATION_MATRIX")
    correlMatrix.transpose.toArray.grouped(correlMatrix.numCols).toList.map(line => line.mkString(" ")).foreach(linea => log.file(">>>>" + linea))

    val columnas_correladas = scala.collection.mutable.ArrayBuffer[Int]()
    for (fil <- 0 to correlMatrix.numRows - 1; col <- 0 to correlMatrix.numCols - 1) {
      if ((fil < col) & (Math.abs(correlMatrix(fil, col)) > state.max_correlation)) {
        log.file(">>>> Mark colum: " + columns(col) + " to be removed for being correlated with: " + columns(fil))
        columnas_correladas += col
      }
    }
    state.columns_purged = columnas_correladas.distinct.map(x => columns(x)).toArray

    new PurgeCorrelatedTransformModel(state)
  }

}