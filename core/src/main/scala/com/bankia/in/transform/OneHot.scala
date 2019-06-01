package com.modelicious.in.transform

import com.modelicious.in.app.Application
import com.modelicious.in.Constants
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.SchemaTools

import org.apache.log4j.Logger
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.ml.feature.{ BinaryOneHotEncoder, OneHotEncoder }
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, Column }
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType, IntegerType, LongType }
import org.apache.spark.sql.types.MetadataBuilder

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException

import java.{ util => ju }

import scala.collection.mutable.ArrayBuffer
import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls

case class onehot() extends TransformerState {
  var columns: Array[String] = Array("")
  var labels: OneHotLabels = Map[String, Array[String]]()
  var binary: Boolean = false
}

class OneHotTransformModel(theState: TransformerState) extends DataTransformerModel[onehot](theState) {

  def doTransform(dw: DataWrapper): DataWrapper = {
    val sqlcontext = com.modelicious.in.app.Application.sqlContext
    var n_onehot = scala.collection.mutable.Map[String, Int]()
    var newSchema = new StructType()
    
    log.file( "Total input columns: " + dw.data.count  )
    
    dw { data =>
      var tmpdata = data
      tmpdata.cache()
      state.labels.foreach({
        case (column, labels) =>
          try {
            log.file("ONE HOT FOR " + column + " With labels " + labels.toList.toString)
            // To create Metadata
            val oldschema = tmpdata.schema
            import scala.util.Try
            // Try a second time, if we had a problem of case with columns.
            // WE use the lowercase version od the column. Should it fail again, throw
            val metadata_to_propagate = oldschema(oldschema.fieldIndex(column)).metadata
            val mb = new MetadataBuilder()

            // The proper onehotEncoding
            val indexerModel = new StringIndexerModel(labels).setInputCol(column).setOutputCol(column + "_index").setHandleInvalid("skip")
            val df_indexed = indexerModel.transform(tmpdata).na.drop( Seq(column+"_index") ) // ALso fixed in stringIndexer
            val encoder = if( state.binary) { new BinaryOneHotEncoder()} else {new OneHotEncoder()}
            encoder.setInputCol(column + "_index").setOutputCol(column + "_vect")
            val df_encoded = encoder.transform(df_indexed)

            // Create metadata
            val n_cols_filled = labels.size
            val n_cols = df_encoded.select(column + "_vect").first().get(0).asInstanceOf[SparseVector].size
            n_onehot += (column -> n_cols)
            val mb_filled = mb.withMetadata(metadata_to_propagate)
              .putString("onehot_from", column)
              .putDouble("onehot_dimensions", n_cols_filled)
              .putString("onehot_labels", labels.mkString(","))
              .build()

            // We want one column per label, not a vector
            val rdd_expanded = df_encoded.map { x => Row.fromSeq(x.toSeq ++ x.getAs[SparseVector](column + "_vect").toArray) }
            val schema_extra_columns = StructType(1 to n_cols map (x => (StructField(column + "_hot_" + x, DoubleType, false, mb_filled))))
            val schema_expanded = StructType(df_encoded.schema ++ schema_extra_columns)
            // Recreate DF with all the columns
            val df = sqlcontext.createDataFrame(rdd_expanded, schema_expanded)
              .drop(column)
              .drop(column + "_index")
              .drop(column + "_vect")

            tmpdata = df
            tmpdata.take(10).foreach(l => log.file(l.mkString(", ")))
          } catch {
            case e: Exception => {
              log.file("Error:" + e.getStackTraceString + "\n" + e.getMessage())
              rslt = <result> Error: { e.getMessage() } </result>
              tmpdata
            }
          }
      })

      log.file( "Total output columns: " + dw.data.count  )
      
      newSchema = tmpdata.schema
      val schemaAsXMLString = SchemaTools.schemaToXMLString(newSchema)
      rslt = <result> <params> <labels> { state.labels.map(x => x._1 + "->" + x._2.mkString(",")).toString() } </labels> <dimensions> { n_onehot.map(x => x._1 + "->" + x._2).toString() } </dimensions> </params> </result>
      log.file("SCHEMA after ONEHOT: " + schemaAsXMLString)
      tmpdata.unpersist
    }.addToMetaData(state.toXML)
  }
}

class OneHotTransform extends DataTransformer[onehot] {

  override def configure(conf: scala.xml.NodeSeq): this.type = {
    state = TransformerState.fromXML[onehot](conf.asInstanceOf[scala.xml.Node])
    state.columns = state.columns.map{ _.toLowerCase() }
    this
  }
  
  override def fit(dw: DataWrapper): OneHotTransformModel = {
    val sqlcontext = com.modelicious.in.app.Application.sqlContext
    var tmpdata = dw.data
    tmpdata.cache()
    val indexers: ArrayBuffer[StringIndexer] = ArrayBuffer()
    val labels: ArrayBuffer[(String, Array[String])] = ArrayBuffer()
	
	  val columnsList = if (state.columns(0)!="all") state.columns else dw.data.columns
	  log.file( "Apply hot encoding to: " + columnsList.toList.toString() )
    
    for (columna <- columnsList) {
      try{
        val si = new StringIndexer().setInputCol(columna).setOutputCol(columna + "_index").setHandleInvalid("skip").fit( tmpdata )
        labels += (columna -> si.labels)
        log.file( "Labels for " + columna + " " + si.labels.mkString( "{",",","}" ) )
      } catch { 
        case e: Exception => {
           log.file( "Error reading different values when accessing column: " + columna+" . Check if exists")
        }
      }
    }

    tmpdata.unpersist
    state.labels = labels.toArray.toMap
    new OneHotTransformModel(state)
  }

}

