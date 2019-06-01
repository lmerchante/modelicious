package com.modelicious.in.transform

import com.modelicious.in.app.Application
import com.modelicious.in.Constants
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.SchemaTools

import org.apache.log4j.Logger
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.{ StandardScaler, StandardScalerModel }
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, Column }
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType, IntegerType }
import org.apache.spark.sql.types.MetadataBuilder

import org.apache.spark.SparkException

import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls

case class standarize() extends TransformerState {
  var withMean: Boolean = true
  var mean: Vector = _
  var withSTD: Boolean = true
  var std: Vector = _
}

class StandarTransformModel(theState: TransformerState) extends DataTransformerModel[standarize](theState) {

  def doTransform(dw: DataWrapper): DataWrapper = {
    withRDDAndVaryingSchema(dw) { (rdd, schema) =>

      val model = new StandardScalerModel(state.std, state.mean, state.withSTD, state.withMean)
      val rdd_standarized = rdd.map(p => LabeledPoint( p.label, model.transform(p.features) ) )

      rslt = StandarTransformModel.createResult(state)

      val newSchema = StandarTransformModel.getNewSchema(schema, state)
      log.file("SCHEMA after standarization: " + newSchema.map(x => "(" + x.name + "," + x.dataType + "," + x.nullable + "," + x.metadata + ")").mkString(";"))
      (rdd_standarized, newSchema)
    }
      .addToMetaData(state.toXML)
  }

}

class StandarTransform extends DataTransformer[standarize] {

  override def fit(dw: DataWrapper): StandarTransformModel = {
    val rdd = SchemaTools.DFtoLabeledRDD(dw.data)
    val size = dw.data.columns.size - 1 // Eliminamos el label
    val scaler = new StandardScaler(withMean = state.withMean, withStd = state.withSTD).fit(rdd.map(x => x.features))
    if (state.withMean) state.mean = scaler.mean else state.mean = Vectors.dense(Array.fill(size) { 0.0 })
    if (state.withSTD) state.std = scaler.std else state.std = Vectors.dense(Array.fill(size) { 0.0 })

    new StandarTransformModel(state)
  }

}

object StandarTransformModel {
  private[StandarTransformModel] def getNewSchema(oldSchema: StructType, theState: standarize): StructType = {
    val columns = oldSchema.map(_.name).filter(_ != "label")

    val zipMeanWithVector = if (theState.withMean) { columns.zip(theState.mean.toArray).toMap } else Map[String, Double]()
    val zipStdWithVector = if (theState.withSTD) { columns.zip(theState.std.toArray).toMap } else Map[String, Double]()

    val metadata_to_propagate = oldSchema.map(x => (x.name, x.dataType, x.nullable, x.metadata))

    StructType(metadata_to_propagate.map { x =>
      if (x._1 != "label") {
        val mb = new MetadataBuilder()
        var mb_filled = mb.withMetadata(x._4)
        if (theState.withMean) {
          mb_filled = mb_filled.putDouble("standarize_with_mean:", zipMeanWithVector(x._1))
        }
        if (theState.withSTD) {
          mb_filled = mb_filled.putDouble("standarize_with_std", zipStdWithVector(x._1))
        }
        val mb_def = mb_filled.build()
        StructField(x._1, x._2, x._3, mb_def)
      } else {
        StructField(x._1, x._2, x._3, x._4)
      }
    })
  }

  private[StandarTransformModel] def createResult(theState: standarize): scala.xml.Node = {
    <standarize>
      <params>
        <mean>
          { if (theState.withMean) theState.mean.toArray.toList.toString else "No mean standarization" }
        </mean>
        <std>
          { if (theState.withSTD) theState.std.toArray.toList.toString else "No std standarization" }
        </std>
      </params>
    </standarize>
  }
}

