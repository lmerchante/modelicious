package com.modelicious.in.transform

import com.modelicious.in.app.Application
import com.modelicious.in.Constants
import com.modelicious.in.data.{DataWrapper, DataFrameExtender}
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.SchemaTools

import org.apache.log4j.Logger
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, Column }
import org.apache.spark.sql.functions.{col, countDistinct }
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType, IntegerType, MetadataBuilder }
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.ml.feature.{ BQuantileDiscretizer => QuantileDiscretizer, Bucketizer }
import org.apache.spark.SparkException

import java.{ util => ju }

import scala.collection.mutable.ArrayBuffer
import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls

case class discretize() extends TransformerState {
  var limit: Int = 127
  var nCategories: Int = 127
  var exclude: List[String] = List()
  var columns: List[String] = List("all")
  var splits: Splits = Map()
  // Estas variables solo se usan para el metadata. pero está bien tenerlas para comprobar problemas
  var oldCounts: Map[String, Long] = Map()
  var modifiedCounts: Map[String, Long] = Map()

}

class DiscretizeTransformModel(theState: TransformerState) extends DataTransformerModel[discretize](theState) {

  def doTransform(data: DataWrapper): DataWrapper = {
    var oldSchema: StructType = data.schema

    log.file(s"Discretizacion de todas las variables con mas de: " + state.limit + " valores diferentes.")

    val finalCounts = state.oldCounts++state.modifiedCounts
    val newSchema = DiscretizeTransform.add_quantizer_metadata(finalCounts, state.splits, oldSchema)

    rslt = <discretizacion>
             <antes> { SchemaTools.schemaToXML(DiscretizeTransform.add_count(state.oldCounts, oldSchema)) }</antes>
             <despues>{ SchemaTools.schemaToXML(newSchema) }</despues>
           </discretizacion>
    
    data { df => discretiza(df).updateWithMetaData(newSchema) } // Esto es para la columna del schema      
      .addToMetaData(state.toXML) // Esto para el metadata del wrapper -
  }

  // PipelineModel es privado, aqui no podemos crear un pipeline que haga todo seguido
  protected def discretiza(data: DataFrame): DataFrame = {

    val columns_org = data.columns
    val bucketizers: ArrayBuffer[Bucketizer] = ArrayBuffer()

    var transformed = data
    
    var columns_to_discretize = state.splits.map(_._1)
    
    for (i <- columns_to_discretize) {
      log.file(s"Discretize column: " + i)
      
      val column_splits = state.splits(i)
      Application.sc.broadcast( column_splits )
      
      val bucketizer = new Bucketizer()
        .setInputCol(i)
        .setOutputCol(i + "_cat")
        .setSplits( column_splits )
      transformed = bucketizer.transform(transformed).drop(i).withColumnRenamed(i + "_cat", i)
    }

    //transformed.columns.filter(_.endsWith("_cat")).map(_.dropRight(4)).foreach { c => transformed = transformed.drop(c).withColumnRenamed(c + "_cat", c) }
    transformed.select(columns_org.head, columns_org.tail: _*)
  }

}

class DiscretizeTransform extends DataTransformer[discretize] {

  override def configure(conf: scala.xml.NodeSeq): this.type = {
    state = TransformerState.fromXML[discretize](conf.asInstanceOf[scala.xml.Node])
    if (state.nCategories > 127) {
      log.file(s"El numero de categorías seleccionadas supera el máximo de categorias a discretizar (127)")
      state.nCategories = 127
    }

    this
  }

  override def fit(data: DataWrapper): DiscretizeTransformModel = {
    log.file(s"Se discretiza con : " + state.nCategories + " categorias.")
    log.file(s"SCHEMA original before DISCRETIZATION: " + data.schema.toString())

    data.cache
    state.splits = fit_discretizer(data.data).map(x => x._1 -> x._2.getSplits)
    data.unpersist
    
    log.file(s"LIST of splits used in DISCRETIZACION: " + state.splits.map(x => Map(x._1 -> x._2.toList.toString())).toList.toString())

    new DiscretizeTransformModel(state)
  }

  def fit_discretizer(data: DataFrame): Map[String, Bucketizer] = {

    val schema = data.schema
    val discretizers: ArrayBuffer[QuantileDiscretizer] = ArrayBuffer()

    val oldCounts = scala.collection.mutable.Map[String, Long]()
    val modifiedCounts = scala.collection.mutable.Map[String, Long]()
    
    val columns_to_discretize = if (state.columns==List("all")) data.columns.filter(_ != "label").filter( !state.exclude.contains(_) ) else state.columns.toArray.filter(_ != "label").filter( !state.exclude.contains(_) )
    log.file( "Columns to discretize: " + columns_to_discretize.mkString("[",",","]") )
    val counts = data.select(columns_to_discretize.map(c => countDistinct(col(c)).alias(c)): _*).first
    
    
    for ((i,pos) <- columns_to_discretize.zipWithIndex) {
      val different_values = counts.getLong(pos)
      oldCounts += (i -> different_values)
      log.file(s"Column: " + i + " takes number of different values: " + different_values.toString())
      if (different_values > state.limit && !state.exclude.contains(i)) {
        log.file(s"Discretize column: " + i)
        val discretizer = new QuantileDiscretizer()
          .setInputCol(i)
          .setOutputCol(i + "_cat")
          .setNumBuckets(state.nCategories)
        discretizers += discretizer
      }
    }
   
    val pipeline = new Pipeline().setStages(discretizers.toArray)
    val buckets = pipeline.fit(data)

    val splits = buckets.stages.map({ b =>
      val h = b.asInstanceOf[Bucketizer]
      modifiedCounts += (h.getInputCol -> (h.getSplits.length - 1)) // Esto es un test, pero debería ser lo mismo
      (h.getInputCol -> h)
    }).toMap // save splits

    state.oldCounts = oldCounts.toMap
    state.modifiedCounts = modifiedCounts.toMap
    
    splits
  }

}

object DiscretizeTransform {
  def binarySearchForBuckets(splits: Array[Double], feature: Double): Double = {
    // to be used with PREDICT
    //  for (i <- 1 to testvector.size) {
    //    output_vector = output_vector :+ discretizer.binarySearchForBuckets(discretizer.quantizer(s"val_${i+1}").getSplits, testvector(i - 1))
    //  }
    if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = ju.Arrays.binarySearch(splits, feature)
      if (idx >= 0) {
        idx
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == splits.length) {
          throw new SparkException(s"Feature value $feature out of Bucketizer bounds" +
            s" [${splits.head}, ${splits.last}].  Check your features, or loosen " +
            s"the lower/upper bound constraints.")
        } else {
          insertPos - 1
        }
      }
    }
  }

  def add_count(myMap: Map[String, Long], schema: StructType): StructType = {
    val myNewSchema = schema.filter( f => myMap.contains(f.name) )
    .map { f =>
      val mb = new MetadataBuilder()
      new StructField(name = f.name, dataType = f.dataType, nullable = f.nullable, metadata = mb.withMetadata(f.metadata).putLong("dif_values", myMap(f.name)).build())
    } 
    new StructType(myNewSchema.toArray)
  }

  def add_buckets(myMap: Map[String, Bucketizer], schema: StructType): StructType = {
    val myNewSchema = schema.filter( f => myMap.contains(f.name) )
    .map { f =>
      val mb = new MetadataBuilder()
      new StructField(name = f.name, dataType = f.dataType, nullable = f.nullable, metadata = mb.withMetadata(f.metadata).putString("buckets", myMap(f.name).getSplits.toList.toString()).build())
    }
    new StructType(myNewSchema.toArray)
  }

  def add_quantizer_metadata(counts_map: Map[String, Long], buckets_map: Splits, schema: StructType): StructType = {

    // Fase 1= Crear un mapa con ambos valores 
    val merged_map: scala.collection.mutable.Map[String, (Option[Long], Option[Array[Double]])] = scala.collection.mutable.Map()

    counts_map.foreach { case (i, l) => merged_map(i) = (Some(l), None) }
    buckets_map.foreach {
      case (i, b) => merged_map.get(i) match {
        case a @ Some(_) => merged_map(i) = (a.get._1, Some(b)) // Sólo existe si lo hemos rellenado con el foreach anterior
        case None        => merged_map(i) = (None, Some(b)) // No existe, no lo hemos creado en el foreach anterior
      }
    }

    // Crear un metadata con ambos valores
    val myNewSchema = schema.filter( f => merged_map.contains(f.name) )
    .map { f =>
      val n = f.name
      
      val (l, b) = merged_map(n)
      val db = new MetadataBuilder() // builder for discretizer -- If Builder had a clean method we could reuse the same class....
      if (l != None) { db.putLong("dif_values", l.get) }
      if (b != None) { db.putString("buckets", b.get.toList.toString()) }
      // Crear metadata final
      val discretizer_metadata = db.build()
      val mb = new MetadataBuilder()
      new StructField(name = f.name, dataType = f.dataType, nullable = f.nullable,
        metadata = mb.withMetadata(f.metadata).putMetadata("discretizer", discretizer_metadata).build())
    }
    new StructType(myNewSchema.toArray)
  }
}

//// Discretizer with user provided splits -> Allow only one colum
//class CustomDiscretizeTransform extends DataTransformer[discretize] {
//
//  override def configure(conf: scala.xml.NodeSeq): this.type = {
//    state = TransformerState.fromXML[discretize](conf.asInstanceOf[scala.xml.Node])
//    if (state.nCategories > 127) {
//      log.file(s"El numero de categorías seleccionadas supera el máximo de categorias a discretizar (127)")
//      state.nCategories = 127
//    }
//
//    if( state.columns.size != 1  ) {
//      val msg = "El CustomDiscretizer de debe configurar para una sola columna"
//      log.file(msg)
//      throw new RuntimeException(msg)
//    }
//    
//    if( !state.splits.contains( state.columns(0) )  ) {
//      val msg = "Splits proporcionados no coinciden con la columna configurada"
//      log.file(msg)
//      throw new RuntimeException(msg)
//    }
//    
//    
//    
//    this
//  }
//
//  override def fit(data: DataWrapper): DiscretizeTransformModel = {
//    new DiscretizeTransformModel(state)
//  }
//
//}

