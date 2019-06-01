/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.feature

import java.lang.{Double => JDouble, Integer => JInt}

import java.util.{Map => JMap, NoSuchElementException}
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, VectorUDT}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.sql.types.SQLUserDefinedType
import scala.Iterator
import scala.reflect.runtime.universe



/**
 * :: Experimental ::
 * Transform categorical features to use 0-based indices instead of their original values.
 *  - Categorical features are mapped to indices.
 *  - Continuous features (columns) are left unchanged.
 * This also appends metadata to the output column, marking features as Numeric (continuous),
 * Nominal (categorical), or Binary (either continuous or categorical).
 * Non-ML metadata is not carried over from the input to the output column.
 *
 * This maintains vector sparsity.
 *
 * @param numFeatures  Number of features, i.e., length of Vectors which this transforms
 * @param categoryMaps  Feature value index.  Keys are categorical feature indices (column indices).
 *                      Values are maps from original features values to 0-based category indices.
 *                      If a feature is not in this map, it is treated as continuous.
 */
@Experimental
class BVectorIndexerModel  (
    override val uid: String,
    override val numFeatures: Int,
    override val categoryMaps: Map[Int, Map[Double, Int]])
  extends VectorIndexerModel(uid, numFeatures, categoryMaps) {

  import VectorIndexerModel._

  private val SKIP_INVALID: String = "skip"
  private val ERROR_INVALID: String = "error"
  private val KEEP_INVALID: String = "keep"
  
  // Force to SKIP
  def getHandleInvalid = SKIP_INVALID

  /**
   * Pre-computed feature attributes, with some missing info.
   * In transform(), set attribute name and other info, if available.
   */
  private val partialFeatureAttributes: Array[Attribute] = {
    val attrs = new Array[Attribute](numFeatures)
    var categoricalFeatureCount = 0 // validity check for numFeatures, categoryMaps
    var featureIndex = 0
    while (featureIndex < numFeatures) {
      if (categoryMaps.contains(featureIndex)) {
        // categorical feature
         // categorical feature
      val rawFeatureValues: Array[String] =
            categoryMaps(featureIndex).toArray.sortBy(_._1).map(_._1).map(_.toString)
 
         val featureValues = if (getHandleInvalid == KEEP_INVALID) {
           (rawFeatureValues.toList :+ "__unknown").toArray
         } else {
           rawFeatureValues
         }
         if (featureValues.length == 2 && getHandleInvalid != KEEP_INVALID) {
          attrs(featureIndex) = new BinaryAttribute(index = Some(featureIndex),
            values = Some(featureValues))
        } else {
          attrs(featureIndex) = new NominalAttribute(index = Some(featureIndex),
            isOrdinal = Some(false), values = Some(featureValues))
        }
        categoricalFeatureCount += 1
      } else {
        // continuous feature
        attrs(featureIndex) = new NumericAttribute(index = Some(featureIndex))
      }
      featureIndex += 1
    }
    require(categoricalFeatureCount == categoryMaps.size, "BVectorIndexerModel given categoryMaps" +
      s" with keys outside expected range [0,...,numFeatures), where numFeatures=$numFeatures")
    attrs
  }

  // TODO: Check more carefully about whether this whole class will be included in a closure.

  /** Per-vector transform function */
  private lazy val transformFunc: Vector => Vector = {
    val sortedCatFeatureIndices = categoryMaps.keys.toArray.sorted
    val localVectorMap = categoryMaps
    val localNumFeatures = numFeatures
    val localHandleInvalid = getHandleInvalid
    val f: Vector => Vector = { (v: Vector) =>
      assert(v.size == localNumFeatures, "BVectorIndexerModel expected vector of length" +
        s" $numFeatures but found length ${v.size}")
      v match {
        case dv: DenseVector =>
          var hasInvalid = false
          val tmpv = dv.copy
          localVectorMap.foreach { case (featureIndex: Int, categoryMap: Map[Double, Int]) =>
             try {
                           tmpv.values(featureIndex) = categoryMap(tmpv(featureIndex))
             } catch {
               case _: NoSuchElementException =>
                 localHandleInvalid match {
                   case ERROR_INVALID =>
                     throw new SparkException(s"BVectorIndexer encountered invalid value " +
                       s"${tmpv(featureIndex)} on feature index ${featureIndex}. To handle " +
                       s"or skip invalid value, try setting handleInvalid.")
                   case KEEP_INVALID =>
                     tmpv.values(featureIndex) = categoryMap.size
                   case SKIP_INVALID => {
                     logWarning((s"BVectorIndexer encountered invalid value " +
                       s"${tmpv(featureIndex)} on feature index ${featureIndex}. Skipping row, if this warning appears a lot " +
                       " consider training again your model." 
                       ))
                     hasInvalid = true
                   }
                 }
             }
            }
          if (hasInvalid) null else tmpv
          
        case sv: SparseVector =>
          // We use the fact that categorical value 0 is always mapped to index 0.
          var hasInvalid = false
          val tmpv = sv.copy
          var catFeatureIdx = 0 // index into sortedCatFeatureIndices
          var k = 0 // index into non-zero elements of sparse vector
          while (catFeatureIdx < sortedCatFeatureIndices.length && k < tmpv.indices.length) {
            val featureIndex = sortedCatFeatureIndices(catFeatureIdx)
            if (featureIndex < tmpv.indices(k)) {
              catFeatureIdx += 1
            } else if (featureIndex > tmpv.indices(k)) {
              k += 1
            } else {
             try {
                tmpv.values(k) = localVectorMap(featureIndex)(tmpv.values(k))
              } catch {
                case _: NoSuchElementException =>
                  localHandleInvalid match {
                    case ERROR_INVALID =>
                      throw new SparkException(s"BVectorIndexer encountered invalid value " +
                        s"${tmpv.values(k)} on feature index ${featureIndex}. To handle " +
                        s"or skip invalid value, try setting handleInvalid.")
                    case KEEP_INVALID =>
                      tmpv.values(k) = localVectorMap(featureIndex).size
                    case SKIP_INVALID => {  
                      logWarning((s"BVectorIndexer encountered invalid value " +
                       s"${tmpv(featureIndex)} on feature index ${featureIndex}. Skipping row, if this warning appears a lot " +
                       " consider training again your model." ))
                     hasInvalid = true
                      }
                  }
              }
              catFeatureIdx += 1
              k += 1
            }
          }
          if (hasInvalid) null else tmpv
      }
    }
    f
  }
  
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val newField = prepOutputField(dataset.schema)
    val transformUDF = udf { (vector: Vector) => transformFunc(vector) }
    val newCol = transformUDF(dataset($(inputCol)))
    val ds = dataset.withColumn($(outputCol), newCol, newField.metadata)
    if (getHandleInvalid == SKIP_INVALID) {
      ds.na.drop(Array($(outputCol)))
    } else {
      ds
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    val dataType = new VectorUDT
    require(isDefined(inputCol),
      s"BVectorIndexerModel requires input column parameter: $inputCol")
    require(isDefined(outputCol),
      s"BVectorIndexerModel requires output column parameter: $outputCol")
    SchemaUtils.checkColumnType(schema, $(inputCol), dataType)

    // If the input metadata specifies numFeatures, compare with expected numFeatures.
    val origAttrGroup = AttributeGroup.fromStructField(schema($(inputCol)))
    val origNumFeatures: Option[Int] = if (origAttrGroup.attributes.nonEmpty) {
      Some(origAttrGroup.attributes.get.length)
    } else {
      origAttrGroup.numAttributes
    }
    require(origNumFeatures.forall(_ == numFeatures), "BVectorIndexerModel expected" +
      s" $numFeatures features, but input column ${$(inputCol)} had metadata specifying" +
      s" ${origAttrGroup.numAttributes.get} features.")

    val newField = prepOutputField(schema)
    val outputFields = schema.fields :+ newField
    StructType(outputFields)
  }

  /**
   * Prepare the output column field, including per-feature metadata.
   * @param schema  Input schema
   * @return  Output column field.  This field does not contain non-ML metadata.
   */
  private def prepOutputField(schema: StructType): StructField = {
    val origAttrGroup = AttributeGroup.fromStructField(schema($(inputCol)))
    val featureAttributes: Array[Attribute] = if (origAttrGroup.attributes.nonEmpty) {
      // Convert original attributes to modified attributes
      val origAttrs: Array[Attribute] = origAttrGroup.attributes.get
      origAttrs.zip(partialFeatureAttributes).map {
        case (origAttr: Attribute, featAttr: BinaryAttribute) =>
          if (origAttr.name.nonEmpty) {
            featAttr.withName(origAttr.name.get)
          } else {
            featAttr
          }
        case (origAttr: Attribute, featAttr: NominalAttribute) =>
          if (origAttr.name.nonEmpty) {
            featAttr.withName(origAttr.name.get)
          } else {
            featAttr
          }
        case (origAttr: Attribute, featAttr: NumericAttribute) =>
          origAttr.withIndex(featAttr.index.get)
        case (origAttr: Attribute, _) =>
          origAttr
      }
    } else {
      partialFeatureAttributes
    }
    val newAttributeGroup = new AttributeGroup($(outputCol), featureAttributes)
    newAttributeGroup.toStructField()
  }

  def this(vim: VectorIndexerModel) = {
    this( vim.uid, vim.numFeatures, vim.categoryMaps)
    setParent(vim.parent)
    for ( p <- vim.extractParamMap.toSeq ) {
      set(p)
    }
  }
  
}

@Since("1.6.0")
object BVectorIndexerModel extends MLReadable[BVectorIndexerModel] {

  private[BVectorIndexerModel]
  class VectorIndexerModelWriter(instance: BVectorIndexerModel) extends MLWriter {

    private case class Data(numFeatures: Int, categoryMaps: Map[Int, Map[Double, Int]])
    
    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.numFeatures, instance.categoryMaps)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class VectorIndexerModelReader extends MLReader[BVectorIndexerModel] {

    private val className = classOf[BVectorIndexerModel].getName

    override def load(path: String): BVectorIndexerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
        .select("numFeatures", "categoryMaps")
        .head()
      val numFeatures = data.getAs[Int](0)
      val categoryMaps = data.getAs[Map[Int, Map[Double, Int]]](1)
      val model = new BVectorIndexerModel(metadata.uid, numFeatures, categoryMaps)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[BVectorIndexerModel] = new VectorIndexerModelReader

  @Since("1.6.0")
  override def load(path: String): BVectorIndexerModel = super.load(path)
}
