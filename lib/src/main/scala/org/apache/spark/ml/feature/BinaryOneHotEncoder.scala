package org.apache.spark.ml.feature

import org.apache.spark.annotation.{Since, Experimental}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * :: Experimental ::
 * A one-hot encoder that maps a column of category indices to a column of binary vectors, with
 * at most a single one-value per row that indicates the input category index.
 * For example with 5 categories, an input value of 2.0 would map to an output vector of
 * `[0.0, 0.0, 1.0, 0.0]`.
 * The last category is not included by default (configurable via [[BinaryOneHotEncoder!.dropLast]]
 * because it makes the vector entries sum up to one, and hence linearly dependent.
 * So an input value of 4.0 maps to `[0.0, 0.0, 0.0, 0.0]`.
 * Note that this is different from scikit-learn's OneHotEncoder, which keeps all categories.
 * The output vectors are sparse.
 *
 * @see [[StringIndexer]] for converting categorical values into category indices
 */
@Experimental
class BinaryOneHotEncoder(override val uid: String) extends OneHotEncoder {

  def this() = this(Identifiable.randomUID("binaryOneHot"))

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)

    SchemaUtils.checkColumnType(schema, inputColName, DoubleType)
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == outputColName),
      s"Output column $outputColName already exists.")

    val inputAttr = Attribute.fromStructField(schema(inputColName))
    val outputAttrNames: Option[Array[String]] = inputAttr match {
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          nominal.values
        } else if (nominal.numValues.isDefined) {
          nominal.numValues.map(n => Array.tabulate(n)(_.toString))
        } else {
          None
        }
      case binary: BinaryAttribute =>
        if (binary.values.isDefined) {
          binary.values
        } else {
          Some(Array.tabulate(2)(_.toString))
        }
      case _: NumericAttribute =>
        throw new RuntimeException(
          s"The input column $inputColName cannot be numeric.")
      case _ =>
        None // optimistic about unknown attributes
    }

    val filteredOutputAttrNames = outputAttrNames.map { names =>
      if ($(dropLast)) {
        require(names.length > 1,
          s"The input column $inputColName should have at least two distinct values.")
        names.dropRight(1)
      } else {
        names
      }
    }

    val outputAttrGroup = if (filteredOutputAttrNames.isDefined) {
      val attrs: Array[Attribute] = filteredOutputAttrNames.get.map { name =>
        BinaryAttribute.defaultAttr.withName(name)
      }
      new AttributeGroup($(outputCol), attrs)
    } else {
      new AttributeGroup($(outputCol))
    }

    val outputFields = inputFields :+ outputAttrGroup.toStructField()
    StructType(outputFields)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    // schema transformation
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)
    val shouldDropLast = false // No sabemos cuando puede hacer que esto falle, por ej, con 17 labels, la 17 sería el 16 -> 16 = 10000.
    // Si quitamos la ultima columna, sería 0000, que sería la etiqueta de la 1a label
    var outputAttrGroup = AttributeGroup.fromStructField(
      transformSchema(dataset.schema)(outputColName))
    if (outputAttrGroup.size < 0) {
      // If the number of attributes is unknown, we check the values from the input column.
      val size = dataset.select(col(inputColName).cast(DoubleType)).map(_.getDouble(0))
        .aggregate(0.0)(
          (m, x) => {
            assert(x >=0.0 && x == x.toInt,
              s"Values from column $inputColName must be indices, but got $x.")
            math.max(m, x)
          },
          (m0, m1) => {
            math.max(m0, m1)
          }
        ).toInt
      val numAttrs = size // Integer.toBinaryString(size).length()
      val outputAttrNames = Array.tabulate(numAttrs)(_.toString)
      val outputAttrs: Array[Attribute] =
        outputAttrNames.map(name => BinaryAttribute.defaultAttr.withName(name))
      outputAttrGroup = new AttributeGroup(outputColName, outputAttrs)
    }
    val metadata = outputAttrGroup.toMetadata()

    // data transformation
    val size = Integer.toBinaryString(outputAttrGroup.size).size
    
    println( "size: " + size )  
    //val oneValue = Array(1.0)
    //val emptyValues = Array[Double]()
    //val emptyIndices = Array[Int]()
    val encode = udf { label: Double =>
        Vectors.sparse(size, Integer.toBinaryString(label.toInt).reverse.toArray.zipWithIndex.filter( _._1 == '1' ).map{ case (v,i) => (i, 1.0) })
    }

    dataset.select(col("*"), encode(col(inputColName).cast(DoubleType)).as(outputColName, metadata))
  }

  override def copy(extra: ParamMap): BinaryOneHotEncoder = defaultCopy(extra)
}

@Since("1.6.0")
object BinaryOneHotEncoder extends DefaultParamsReadable[BinaryOneHotEncoder] {

  @Since("1.6.0")
  override def load(path: String): BinaryOneHotEncoder = super.load(path)
}