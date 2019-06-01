package com.modelicious.in.modelos.predictor

import org.apache.spark.ml.feature.{ BQuantileDiscretizer => QuantileDiscretizer, Bucketizer }
import org.apache.spark.mllib.classification.{ NaiveBayes => SparkNaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import org.apache.spark.sql.{ DataFrame, Row, Column }
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType, IntegerType }

import java.{ util => ju }

import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.{SchemaTools, FileUtils}

import scala.language.reflectiveCalls

import scala.reflect.runtime.{ universe => ru }

// Probably this should be a class and a transform by its own
class NBDiscretizer( var quantizer: Map[String, Bucketizer] ) extends Serializable{
  
  def this() = this( Map[String, Bucketizer]() )
   // Bayes require non-negative feature values
  def discretize(data: RDD[LabeledPoint] ): RDD[LabeledPoint] = {
     val sqlContext = com.modelicious.in.app.Application.sqlContext
    // discretiza los datos de entrada por columnas
    val rddRow = data.map(x => Row.fromSeq(Array(x.label) ++ x.features.toArray))
    val schema = StructType((1 to rddRow.first.size).map(i => StructField(s"val_$i", DoubleType, false)))
    var df = sqlContext.createDataFrame(rddRow, schema)
    val columns = df.columns
    for (i <- columns.tail) { // not discretize label
      var discretizer = new QuantileDiscretizer()
        .setInputCol(i)
        .setOutputCol(i + "_cat")
        .setNumBuckets(127)
      val bucket = discretizer.fit(df)
      quantizer = quantizer + (i -> bucket)
      val result = bucket.transform(df)
      df = result.drop(i)
    }
    val rdd_quantified = df.rdd.map { x =>
      val label = x.toSeq(0).asInstanceOf[Number].doubleValue()
      val vector = Vectors.dense(x.toSeq.drop(1).toArray.map(_.asInstanceOf[Number].doubleValue()))
      LabeledPoint(label, vector)
    }
    rdd_quantified
  }

  def binarySearchForBuckets(splits: Array[Double], feature: Double): Double = {
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

}

class NaiveBayesBase extends ModeloMLLIB  {

  type T = NaiveBayesModel
  val tag = ru.typeTag[NaiveBayesModel]
  val name = "NaiveBayesBase"

  var discretizer = new NBDiscretizer() 
  
  def doTrain(input: RDD[LabeledPoint], vector_schema: StructType, conf: scala.xml.Elem ) = {
    val lambda = (conf \\ "lambda").textAsDoubleOrElse(1.0)
    val modelType = (conf \\ "modelType").textOrElse("multinomial")
    val discretized_data = discretizer.discretize(input)
    modelo = Some(SparkNaiveBayes.train(discretized_data, lambda, modelType))
  }

  def configuration(conf: scala.xml.Node): String = {
    val lambda = (conf \\ "lambda").textAsDoubleOrElse(1.0)
    val modelType = (conf \\ "modelType").textOrElse("multinomial")
    return "lambda: " + lambda + "; modelType: " + modelType
  }
  
  override def doSave( path: String): Unit = {
    val sc = com.modelicious.in.app.Application.sc
    modelo.getOrElse(throw new RuntimeException("Train your model before trying to save it!") ).save(sc, path)
    save_quantizers( path+ "/quantizers", discretizer.quantizer )
  }
  
  // Wrap in a some and not a Try because it MUST throw if the load fails.
  override def loadModel( path: String ) = { 
    modelo = Some(new Loader[T]().load( path )) 
    val quantizers = load_quantizers( path+ "/quantizers" )  
    log.file( "Quantizers = " + quantizers.keys.mkString(",") )
    discretizer = new NBDiscretizer( quantizers )  
  }
  
  
  def load_quantizers( path: String ) : Map[String, Bucketizer] = {
    
    val files = FileUtils.list_files( path )
    log.file( "Quantizer files = " + files.mkString(",") )
    
    files.map { name =>  
      val splits = FileUtils.readFileToString(path + "/" + name).split(',').map { _.toDouble }
      val bucket = new Bucketizer
      bucket.setSplits(splits)
      ( name, bucket )
    }.toMap
  }
  
  def save_quantizers( path: String, quantizers: Map[String, Bucketizer]) = {
    quantizers.foreach{ case ( key, bucket ) =>
      FileUtils.writeStringToFile( path + "/" + key, bucket.getSplits.mkString(",") )  
    }
  }
  
}
