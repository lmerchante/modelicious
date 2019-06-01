package com.modelicious.in.modelos.predictor

import com.modelicious.in.modelos.Modelo
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Strategy
import com.modelicious.in.tools.Implicits._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, IntegerType, MetadataBuilder}
//import org.apache.spark.ml.util.MLWritable

import org.apache.spark.ml.feature.VectorIndexerModel
import org.apache.spark.ml.feature.BVectorIndexerModel

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.language.postfixOps
import scala.language.reflectiveCalls
import scala.util.Try
import scala.xml._

import org.apache.log4j.Logger

import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.FileUtils
import com.modelicious.in.tools.SchemaTools
import com.modelicious.in.tools.Implicits._

import scala.reflect.runtime.{ universe => ru }

/**
 * Base class for MLLIB and ML classifiers and scorers
 * 
 * @see  [[com.modelicious.in.modelos.predictor.classifier]]
 * @see  [[com.modelicious.in.modelos.predictor.scorers]]
*/

trait Predictor extends Modelo {
 
  def train( input: DataFrame, conf: scala.xml.Elem )
    
  def predict( testData: DataFrame ): RDD[(Double, Double)]
  
  def configuration(conf: scala.xml.Node):String

}


trait VectorIndexedModel {
  
  protected var _vim: Option[BVectorIndexerModel] = None 
  
  def setModelIndexer( vim: Option[BVectorIndexerModel] ) = { _vim = vim }
  def getModelIndexer( ): Option[BVectorIndexerModel] = _vim
  
}

object VectorIndexedModel {
  
  def load(path: String ) : Option[BVectorIndexerModel] = {
    val vim = scala.util.Try {
      val v = VectorIndexerModel.load(path + "/vim")
      new BVectorIndexerModel(v)
    }.toOption
    if( vim.isDefined) { return vim }
    else
    {
      val bvim = scala.util.Try {
      BVectorIndexerModel.load(path + "/vim")
      }.toOption
      return bvim
    }
  }
}
/**
 * Abstract class for all classifiers that underneath implement an MLLIB model.
 *  
 * The loadModel method is implemented through generics, so each child does not have to reimplement it again 
 */
trait ModeloMLLIB extends Predictor {  
  
  type T <: Saveable { def predict(testData: Vector) : Double;} 
  // http://stackoverflow.com/questions/20580513/no-classtag-available-for-myclass-this-t-for-an-abstract-type
  implicit def tag: ru.TypeTag[T]

  
  def predict( testData: DataFrame ) : RDD[(Double, Double)] = {
    val real_model=(modelo.getOrElse(throw new RuntimeException("Train you model before predict!")))
    
    checkColumns(testData)
    
    val my_RDD = SchemaTools.DFWithVectorToLabeledRDD(testData)
    
    my_RDD.map(p => (real_model.predict(p.features), p.label))
  }
  
  def train(input: DataFrame, conf: scala.xml.Elem ) = {
    this.conf = conf
    _columns = Some(input.schema("features").metadata.getMetadata("ml_attr").toString)
    val my_RDD = SchemaTools.DFWithVectorToLabeledRDD(input)
    // Have to remove "label" from schema to be consistent with Vector element from RDD[LabeledPoint]
    val vector_schema = input.drop("label").schema
    doTrain(my_RDD, vector_schema, conf )
  }
  
  def doTrain(input: RDD[LabeledPoint], schema: StructType, conf: scala.xml.Elem )
  
  def doSave( path: String): Unit = {
    val sc = com.modelicious.in.app.Application.sc
    modelo.getOrElse(throw new RuntimeException("Train your model before trying to save it!") ).save(sc, path)
  }
 
  /**
   * Helper class to implement load method for all the children. It uses generics and assumes all the models ( and this happen for 
   * MLLIB ) have a companion object with the same name and with a ***load(sc: SparkContext, path: String)*** method. 
   */
  class Loader[R : ru.TypeTag] {
    def load( path: String ) : R = {
      val sc = com.modelicious.in.app.Application.sc
      val m = ru.runtimeMirror(getClass.getClassLoader)
      println( "Model: " + ru.typeOf[R]  )
      val ts = ru.typeOf[R].typeSymbol
      val objectC = ts.companionSymbol.asModule
      val mm = m.reflectModule(objectC)
      // as we are working with generics, must define structural type to call load.
      val obj = mm.instance.asInstanceOf[ {def load(sc: SparkContext, path: String): R } ]
      obj.load(sc, path)
    }
  }
  
  // Wrap in a some and not a Try because it MUST throw if the load fails.
  def loadModel( path: String ) = { modelo = Some(new Loader[T]().load( path )) }
  
}

trait ModeloML extends Predictor {  
  
  type T <: Transformable
  
  def train(input: DataFrame, conf: scala.xml.Elem ) = {
    this.conf = conf
    val sqlContext = com.modelicious.in.app.Application.sqlContext
    //import sqlContext.implicits._
    // TODO: one pass DF(label, ....) to DF(label, features)
    //val newDF = SchemaTools.DFtoLabeledRDD(input).cache().toDF("label","features")
    _columns = Some(input.schema("features").metadata.getMetadata("ml_attr").toString)
    doTrain(input, conf)
  }
  
  protected def doTrain(input: DataFrame, conf: scala.xml.Elem )
      
  def predict( testData: DataFrame ) : RDD[(Double, Double)] = {
    val sqlContext = com.modelicious.in.app.Application.sqlContext
    import sqlContext.implicits._
    val real_model=(modelo.getOrElse(throw new RuntimeException("Train you model before predict!")))
    
    checkColumns(testData)
    
    // TODO: one pass DF(label, ....) to DF(label, features)
    //val my_RDD = SchemaTools.DFtoLabeledRDD(testData).cache()
    
    log.file("Applying prediction")
    val tr = real_model.transform(testData)
    
    tr.show()
    
    getPrediction( tr )
  }
  
  // Clases ML extienden MLWritable a partir de 1.6 y tienen metodo save( path: String )
  // Hasta entonces nada
  def doSave( path: String ) = {  }
    
  def getPrediction: ( DataFrame ) => RDD[(Double, Double)]
  
 

}

// Hack for the Perceptron, that does not have save at Spark 1.6.* version. Trying to have it at the lowest level possible, it is at Classifier and not at predictor 
trait SaveableModeloML extends ModeloML {
   type T <: Saveable with Transformable
   
   override def doSave(path: String): Unit = {
     val sc = com.modelicious.in.app.Application.sc
     modelo.getOrElse(throw new RuntimeException("Train your model before trying to save it!") ).save(sc, path)
   }
}

