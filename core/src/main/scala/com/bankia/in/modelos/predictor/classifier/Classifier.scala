package com.modelicious.in.modelos.predictor.classifier

import com.modelicious.in.modelos.OModel

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, IntegerType, MetadataBuilder}
//import org.apache.spark.ml.util.MLWritable

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.language.postfixOps
import scala.language.reflectiveCalls
import scala.util.Try
import scala.xml._

import org.apache.log4j.Logger

import com.modelicious.in.data.DataWrapper
import com.modelicious.in.modelos.predictor._
import com.modelicious.in.tools.FileUtils
import com.modelicious.in.tools.SchemaTools
import com.modelicious.in.tools.Implicits._

import scala.reflect.runtime.{ universe => ru }
 
/**
 * Base class for all classifiers 
 * For a classifier to be instantiable, it must implement the following methods:
 * 
 * 		- train
 * 		- predict
 * 		- configuration 
 * 
 * @todo With a bit of reflection like the used in transformer, the configurarion method could be generic for all children
 * 
 */

trait Classifier extends Predictor 


/**
 * Instance method that allows to create Classifiers. It can be a new one with the factory method implemented through apply 
 * or loading a previous saved one through load.
 */
object Classifier extends OModel[Classifier]


// As this sets a numClasses, I put it under Classifier, if we find any model that has Strategy but can be a scorer, move up to Predictor level
trait Strategist extends ModeloMLLIB with Classifier{
  
  // TODO: To be completed
  //  Would like to do it dynamicaly, but it is not recommended way (But possible 

  import scala.language.implicitConversions
  implicit def makeStrategy(conf: scala.xml.Node) : Strategy = {
    import org.apache.spark.mllib.tree.configuration.Algo
    import org.apache.spark.mllib.tree.configuration.Strategy
    import org.apache.spark.mllib.tree.impurity.Gini
    
    val algo = (conf \\ "algo").textOrElse("Classification")
    val st = Strategy.defaultStrategy(algo)
    for ( c <- conf.child )  c.label match {
      case "numClasses" => st.setNumClasses( c.text.toInt )
      case "maxBins" => st.setMaxBins(c.text.toInt)
      case "maxDepth" => st.setMaxDepth(c.text.toInt)
      case "impurity" => if (c.text=="gini") st.setImpurity(Gini) else ()
      case "useNodeIdCache" => if (c.text=="true") st.setUseNodeIdCache(true) else st.setUseNodeIdCache(false) 
      case _ => ()
    }

    st.categoricalFeaturesInfo=Map[Int, Int]()
    st
  }
  
}

trait MLClassifier extends ModeloML with Classifier {
  
  def getPrediction: ( DataFrame ) => RDD[(Double, Double)] =  { data =>
    data.map{ x => (x.getDouble(x.fieldIndex("prediction")), x.getDouble(x.fieldIndex("label")) ) }
  }
   
}

trait MLTreeClassifier extends MLClassifier {
  
  override def getPrediction: ( DataFrame ) => RDD[(Double, Double)] =  { data =>
    data.map{ x => (x.getString(x.fieldIndex("predictedLabel")).toDouble, x.getDouble(x.fieldIndex("label")) ) }
  }
   
}


// Hack for the Perceptron, that does not have save at Spark 1.6.* version. Trying to have it at the lowest level possible, it is at Classifier and not at predictor 
trait SaveableMLClassifier extends MLClassifier with SaveableModeloML {
   type T <: Saveable with Transformable
   
   override def doSave(path: String): Unit = {
     val sc = com.modelicious.in.app.Application.sc
     modelo.getOrElse(throw new RuntimeException("Train your model before trying to save it!") ).save(sc, path)
   }
   
}


