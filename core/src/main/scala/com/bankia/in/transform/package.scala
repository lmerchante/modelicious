package com.modelicious.in

import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import com.modelicious.in.tools.SchemaTools
import org.apache.spark.sql.types.StructType
import com.modelicious.in.data.DataWrapper
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import scala.xml.{Null, Attribute, Text}


/**
 * == Data transformers ==
 * 
 * Contains all the classes that allow to transform dataframes.
 * 
 * The current list of transformations available is:
 * 
 *   - Filter Columns
 *   - Drop columns
 *   - Discretization
 *   - Feature Selection
 *   - One hot Enconding
 *   - PCA
 *   - Purge Correlated Columns
 *   - Purge Invariant Columns
 *   - Remove Outliers
 *   - Sampling
 *   - Scaling
 * 
 * 
 * The flow mimics the one that is implemented by ML and MLLIB model in the sense that, for each transformation, there is a main class 
 * ''DataTransformer'' that can be configured and fited and another class ''DataTransformerModel'' That is created when the former is 
 * fitted and that will apply the real transformation to the DataFrame.<br/> 
 * Also, for simplicity when creating new Models and not having to tinker with xml serialization a helper class 
 * [[com.modelicious.in.transform.TransformerState]] must be inherited. 
 * Creating the configuration, or ''state'' in this case, that the Model and the TransformModel will share is then automatic.
 * 
 * To create a brand new '''X''' transformation flow, the following classes must be created:
 * 
 *   - An '''x TransformerState''' case class. This one must inherit from TransformerState that has a set of utilities that allow to, 
 *   normally, obviate the xml serialization logic. Check [[com.modelicious.in.transform.TransformerState]] for more information on the 
 *   capabilities of this class.
 *   - A '''XTransformer''' that inherits from [[com.modelicious.in.transform.DataTransformer]] and must meet the follwoing interface:
 * {{{
 * abstract class DataTransformer[T <: TransformerState]( implicit tt: scala.reflect.runtime.universe.TypeTag[T], ct: scala.reflect.ClassTag[T] ) {
 *    
 *  def configure( conf: scala.xml.NodeSeq ): this.type = {
 *    state = TransformerState.fromXML[T]( conf.asInstanceOf[scala.xml.Node] )
 *    this
 *  }
 *  
 *  def fit( data: DataWrapper ) : DataTransformerModel[T]
 *  
 *}
 * }}}
 * 
 * where ''T'' is the case class of the first point.
 * It must implement a fit method that receives the data and creates the TransformerModel that will apply the transform.
 * This method will complete the state configuration that allows the generated TransformModel to apply the final transformation to the data.
 * Optionally, the user can also override the [[com.modelicious.in.transform.DataTransformer.configure]] method to apply some validity
 * checks on the passed state configuration.
 * An example of when this can be helpful is on the [[com.modelicious.in.transform.DiscretizeTransform.configure]]  method.
 * 
 * - Finally, a '''XTransformerModel''' that follows the following interface:
 * 
 * {{{
 * abstract class DataTransformerModel [T <: TransformerState]( theState : TransformerState ) {
 *   
 *   def doTransform( data: DataWrapper ): DataWrapper
 *   
 *   var rslt : scala.xml.Node = _
 *   
 * }
 * }}}
 * 
 * where T is, again the state case class that holds all the configuration needed to apply the transformation.
 * The doTransform is the method that, using the state case class configuration, applies the transformation to the input DataFrame, 
 * returning a new one. And, also the ''var rslt'' should be filled with the result of the transformation, so the user can do a follow 
 * up of the results of each transformation.
 *
 * == The Transformation Flow ==
 *
 * When we train a Model, we must configure a whole set of transformations with a set of given parameters. 
 * Take, for example, that we want to apply a standarization.
 * We have 2 cases here, the training of the model, and the effective application of the model to real data.
 * 
 * === Training ===
 *   	- We create a DataTransformer with the configuration set to apply only standarization by mean.
 *   	- When we fit the train data to the DataTransformer object, this will return a DataTransformerModel with an internal state that 
 *   has the relevant information, in this case, the mean for each column of the train data. This data can be stored.
 *   	- We apply the ''TransformModel'' '''transform''' method to the train data.
 *   	- We train our Model with the transformed train data.
 *   
 * === Prediction ===
 * If our model is a success and we want to apply it to brand new real data, we have to apply to this data 
 * the very same transformations that we used for the training set.
 * Luckily, whe we save the model, we also saved the chain fo transformations that we applied to the train data. So we have the state 
 * that the TransformModel had when it applied the transformation to the Data.
 * So this time, the flow is the following:
 * 		- We read from the model metadata the state with the mean for every column.
 * 		- We create a StandarizerTransformModel to which we pass this state we just read.
 * 		- The data is transformed by the TransformModel.
 * 		- Finally, the Model can just compute the predictions over this transformed real data set. 
 * 		
 * 
 * == Creation of new States ==
 * 
 * The state is the base of the transform model as it will hold both the configuration passed by the user, that is read by the 
 * [[com.modelicious.in.transform.DataTransformer.configure]] method, and the configuration generated by the 
 * [[com.modelicious.in.transform.DataTransformer.fit]] method.
 * In order to have a faster development of new transformations, the task of reading and writing configurations from the XML has 
 * been delegated to the [[com.modelicious.in.transform.TransformerState]] class, from which any new state must inherit.
 * A number of types has been already implementd, that should cover the new cases, and, only for new types this would need to be extended.
 * 
 * All the case classes must be declared with the following contraints:
 * 
 *   - The case class name must be the same as it will apper on the configuration XML ( This includes case or underscores ) 
 *   - It must not accept any argument it their constructor (This is due to a limitation on scala reflection, it is not easy to call to the default constructor.)
 *   - It must declare each variable member that can apper on the xml element  wit its type and also a default value
 *   
 * Take for example the 
 * 
 * {{{
 * case class feature_selection() extends TransformerState{
 *   var method: String = "mifs"
 *   var num_features: Int = 10
 *   var num_partitions: Int = 100
 *   var selected_features: List[Int] = List() // TODO: Sería mejor quedarse con los nombres
 * }  
 * }}}
 * 
 * that will match with this xml in the conf files:
 * 
 * <feature_selection>
 * 		<num_features>10</num_features>
 *  	<num_partitions>100</num_partitions>
 * </feature_selection>		
 * 
 * We can see the following:
 * 
 *  - The case class name is '''exactly''' the same than the xml element.
 *  - Each var in the case class matches a child element in the xml. 
 *  - If one is not present, like '''method''', the default value will be set.
 *  - The variable '''selected_features''' is intended to be filled by the fit method.  
 *   
 * 
 * == Helper methods ==
 * 
 * 
 * @see [[com.modelicious.in.transform.TransformerState]]
 * @see [[com.modelicious.in.transform.DataTransformer]]
 * @see [[com.modelicious.in.transform.DataTransformerModel]]
 * 
 */
package object transform {

  type Splits = Map[String, Array[Double]]
  type OneHotLabels = Map[String, Array[String]]
  case class OutliersStats(count: Long, mean: Double, std: Double)
  type MapOutliersStats = Map[String, OutliersStats]
  type TransformCreator[T <: TransformerState] = (() => DataTransformer[T])
  type ModelCreator[T <: TransformerState] = (TransformerState => DataTransformerModel[T])

  def withRDDAndSameSchema(data: DataFrame)(block: RDD[LabeledPoint] => RDD[LabeledPoint]): DataFrame = {
    // Seguro q muy eficiente no es ....
    val schema = data.schema
    val labeled = SchemaTools.DFtoLabeledRDD(data)

    val transformed = block(labeled)

    SchemaTools.LabeledRDDtoDF(transformed, schema)
  }

  def withRDDAndVaryingSchema(data: DataFrame)(block: (RDD[LabeledPoint], StructType) => (RDD[LabeledPoint], StructType)): DataFrame = {
    // Seguro q muy eficiente no es ....
    val schema = data.schema
    val labeled = SchemaTools.DFtoLabeledRDD(data)

    val (transformed, newSchema) = block(labeled, schema)

    SchemaTools.LabeledRDDtoDF(transformed, newSchema)
  }

  def withRDDAndSameSchema(data: DataWrapper)(block: RDD[LabeledPoint] => RDD[LabeledPoint]): DataWrapper = {
    data { dataframe =>
      withRDDAndSameSchema(dataframe) { rdd =>
        block(rdd)
      }
    }
  }

  def withRDDAndVaryingSchema(data: DataWrapper)(block: (RDD[LabeledPoint], StructType) => (RDD[LabeledPoint], StructType)): DataWrapper = {
    data { dataframe =>
      withRDDAndVaryingSchema(dataframe) { (rdd, schema) =>
        block(rdd, schema)
      }
    }
  }

  implicit class SplitsExtender(s: Splits) {
    def toSplitString: String = { s.map(s => s._1 + ":" + s._2.mkString("[", ",", "]")).mkString(";") }
  }

  implicit class OneHotLabelsExtender(oh: OneHotLabels) {
    def toOneHotLabelsString: String = { oh.map(o => o._1 + ":" + o._2.mkString("[", ",", "]")).mkString(";") }
  }

  implicit class DenseMatrixExtender(dm: DenseMatrix) {
    def toDenseMatrixString: String = { dm.numRows + "; " + dm.numCols + ";" + dm.toArray.mkString("[", ",", "]") }
  }

  implicit class VectorExtender(v: Vector) {
    def toVectorString: String = v.toArray.mkString("[", ",", "]")
  }

  implicit class OutliersExtender(mos: MapOutliersStats) {
    def toOutliersStatsString: String = mos.map(os => os._1 + ": " + os._2.count + ", " + os._2.mean + ", " + os._2.std).mkString("; ")
  }
  
  implicit class MapOfLongs( mol: Map[String, Long]) {
    def toMapOfLongsString: String = mol.map{ x => x._1+ ": " + x._2   }.mkString("; ")
  }

  implicit class MapOfDoubles( mod: Map[String, Double]) {
    def toMapOfDoublesString: String = mod.map{ x => x._1+ ": " + x._2   }.mkString("; ")
  }
  
  implicit class StringToStruct(s: String) {
    def toSplit: Splits =
      s.split(";")
        .map(_.split(":"))
        .map {
          case Array(a, b) =>
            (a.trim -> b.trim.replace("[", "").replace("]", "").split(",").map(_.toDouble))
        }
        .toMap

    def toOneHotLabels: OneHotLabels =
      s.split(";")
        .map(_.split(":"))
        .map {
          case Array(a, b) =>
            (a -> b.trim.replace("[", "").replace("]", "").split(","))
        }.toMap

    def toDenseMatrix: DenseMatrix = {
      val data = s.split(";").map(_.trim)
      val numRows = data(0).toInt
      val numCols = data(1).toInt
      val values = data(2).trim.replace("[", "").replace("]", "").split(",").map(_.toDouble)
      new DenseMatrix(numRows, numCols, values)
    }

    def toDenseVector: Vector = {
      Vectors.dense(s.trim.replace("[", "").replace("]", "").split(",").map(_.toDouble))
    }

    def toOutliersStats: MapOutliersStats = {
      s.split(";")
        .map(_.split(":"))
        .map {
          case Array(a, b) =>
            val key = a.trim
            val value = b.split(",").map(_.trim)
            (key -> OutliersStats(value(0).toLong, value(1).toDouble, value(2).toDouble))
        }.toMap.asInstanceOf[MapOutliersStats] // Evita q el compilador de eclipse de error
    }
    
    def toMapOfLongs: Map[String, Long] = {
      s.split(";").map(_.trim).map{ x => 
        val a = x.split(":")
        ( a(0).trim -> a(1).trim.toLong ) 
      }.toMap
    }

    def toMapOfDoubles: Map[String, Double] = {
      s.split(";").map(_.trim).map{ x => 
        val a = x.split(":")
        ( a(0).trim -> a(1).trim.toDouble ) 
      }.toMap
    }
    
  }

  import scala.reflect._
  import scala.reflect.runtime.{ universe => ru }
  implicit class TransformerStateSerializer[T <: TransformerState](c: T)(implicit tt: ru.TypeTag[T], ct: ClassTag[T]) {
    def toXML: scala.xml.Node = {
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val im = m.reflect(c)
      val fields: scala.collection.mutable.Map[String, ru.FieldMirror] = scala.collection.mutable.Map()

      tt.tpe.declarations.filter(_.asTerm.isVar).foreach { f =>
        val termfield = ru.typeOf[T].member(f.name).asTerm
        val fieldMirror = im.reflectField(termfield)
        fields += (f.name.decoded -> fieldMirror)
      }

     var myNode = <a>{ for (f <- fields) yield <b>{ encodeValue(f._2) }</b>.copy(label = f._1.trim) }</a>.copy(label = c.getClass.getName.split('.').last)
     
     c.attributes.foreach{ case(key, value) =>
       myNode = myNode % Attribute(None, key, Text(value), Null)
     }
     
     myNode
    }

    private def encodeValue(fm: ru.FieldMirror): String = {
      fm.symbol.asTerm.typeSignature match {
        case tpe if tpe =:= ru.typeOf[Splits]           => fm.get.asInstanceOf[Splits].toSplitString
        case tpe if tpe =:= ru.typeOf[List[Int]]        => fm.get.asInstanceOf[List[Int]].mkString(",")
        case tpe if tpe =:= ru.typeOf[List[String]]     => fm.get.asInstanceOf[List[String]].mkString(",")
        case tpe if tpe =:= ru.typeOf[Array[String]]    => fm.get.asInstanceOf[Array[String]].mkString(",")
        case tpe if tpe =:= ru.typeOf[Array[Long]]      => fm.get.asInstanceOf[Array[Long]].mkString(",")
        case tpe if tpe =:= ru.typeOf[OneHotLabels]     => fm.get.asInstanceOf[OneHotLabels].toOneHotLabelsString
        case tpe if tpe =:= ru.typeOf[DenseMatrix]      => fm.get.asInstanceOf[DenseMatrix].toDenseMatrixString
        case tpe if tpe =:= ru.typeOf[MapOutliersStats] => fm.get.asInstanceOf[MapOutliersStats].toOutliersStatsString
        case tpe if tpe =:= ru.typeOf[Map[String, Long]]=> fm.get.asInstanceOf[Map[String, Long]].toMapOfLongsString
        case tpe if tpe =:= ru.typeOf[Map[String, Double]]=> fm.get.asInstanceOf[Map[String, Double]].toMapOfDoublesString
        case _                                          => fm.get.toString()
      }
    }
  }

}
