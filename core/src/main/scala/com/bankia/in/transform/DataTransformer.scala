package com.modelicious.in.transform

import org.apache.log4j.Logger

import com.modelicious.in.app.Application
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.Vector

trait TransformerState {
  var attributes: Map[String,String] = Map()
}


/**
 * Automates the deserialization from XML to a state case class.
 * 
 * It uses reflection heavily and the process is explained in more detail in the [[com.modelicious.in.transform.TransformerState.fromXML]] method.
 * 
 * Any new type that should be de/serialized should be added both to this object´s 
 * [[com.modelicious.in.transform.TransformerState.setValue]] (for deserialization i.e. from String to the needed Type) and to the package's 
 * implicit methods for the serialization. 
 */
object TransformerState {
  
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  import scala.reflect._
  import scala.reflect.runtime.{universe => ru}
  
  /**
   * Automates the reading of XML to a state '''T <: TransformerState'''.
   * It takes the root element os the passes xml Node and tries to instance a case class named like this root label with all the default values.
   *  
   * It then iterates each child element and tries to set, using the case class accessors and the helper method 
   * [[com.modelicious.in.transform.DataTransformer.TransformerState.setValue]], to the value hold by the text of the element.
   * Conversions from the String to the needed type must be applied and a implicit class [[com.modelicious.in.transform.DataTransformer.StringToStruct]] 
   * has been implemented to ease this.
   * 
   * 
   * @see [[com.modelicious.in.transform.DataTransformer.TransformerState.setValue]]
   * @see [[com.modelicious.in.transform.DataTransformer.StringToStruct]]
   * 
   */
  def fromXML[T <: TransformerState ]( node: scala.xml.Node )( implicit tt: ru.TypeTag[T], ct: ClassTag[T] ): T = {
    
    // Create empty case class ( All values must be defaulted to do this )
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val my_class = newInstance[T]
    val im = m.reflect(my_class)
    
    log.file( "Settings for " + node.label )
    
      node.child.filterNot( _.label == "#PCDATA" ).foreach { n =>
        try {
          log.file( n.label + " = " + n.text)
          val termname = ru.newTermName( n.label )
          val termfield = ru.typeOf[T].member(termname).asTerm
          val fieldMirror = im.reflectField(  termfield  )      
          if (!n.text.isEmpty) setValue( fieldMirror, n.text )
        }
        catch {
          case e: scala.ScalaReflectionException => { 
            log.file( "TransformerState: Error looking for parameter. Please, check your xml!" + "\n\tField " + n.label + 
                " is not part of the configuration class for " + node.head.label ) 
          }
          case e: Exception => { 
            log.file( "TransformerState: Error:" + e.getStackTraceString + "\n" +e.getMessage() )
          }
        }
      }
    
    val attributesMap : scala.collection.mutable.Map[String, String]= scala.collection.mutable.Map()
    node.attributes.foreach { a => 
      attributesMap(a.key) = a.value.toString()
    }
    my_class.attributes = attributesMap.toMap
    my_class
  }
  
  private def newInstance[T: ClassTag] = implicitly[ClassTag[T]].runtimeClass.newInstance.asInstanceOf[T]
  
  private def setValue( fm: ru.FieldMirror, value: String ) = 
    fm.symbol.asTerm.typeSignature match {
      // https://groups.google.com/forum/#!topic/scala-user/XElKxcK39Ik
      case tpe if tpe =:= ru.typeOf[String] => fm.set(value)  
      case tpe if tpe =:= ru.typeOf[Int]  => fm.set(value.toInt)
      case tpe if tpe =:= ru.typeOf[Double]  => fm.set(value.toDouble)
      case tpe if tpe =:= ru.typeOf[Boolean] => fm.set( value.toBoolean )
      case tpe if tpe =:= ru.typeOf[Array[String]] => fm.set(value.split(",").map( _.trim ) )
      case tpe if tpe =:= ru.typeOf[Array[Long]] => fm.set(value.split(",").map( _.trim ).map( _.toLong ) )
      case tpe if tpe =:= ru.typeOf[List[String]] => fm.set(value.split(",").map( _.trim ).toList )
      case tpe if tpe =:= ru.typeOf[List[Int]] => fm.set(value.split(",").map( _.trim.toInt ).toList )
      case tpe if tpe =:= ru.typeOf[Splits] => fm.set(value.toSplit )
      case tpe if tpe =:= ru.typeOf[OneHotLabels] => fm.set(value.toOneHotLabels )
      case tpe if tpe =:= ru.typeOf[MapOutliersStats] => fm.set( value.toOutliersStats )
      case tpe if tpe =:= ru.typeOf[DenseMatrix] => fm.set(value.toDenseMatrix )
      case tpe if tpe =:= ru.typeOf[Vector] => fm.set( value.toDenseVector )
      case tpe if tpe =:= ru.typeOf[Map[String, Long]] => fm.set( value.toMapOfLongs )

      case _ => { 
        val msg = " I dont know which type to use!!!! " + value + " Is type " + fm.get.getClass.getName
        log.file( msg )
        throw new RuntimeException( msg )        
      }
  }
  
}


/**
 * Base class for all transformer objects. It holds a var state of type T <: TransformerState which has the needed configuration for the whole
 *  transformation process, i.e. the transform and the model generated by the [[com.modelicious.in.transform.DataTransformer.fit]] method.
 * 
 * @tparam The case class inheriting from [[com.modelicious.in.transform.TransformerState]] that holds the configuration for the transformation
 * 
 * Method [[com.modelicious.in.transform.DataTransformer.fit]] must be always be implemented, while [[com.modelicious.in.transform.DataTransformer.configure]] 
 * must be overriden only when some extra validation must be done to the parameters. 
 * e.g. [[com.modelicious.in.transform.DiscretizeTransform.configure]]
 * 
 * Method [[com.modelicious.in.transform.DiscretizeTransform.exec]] is just a shortcut the chains all the methods and transforms the data. Kept for 
 * backwards compatibilty and should nolt be used
 * 
 */
abstract class DataTransformer[T <: TransformerState]( implicit tt: scala.reflect.runtime.universe.TypeTag[T], ct: scala.reflect.ClassTag[T] ) {
  
  var state : T = _
  
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def configure( conf: scala.xml.NodeSeq ): this.type = {
    state = TransformerState.fromXML[T]( conf.asInstanceOf[scala.xml.Node] )
    this
  }
    
  def fit( data: DataWrapper ) : DataTransformerModel[T]
 
  /**
   * @deprecated( Kept for backwards compatibility and candidate for purge  , 1.0)
   */
  def exec( conf: scala.xml.NodeSeq, data: DataWrapper ) : DataWrapper = {
    val transformer = configure(conf).fit( data )
    transformer.transform( data )
  }
  
}

/**
 * Base class for all transformer model objects. It holds a var state of type T <: TransformerState which has the needed configuration for the whole
 *  transformation process, i.e. the transform and the model generated by the [[com.modelicious.in.transform.DataTransformer.fit]] method.
 * 
 * @tparam The case class inheriting from [[com.modelicious.in.transform.TransformerState]] that holds the configuration for the transformation
 * 
 * Method [[com.modelicious.in.transform.DataTransformerModel.doTransform]] must be always be implemented.
 * 
 * [[com.modelicious.in.transform.DataTransformerModel.rslt]] var should be filled with a meaningfull xml element holding the result of the transfomration so it is easy to 
 * trace the process in case of any error. 
 * 
 */
abstract class DataTransformerModel [T <: TransformerState]( theState : TransformerState ) {
  
  var state : T = theState.asInstanceOf[T]
  
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  def doTransform( data: DataWrapper ): DataWrapper
  
  def transform( data: DataWrapper ): DataWrapper = {
    val d = doTransform( data )
    state.attributes.get("force_checkpoint") match {
      case Some("true") => if(  Application.get().allow_checkpoints ) d.checkPoint else d
      case _ => d
    }
  }
  
  var rslt : scala.xml.Node = <result>Not implemented</result>
  def result: scala.xml.Node = rslt
  
  def useForPrediction = true
}

object DataTransformer {
  
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  private[transform] def register [ A <: TransformerState ]( transform: TransformCreator[A] ) 
  ( implicit tt: scala.reflect.runtime.universe.TypeTag[A] ) = {
    val name = tt.tpe.toString.split('.').last
    transformers.put( name, transform )
    log.file( "DataTransformer " + name + " registered"  )
  }
  
  private val transformers = scala.collection.mutable.Map[String, TransformCreator[_]] ()
    
  def apply( t: scala.xml.NodeSeq ): DataTransformer[_] = {
      return transformers( t.head.label )()
  }
  
}

object DataTransformerModel {
  
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  private[transform] def register [ A <: TransformerState ]( model_creator: ModelCreator[A]) 
  ( implicit tt: scala.reflect.runtime.universe.TypeTag[A], ct: scala.reflect.ClassTag[A] ) = {
    val name = tt.tpe.toString.split('.').last
    states.put( name , TransformerState.fromXML[A](_) )
    transformers.put( name, model_creator )
    log.file( "DataTransformerModel " + name + " registered"  )
  }

  
  private val states = scala.collection.mutable.Map[String, ( scala.xml.Node => TransformerState) ] ()
  private val transformers = scala.collection.mutable.Map[String, ModelCreator[_]] ()
  
  def apply( t: scala.xml.Node ): DataTransformerModel[_] = {
    val state = states(t.head.label)( t )
    transformers( t.head.label )(state)
  }
  
}

  /**
   * This object is used to register at once both the Transform and the TranformModel.
   *
   */

object DataTransformRegistrar {
  
  /**
   * Registers at once both the Transform and the TranformModel
   * 
   * As stated on the [[com.modelicious.in.transform.TransformerState]] documentation, there is a convention such the xml element and the state case class share the name.
   * This methods relies on that to get the key from the underlying state class of the models passed. 
   * 
   * 
   * @param transform The DataTransformer. With type [[com.modelicious.in.transform.TransformerState]]
   * @param model_creator The Data TransformerModel. With type [[com.modelicious.in.transform.ModelCreator]]
   *   
   * For example, to add PCATransform to the factory:
   * 
   * {{{    DataTransformRegistrar.register((() => new PCATransform), new PCATransformModel(_) ) }}}
   * 
   * @see [[com.modelicious.in.Init.initialize]]
   * @see [[com.modelicious.in.transform.TransformerState]]
   * @see [[com.modelicious.in.transform.TransformCreator]]
   * @see [[com.modelicious.in.transform.ModelCreator]]
   */
  
  def register[ A <: TransformerState ](  transform: TransformCreator[A], model_creator: ModelCreator[A]) 
  ( implicit tt: scala.reflect.runtime.universe.TypeTag[A], ct: scala.reflect.ClassTag[A] ) = {
    DataTransformer.register[A]( transform )
    DataTransformerModel.register[A]( model_creator )
  }
}
