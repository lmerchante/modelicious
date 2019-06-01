package com.modelicious.in.data

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import com.modelicious.in.app.Application
import com.modelicious.in.data._

import com.modelicious.in.tools.FileUtils

import scala.reflect.runtime.universe._
//import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.{ universe => ru }


object HandleInstanceCreator {

  def newInstance[T: TypeTag](s: String, opts: Map[String, String]) : T = {   
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val classType = ru.typeOf[T].typeSymbol.asClass
    val constructor = typeTag[T].tpe.declaration(ru.nme.CONSTRUCTOR).asMethod
    val constructorMethod = m.reflectClass(classType).reflectConstructor(constructor)
    constructorMethod.apply(s, opts).asInstanceOf[T] 
  }
  
}


/**
 * Decorator that creates an InputHandle that loads the data from local, stores to Hive, and then loads to the
 * given InputHandle Type
 */
class InputLocalDataHandle[T <: InputDataHandle: TypeTag](uri: String, opts: Map[String, String]) extends InputDataHandle(uri, opts) {

  def doRead(): DataWrapper = {
    val uuid = Application.get.uuid
    
    val my_remote_temp_location: String = Application.get.getPath( uuid +  "/tempFromLocal/"  + new java.io.File(uri).getName )
    val my_remote_temp_location_data = my_remote_temp_location + "/data"
    val my_remote_temp_location_metadata = my_remote_temp_location + "/metadata.xml"
    
    // Load to temp Hive location
    FileUtils.copyFromLocal(uri_df, my_remote_temp_location_data )
    FileUtils.copyFromLocal(uri_md, my_remote_temp_location_metadata )
    
    HandleInstanceCreator.newInstance( my_remote_temp_location, opts )( implicitly[TypeTag[T]]).read( )
  }
}

/**
 * Decorator that creates copies the output data stored in Hive by the given OutputHandle to local
 */

class OutputLocalDataHandle[T <: OutputDataHandle: TypeTag](uri: String, opts: Map[String, String]) extends OutputDataHandle(uri, opts) { 
  
  def doWrite(data: DataWrapper) = {
    val uuid = Application.get.uuid

    val my_remote_temp_location: String = Application.get.getPath( uuid +  "/tempToLocal/"  + new java.io.File(uri).getName )
    val my_remote_temp_location_data = my_remote_temp_location + "/data"
    val my_remote_temp_location_metadata = my_remote_temp_location + "/metadata.xml"
    
    HandleInstanceCreator.newInstance( my_remote_temp_location, opts )( implicitly[TypeTag[T]]).write( data )
    
    FileUtils.copyToLocal(my_remote_temp_location_data, uri_df )
    FileUtils.copyToLocal(my_remote_temp_location_metadata, uri_md )

  }
}

