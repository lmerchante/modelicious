package com.modelicious.in.data

import org.apache.log4j.Logger
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.app.Application

/**
 * Base class that all represents the each of the application data resources regardless of the format used to store it or if it is input/output
 * 
 * It extends Disposable as we want it to be used as a resource that always get closed.
 * It only stores the URI of both data and metadata members of a DataWrapper.
 * 
 * @see [[com.modelicious.in.app.Config.getPath]]
 * @see [[com.modelicious.in.data.DataWrapper]]
 * @see [[com.modelicious.in.data.Disposable]]
 * 
 */
sealed abstract class DataHandle(private val uri: String, opts: Map[String, String] ) extends Disposable {
  val abs_uri = Application.get.getPath( uri ) // Make it absolute
  val uri_df = abs_uri + "/data" 
  val uri_md = abs_uri + "/metadata.xml" 
}


/**
 * Base Class for all Input DataHandles regardless of the format, child classes must implement the doRead method that stores the
 * data with the desired format.
 *  
 */
abstract class InputDataHandle( uri: String, opts: Map[String, String] ) extends DataHandle(uri, opts) {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def read(): DataWrapper = {
    log.file("Reading file " + abs_uri)
    var file = doRead()
    log.file("File " + abs_uri + " read succesfully")
    
    log.file( "DataHandle Options: " + opts.mkString("[",",","]") )
    
    opts.get( "repartition" ) match {
      case Some( x ) => {
        log.file( " Repartitioning input data to " + x + " partitions " )
        file = file.repartition( x.toInt )
        }
      case _ => {}
    }
  
    file
  }

  def doRead(): DataWrapper
}


abstract class OutputDataHandle(uri: String, opts: Map[String, String]) extends DataHandle(uri, opts) {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  log.file( "DataHandle Options: " + opts.mkString("[",",","]") )
  
  def write(data: DataWrapper) = {
    log.file("Writing to file " + abs_uri)
    doWrite(data)
    log.file("File " + abs_uri + " succesfully written")
  }

  def doWrite(data: DataWrapper)
}

// Generic. But or now, only for output
// Not sure if it would be worth to have several input origins ( with same struct, ofc ) But then we could reuse this 
trait CompositeHandle[T <: DataHandle] {
  var handles: List[T] = List()

  def add(handle: T) = {
    handles = handle :: handles
  }

}

class CompositeOutputDataHandle() extends OutputDataHandle("Composite", Map()) with CompositeHandle[OutputDataHandle] {

  def doWrite(data: DataWrapper) = {
    for (h <- handles) {
      h.write(data)
    }
  }
}



