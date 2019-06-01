package com.modelicious.in.data.memory

import org.apache.spark.sql.DataFrame
import com.modelicious.in.data._

// All this is just a hack to not have to read/write if not needed 
class InputMemoryDataHandle(uri: String, opts: Map[String, String]) extends InputDataHandle(uri, opts){
  def doRead(): DataWrapper = {
    MemoryData.rdds( uri )
  }
  override def dispose() = { MemoryData.rdds.remove(uri) } // Avoid it to keep a reference, It must be GC'ed 
}

class OutputMemoryDataHandle(uri: String, opts: Map[String, String]) extends OutputDataHandle(uri, opts){
  def doWrite(data: DataWrapper) = {
    MemoryData.rdds( uri ) = data
  }
  // No need to dispose, we are keeping it for the next command
}

private[data] object MemoryData {
  val rdds = scala.collection.mutable.Map[String, DataWrapper]()
}