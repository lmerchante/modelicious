package com.modelicious.in.data

import scalax.file.Path


/**
 * Class that groups all the handling of data, regardless of the origin
 * 
 * The Main App singleton should create only one object of this type that will be used for all data input/output.
 * 
 */
// This is a class, not an object as we want to be able to inject Mocks for testing purposes.
class DataManager{

  
  def read(uri: String): DataWrapper = {
    var data: DataWrapper = new DataWrapper ( com.modelicious.in.app.Application.sqlContext.emptyDataFrame )
    using(getInput(uri)) {
      d => data = d.read()
    }
    data
  }

  def write(uri: String, data: DataWrapper) = {
    using(getOutput(uri)) {
      d => d.write(data)
    }
  }
  
  def deleteLocal(uri: String): Boolean = {
    if  ((uri.split(":")(0)=="lcsv") || (uri.split(":")(0)=="lcsvgz")) {
      val path = Path.fromString(uri.split(":")(1)) 
      path.deleteRecursively(continueOnFailure = false) 
      true
    } else false
  }

  private def getInput(uri: String): InputDataHandle = {
    val (prefix, location, opts) = parseURI(uri)

    return inputwrappers(prefix)(location, opts)
  }

  private def getOutput(uri: String): OutputDataHandle = {
    
    // One more indirection calling write is not a problem. so keep the code simple just using always a compsoite 
    val handle = new CompositeOutputDataHandle

    uri.split('|').foreach { h =>
      val (prefix, location, opts) = parseURI(h)
      handle.add(outputwrappers(prefix)(location, opts))
    }

    return handle
  }

  private val inputwrappers = Map[String, ((String, Map[String, String]) => InputDataHandle)](
    "memory" -> (new memory.InputMemoryDataHandle(_, _)),
    "hdfs" -> (new InputHDFSDataHandle(_,_)),
    "lhdfs" -> (new InputLocalDataHandle[InputHDFSDataHandle](_,_)), // This is a Parket
    "csv" -> (new InputCSVDataHandle(_,_)),
    "lcsv" -> (new InputLocalDataHandle[InputCSVDataHandle](_,_)),
    "csvgz" -> (new InputCSVGZDataHandle(_,_)),
    "lcsvgz" -> (new InputLocalDataHandle[InputCSVGZDataHandle](_,_)) 
    )

  private val outputwrappers = Map[String, ((String, Map[String, String]) => OutputDataHandle)](
    "memory" -> (new memory.OutputMemoryDataHandle(_, _)),
    "hdfs" -> (new OutputHDFSDataHandle(_, _)),
    "lhdfs" -> (new OutputLocalDataHandle[OutputHDFSDataHandle](_,_)), // This is a Parket
    "csv" -> (new OutputCSVDataHandle(_, _)),
    "lcsv" -> (new OutputLocalDataHandle[OutputCSVDataHandle](_,_)),
    "csvgz" -> (new OutputCSVGZDataHandle(_, _)),
    "lcsvgz" -> (new OutputLocalDataHandle[OutputCSVGZDataHandle](_,_)) ,
    "db" -> ( new OutputDBDataHandle(_,_)),
    "scores" -> ( new OutputScoreDBDataHandle(_,_))
    )

  def debugMemory: Map[ String, String ] = {
    memory.MemoryData.rdds.map{ case(k, v)=> (k, v.toString) }.toMap
  }
    
}