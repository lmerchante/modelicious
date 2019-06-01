package com.modelicious.in.stage

import com.modelicious.in.tools.Implicits._
import org.apache.log4j.Logger

// Maybe better a class and pass parameters through constructor instead of init?
// 
trait Stage {

  var _error_code: Int = 0
  
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  def execute( conf: scala.xml.Elem ): scala.xml.Elem
  
  def preExecute( ) = {
     log_persistent_rdds("PRE")
  }
  
  def postExecute( ) = {
    log_persistent_rdds("POST")
  }
  
  
  private def log_persistent_rdds( phase: String ) = {
    val myRdds = com.modelicious.in.app.Application.sc.getPersistentRDDs
    log.file( "==============================================================" )
    log.file( "==============================================================" )
    log.file( "Persistent RDDs on " + phase )
    for( r <- myRdds ) { log.file( r._1+ ": " + r._2.toString )  }
    log.file( "==============================================================" )
    log.file( "==============================================================" )
  }

}