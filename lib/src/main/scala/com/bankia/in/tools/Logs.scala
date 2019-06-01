package com.modelicious.in.tools


import org.apache.log4j.Logger
import org.apache.log4j.{Appender, FileAppender, ConsoleAppender}
import org.apache.log4j.PatternLayout
import org.apache.log4j.Level
import org.apache.log4j.spi.LoggingEvent
import scala.collection.JavaConversions._


object Logs {
  
  def config_logger(appName: String, logPath: String) =
  {
    val logger = Logger.getLogger("com.modelicious.in")// .getRootLogger()    
    
    val PATTERN = "[%X{appName}] %d{yyyy-MM-dd HH:mm:ss} %-5p - %X{Categoria} [Model: %X{ModelID} - Pipeline: %X{Pipeline} - Phase: %X{Phase}] - %m%n"
    
//    val allAppenders = logger.getAllAppenders
//    
//    for ( a <- allAppenders) {
//      val appender = a.asInstanceOf[Appender]
//      
//      println( "================================================" + appender.getName() )
//      
//      appender.setLayout(new PatternLayout(PATTERN));
//    }
    
    val fa = new MyFileAppender();
    fa.setName("FP_Logger");
    fa.setFile(logPath);
    fa.setLayout(new PatternLayout(PATTERN));
    fa.setThreshold(Level.ERROR);
    fa.setAppend(true);
    fa.activateOptions();
    logger.addAppender(fa)
    
    logger.info( "Log initialized to " + logPath  )
        
//    val lmodelicious = Logger.getLogger("com.modelicious.in")
//    
//    val console = new ConsoleAppender(); //create appender
//    //configure the appender
//  
//    console.setLayout(new PatternLayout(PATTERN)); 
//    console.setThreshold(Level.ERROR);
//    console.activateOptions();
//    lmodelicious.addAppender(console);

    
  }

}

class MyFileAppender extends FileAppender {

    override def append(e: LoggingEvent) {     
      try {
        val msg = e.getMessage.asInstanceOf[String].replaceAll("\\x1b\\[[0-9;]*m", "")
        
        val myEvent = new LoggingEvent(e.fqnOfCategoryClass, e.getLogger, e.timeStamp, e.getLevel, 
            msg, e.getThreadName, e.getThrowableInformation, e.getNDC, e.getLocationInformation, e.getProperties)   
        
        super.append( myEvent )
      } catch {
        case e: Exception => e.printStackTrace(); // Dont log it as it will lead to infinite loop. Simply print the trace
      }
    }
}

