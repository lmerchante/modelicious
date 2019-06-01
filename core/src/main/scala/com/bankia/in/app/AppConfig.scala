package com.modelicious.in.app

import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.{FileUtils, EnvReader}
import com.modelicious.in.Constants
import com.modelicious.in.tools.DocGenerator
import org.apache.log4j.{Logger, Level}
//import org.apache.spark.h2o._
import util.Random
import scala.collection.JavaConversions

import scala.language.reflectiveCalls
import scala.language.postfixOps


import java.util.UUID


/**
 * Parametrize mail notifications
 */

case class MailOptions(mailEnabled: Boolean, mailRecipient: String)


/**
 * Holds all Application related configuration and resources. 
 * Must be extended by a class setting the needed configuration.
 * 
 * Allows to have a single entry point to access to the application context:
 * 		- '''sc''': SparkContext
 * 		- '''sqlContext''': HiveContext
 * 		- '''mailOptions''': If not Null, a result mail will be sent to the given recipients when execution has finished
 * 		- '''dataManager''': Resource manager for all kind of Data Types supported by the application see [[com.modelicious.in.data.DataManager]]
 * 		- '''base''': Base folder in hdfs setting where all relative paths used in the application configuration will be searched. 
 * 		- '''env''': Reader of environmental and application external parameters
 * 		- '''AppName''': Name of the job to show in the spark monitoring tools
 * 		- '''ModelID''': Identifier of the job to be used in an operativization tool
 * 		- '''SessionID''': Snapshot of the execution to be used in an operativization tool
 * 		- '''Pipeline''': Purpose of the job (TRAINING, SCORING, TEST SCORING,...). To be used with logging.
 * 		- '''allow_checkpoints''': Enable/disable creation of checkpoints in large transformation pipelines
 */
trait Config {
  
  @transient lazy val log = Logger.getLogger(getClass.getName)

  var sc: org.apache.spark.SparkContext = _
  var sqlContext: org.apache.spark.sql.SQLContext = _
  var mailOptions: MailOptions = _
  var document: DocGenerator = _
  var dataManager: com.modelicious.in.data.DataManager = _ 
  var base: String = _
  var env: EnvReader = _
  var AppName: String = _
  var ModelID: String = _
  var SessionID: String = _
  var Pipeline: String = _
  var allow_checkpoints: Boolean = true
  
  val uuid = UUID.randomUUID().toString.takeRight(12)
  
  protected def setSparkConfSettingsWithDefaults(myconf: org.apache.spark.SparkConf, settings: scala.xml.NodeSeq) = {

    // Set defaults, will only be overriden if another value comes from the xml. Complete as needed
    var my_settings = scala.collection.mutable.Map(
      "spark.executor.memory" -> Constants.DEFAULT_EXECUTOR_MEMORY,
      "spark.executor.cores" -> Constants.DEFAULT_EXECUTOR_CORES,
      "spark.executor.instances" -> Constants.DEFAULT_EXECUTOR_INSTANCES,
      "spark.ui.showConsoleProgress" -> Constants.DEFAULT_SHOW_CONSOLE_PROGRESS,
      "spark.serializer" -> Constants.DEFAULT_SERIALIZER)

    settings.foreach { s => my_settings((s \ "@name").text) = s.text } // Update with replace

    log.file("Spark-Submit con settings : " + my_settings.mkString("[", ",", "]"))

    my_settings.foreach { case (k, v) => myconf.set(k, v.toString()) }

  }
  
  def getPath( path: String ) : String = FileUtils.getAbsolutePath( path, base )
  
  def stop
  
}

/**
 * Holds all Application related configuration and resources. 
 * 
 * 		- '''AppName''': Name of the job to show in the spark monitoring tools
 * 		- '''ModelID''': Identifier of the job to be used in an operativization tool
 * 		- '''SessionID''': Snapshot of the execution to be used in an operativization tool
 * 		- '''Pipeline''': Purpose of the job (TRAINING, SCORING, TEST SCORING,...). To be used with logging.
 * 		- '''allow_checkpoints''': Enable/disable creation of checkpoints in large transformation pipelines
 * 
 * @see [[com.modelicious.in.app.Config]]
 */

class AppConfig(xml_conf: scala.xml.NodeSeq, dm: com.modelicious.in.data.DataManager = new com.modelicious.in.data.DataManager()) extends Config {

  AppName = (xml_conf \ "@App").text
  ModelID = (xml_conf \ "@ModelID").textOrElse("ModelID")
  SessionID = (xml_conf \ "@SessionID").textOrElse("SessionID")
  Pipeline = (xml_conf \ "@Pipeline").textOrElse("SCORING")
  allow_checkpoints = (xml_conf \ "@AllowCheckpoints").textAsBoolOrElse(true)
  
  import scala.collection.JavaConversions._
  try {
    base = (xml_conf \ "@base").text
    log.file( "Base Folder in HDFS for application is set to: " + base )
  } catch {
    case e: Exception => { log.file( "Cannot continue without setting a base folder." ); System.exit(1)} 
  }
    
  val conf = new org.apache.spark.SparkConf()
    .setAppName(AppName)
    .setMaster(xml_conf \ "@Master" text)
    
  setSparkConfSettingsWithDefaults(conf, xml_conf \ "settings" \ "setting")

  sc = new org.apache.spark.SparkContext(conf)
  
  val checkPointFolder = xml_conf \ "@checkpointFolder"
  if( !checkPointFolder.isEmpty ) {
    val path = checkPointFolder.text
    sc.setCheckpointDir( getPath(path) )
    FileUtils.createFolder( getPath(path) )
    log.file( "Setting checkpoint dir: " + path )
  }
  
  
  val logLevel = (xml_conf \ "@logLevel").textOrElse( "INFO" )
  

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.toLevel(logLevel))
  
//  val sparkLogger = Logger.getLogger("log4j.logger.org.apache.spark")
//  sparkLogger.setLevel(Level.toLevel(logLevel))
//  val akkaLogger = Logger.getLogger("log4j.logger.Remoting")
//  akkaLogger.setLevel(Level.toLevel(logLevel))  
  
  sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  mailOptions = new MailOptions(
    (xml_conf \ "notifications" \ "enabled").textAsBoolOrElse(Constants.DEFAULT_SEND_MAIL_ENABLED),
    (xml_conf \ "notifications" \ "email").textOrElse("none"))

  document = new DocGenerator(
    (xml_conf \ "documentation" \ "enabled").textAsBoolOrElse(Constants.DEFAULT_GENERATE_DOCUMENTATION_ENABLED),
    (xml_conf \ "documentation" \ "format").textOrElse("latex"))
  
  dataManager = dm
  
  def stop = {   
    FileUtils.deleteFolder( getPath(uuid) )
//    try {
//      sc.stop()
//    } catch {
//      case e: Throwable => log.file(" Exception " + e.getMessage + " captured. Ignoring it.")
//    }
  }
  
}

/**
 * Object Application. 
 * @see [[com.modelicious.in.app.Config]]
 */
object Application {
  private var appConfig: Option[Config] = None
  private var seed: Option[Int] = None
  var stagesChannel = scala.collection.mutable.Map[String, String]()

  def init(config: Config) = {
    appConfig = Some(config)
  }

  def sc = get().sc

  def sqlContext = get().sqlContext

  def dataManager = get().dataManager

  def mailOptions = get().mailOptions
  
  def document = get().document

  def stop() = {
    if (appConfig.isDefined) {
      try {
        appConfig.get.stop
      }
      catch { // Hope to ignore lost executor errors
         case e: Throwable => {}
      }
    }
  }

  def reset() = {
    stop()
    appConfig = None
    seed = None
  }

  def get(): Config = { return appConfig.getOrElse(throw new RuntimeException("App not created yet!")) }

  def setSeed(seed: Option[Int]) = { this.seed = seed }
  def getNewSeed(): Int = seed.getOrElse(Random.nextInt())
}

//class ShellConfig(aSc: org.apache.spark.SparkContext, 
//                  aSqlContext: org.apache.spark.sql.SQLContext,
//                  aDm: com.modelicious.in.data.DataManager = new com.modelicious.in.data.DataManager()) extends Config {
//
//  sc = aSc
//  sqlContext = aSqlContext
//  dataManager = aDm 
//  
//  val checkPointFolder = xml_conf \ "@checkpointFolder"
//  if( !checkPointFolder.isEmpty ) {
//    val path = getPath( checkPointFolder.text )
//    sc.setCheckpointDir( path )
//    FileUtils.createFolder( path )
//    log.file( "Setting checkpoint dir: " + path )
//  }
//  
//  val logLevel = "INFO"
//  
//  val rootLogger = Logger.getRootLogger()
//  rootLogger.setLevel(Level.toLevel(logLevel))
//  
////  val sparkLogger = Logger.getLogger("log4j.logger.org.apache.spark")
////  sparkLogger.setLevel(Level.toLevel(logLevel))
////  val akkaLogger = Logger.getLogger("log4j.logger.Remoting")
////  akkaLogger.setLevel(Level.toLevel(logLevel))  
//  
//  mailOptions = new MailOptions(false, "none")
//
//  
//  def stop = sc.stop()
//  
//}

