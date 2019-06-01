package com.modelicious.in.app

import com.modelicious.in.tools.Implicits._
import com.modelicious.in.Constants
import com.modelicious.in.data.DataManager
import com.modelicious.in.tools.EnvReader

import org.apache.log4j.Logger
//import org.apache.spark.h2o._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.language.reflectiveCalls
import scala.language.postfixOps
import org.apache.spark.sql.hive.HiveContext


class TestAppConfig(xml_conf: scala.xml.NodeSeq, dm: com.modelicious.in.data.DataManager = new com.modelicious.in.data.DataManager() ) extends Config {
  
  
  
  ModelID = "TestModel"
  SessionID = "TestSession"
  env= new EnvReader()
  
  val conf = new org.apache.spark.SparkConf()
    .setAppName(xml_conf \ "@App" text)
    .setMaster(xml_conf \ "@Master" text)

  setSparkConfSettingsWithDefaults(conf, xml_conf \ "settings" \ "setting")

  sc = new org.apache.spark.SparkContext(conf)
  sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //hc = H2OContext.getOrCreate(sc)

  mailOptions = new MailOptions( 
      (xml_conf \ "notifications" \ "enabled").textAsBoolOrElse(Constants.DEFAULT_SEND_MAIL_ENABLED),
      (xml_conf \ "notifications" \ "email").textOrElse("none")
      )

  dataManager = dm
  
  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  
  def stop = sc.stop()
}