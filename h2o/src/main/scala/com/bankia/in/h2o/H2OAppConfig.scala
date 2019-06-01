package com.modelicious.in.h2o

import com.modelicious.in.app.AppConfig
import com.modelicious.in.tools.Implicits._

import org.apache.spark.h2o.{H2OContext, H2OConf}

/**
 * Holds all Application related configuration and resources. Adding support for H2O 
 * 
 * @see com.modelicious.in.app.Config
 */
class H2OAppConfig(xml_conf: scala.xml.NodeSeq, dm: com.modelicious.in.data.DataManager = new com.modelicious.in.data.DataManager()) 
  extends AppConfig( xml_conf, dm ) {
  
  val h2o_conf = new H2OConf( sc.getConf )
  
  val xml_h2o_settings = (xml_conf \ "h2o-settings" \ "setting")
  
  val h2o_settings = xml_h2o_settings.map({ s => ((s \ "@name").text, s.text) })
  
  log.file("H2O-settings : " + h2o_settings.mkString("[", ",", "]"))
  
  h2o_settings.foreach { case (k, v) => h2o_conf.set(k, v.toString()) }
  
  val h2oc: org.apache.spark.h2o.H2OContext = H2OContext.getOrCreate(sc, h2o_conf)
  
  override def stop = {
    h2oc.stop(stopSparkContext= true)    
  }
  
}
