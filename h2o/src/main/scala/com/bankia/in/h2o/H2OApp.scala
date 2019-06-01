package com.modelicious.in.h2o


import scala.xml._

import com.modelicious.in.BaseApp
import com.modelicious.in.app.Application


/**
  * The Application Object 
  *
  * @author Alberto Lago
  * @author Luis Sanchez
  * @version 1.0
  * @todo Add more functionality.
  * 
  */
object H2OApp extends BaseApp {

  override def Init: Unit = {
    super.Init
    H2OInit.initialize
  }
  
  override def AppInit ( xml_conf: scala.xml.Elem ) = {
    val appconfig = new H2OAppConfig(xml_conf)

    Application.init(appconfig)
  }
  
}