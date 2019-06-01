package com.modelicious.in

import com.modelicious.in.app.Config
import com.modelicious.in.h2o.H2OAppConfig

import scala.language.implicitConversions

/**
 * This package handles the config to interface with H2O Application.
*/
package object h2o {

  implicit def toH2OAppConfig( ac: Config ) = ac.asInstanceOf[ H2OAppConfig ]
    
}