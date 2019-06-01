package com.modelicious.in

import org.apache.log4j.Logger
import com.modelicious.in.tools.Implicits._

package object stage {

//  @transient lazy val log = Logger.getLogger(getClass.getName)
//  
//  object SaveOpts {
//    sealed trait SaveVal
//    case object Nope extends SaveVal
//    case object Local extends SaveVal
//    case object Remote extends SaveVal
//    case object Both extends SaveVal
//    
//    val saveOptions = Seq(Nope, Local, Remote, Both)
//    
//    def apply( opt: String ): SaveVal = opt.trim.toLowerCase match {
//      case "Nope" => Nope 
//      case "Local" => Local
//      case "Remote" => Remote
//      case "Both" => Both 
//      // Default to Nope, but logging a warning
//      case _ => { log.file( "[WARN] Invalid Save Option " + opt + ". Defaulting to not save" ); Nope}
//    }
//    
//  }

}