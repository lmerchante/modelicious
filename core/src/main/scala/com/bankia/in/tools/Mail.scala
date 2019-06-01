package com.modelicious.in.tools

import scala.sys.process._
import com.modelicious.in.tools.Implicits._


object Mail {
  // Solo funcionara en aquellos puestos virtuales en los que se pueda configurar una cuenta de correo corporativa
  // por lo tanto no tiene sentido usar esta libreria en cluster-mode
  
  def send_mail(log:  org.apache.log4j.Logger, receiver: String, subject: String, xml: String, html: String) =
  {
    // TO DO: Ver si conseguimos enviar un fichero adjunto y enviar el XML de salida
      val script_stream  = this.getClass().getClassLoader().getResourceAsStream("send_mail.py") 
      val arg=" \""+receiver+"\" "+" \""+subject+"\" "+" \""+xml+"\"" +" \""+html+"\""
      val thread = new Thread {
        override def run { 
          val codigoretorno: Int = (s"python - $arg" #< script_stream).!
          if (codigoretorno==0) 
            log.file( s"Correo enviado a: "+receiver+": OK") 
          else 
            log.file( s"Correo enviado a: "+receiver+": Failed")
        }
      }
      thread.start()
  }

}
