package com.modelicious.in.stage

import com.modelicious.in.Constants
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.AddChildrenTo

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame


import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls
import scala.xml.transform.RuleTransformer

import com.modelicious.in.app.Application
import com.modelicious.in.transform._

import com.modelicious.in.data.DataManager

class PrepareData  extends Stage
{
  
  def execute( conf: scala.xml.Elem ): scala.xml.Elem = {
   
    implicit val sc = Application.sc
    implicit val sqlContext = Application.sqlContext
    
    val in_uri = conf \ "data" \ "input" \ "@url" text
    val out_uri = conf \ "data" \ "output" \ "@url" text
    
    var input_data = Application.dataManager.read(in_uri)
   
    var xml_out: Node = conf.asInstanceOf[Elem].copy()
    
    val transformations = (conf \ "transformations").head.child.filter { e => !e.toString.trim.isEmpty }
    
    log.file( transformations.toString() )
    
    transformations.foreach { t =>
      val transformer = DataTransformer(t)
      
      log.file( "Applying Transformer " + transformer.getClass + " with configuration: " + t.toString() ) 
      
      val transformModel = transformer.configure(t).fit(input_data )
      input_data = transformModel.transform(input_data)
      //input_data.unpersist
      //input_data = new_data
      //input_data.cache
      
      log.file( "Tranformer " + transformer.getClass + " ended with result " + transformModel.result.toString())
      
      xml_out = new RuleTransformer(new AddChildrenTo(t, transformModel.result )).transform(xml_out).head
    }
    println(">>>>>>>>>>>"+input_data.schema.toString())
    Application.dataManager.write(out_uri, input_data)

    xml_out.asInstanceOf[scala.xml.Elem]
   
  }
  
  
  
}