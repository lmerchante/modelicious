package com.modelicious.in.web.controller

import org.scalatra._
import scala.xml._

class IndexController extends WebStack {
  
  get("/") { 
    contentType = "text/html"
    layoutTemplate("/result", "res" -> my_result) 
    }
  
  def my_result = scala.xml.Utility.trim( 
      XML.load( this.getClass().getClassLoader().getResourceAsStream("results_sample.xml")  )
      )
  
}