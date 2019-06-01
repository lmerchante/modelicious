package com.modelicious.in.test.tools

import com.modelicious.in.tools.{PredictionStats, PredictionStatistics}
import org.scalatest._
import test.scala.modelos.predictor.TestModelosBase

import com.modelicious.in.app.{Application, TestAppConfig}

class PredictionStatsTest extends TestModelosBase {

 
  
  "PredictionStats" should " return a jsonString" in {
    val data = ds
    
    val v = PredictionStats.compute(data)

    print(v.toJson)
  }
  

  def ds =  Application.sc.parallelize(Seq(
     (0.17,1.0),
     (0.23,2.0),
     (0.39,3.0),
     (0.40,4.0),
     (0.35,5.0),
     (0.89,6.0),
     (0.34,7.0),
     (0.62,8.0),
     (0.98,9.0),
     (0.71,10.0),
     (0.15,11.0),
     (0.12,12.0),
     (0.26,13.0),
     (0.77,14.0),
     (0.83,15.0),
     (0.42,16.0),
     (0.55,17.0),
     (0.16,18.0)
    )
  )
  

  
}