package com.modelicious.in.transform

import org.apache.spark.sql.DataFrame

import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.app.Application


case class sampling() extends TransformerState {
  var sampling_rate: Double = 1.0
}

class SamplingTransformModel( theState: TransformerState) extends DataTransformerModel[sampling](theState) {
  
  override def useForPrediction = false
  
  def doTransform( data: DataWrapper ): DataWrapper = {
    try {
      val sampling_rate: Double = state.sampling_rate
      log.file( "Applying sampling rate  " + sampling_rate )
      data { df => 
        
        val new_df = df.sample(false,sampling_rate,Application.getNewSeed())
        val count = new_df.count()
        rslt = <result> SamplingRate {sampling_rate} applied. AfterSamplingSize {count} </result>  
        new_df
      } 
      .addToMetaData( state.toXML )
    }
    catch
    {
      case e: Exception => { 
        log.file( "Error:" + e.getStackTraceString + "\n" +e.getMessage() )
        rslt = <result> Error: {e.getMessage()} </result>  
        data
      }
    }
  }
  
}

class SamplingTransform extends DataTransformer[sampling] {
  
  def fit( data: DataWrapper ): SamplingTransformModel = {
    new SamplingTransformModel( state )
  }

}


