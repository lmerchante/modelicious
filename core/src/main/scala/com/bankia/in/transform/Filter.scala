package com.modelicious.in.transform

import org.apache.spark.sql.DataFrame

import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._


case class filter() extends TransformerState {
  var condicion: String = ""
}

class FilterTransformModel( theState: TransformerState) extends DataTransformerModel[filter](theState) {
  
  def doTransform( data: DataWrapper ): DataWrapper = {
    try {
      val condicion: String = state.condicion
      log.file( "Applying filter  " + condicion )
      data { df => 
        
        val new_df = df.filter( condicion ) 
        rslt = <result> Filter {condicion} applied </result>  
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
  override def useForPrediction = false
}

class FilterTransform extends DataTransformer[filter] {
  
  def fit( data: DataWrapper ): FilterTransformModel = {
    new FilterTransformModel( state )
  }

}


