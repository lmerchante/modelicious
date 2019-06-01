package com.modelicious.in.transform

import org.apache.spark.sql.DataFrame

import com.modelicious.in.app.Application
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._

case class save() extends TransformerState

class SaveTransformModel(theState: TransformerState) extends DataTransformerModel[save](theState) {

  def doTransform( data: DataWrapper): DataWrapper = {
    try {
      val url = state.attributes("url")
      log.file( "Saving Data to " + url )
      
      Application.dataManager.write(url, data)
      rslt = <result>Data saved to {url}</result>
      
      data
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

class SaveTransform extends DataTransformer[save] {
  
  def fit( data: DataWrapper ): SaveTransformModel = {
    try {
      state.attributes("url")
    }
    catch
    {
      case e: NoSuchElementException => { 
        log.file( "Error SaveTransform: Invalid configuration." )
        throw e
      }
    }
    new SaveTransformModel( state )
  }

}


