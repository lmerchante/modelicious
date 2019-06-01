package com.modelicious.in.transform

import org.apache.spark.sql.DataFrame

import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._

case class drop() extends TransformerState {
  var columna: String = ""
}


class DropTransformModel(theState: TransformerState) extends DataTransformerModel[drop](theState) {

  def doTransform( data: DataWrapper): DataWrapper = {
    try {
      val columna = state.columna
      log.file( "Dropping column " + columna )
      data { df => 
        val new_df = df.drop( columna ) 
        rslt = <result> Column {columna} dropped </result>
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

class DropTransform extends DataTransformer[drop] {
  
  def fit( data: DataWrapper ): DropTransformModel = {
    new DropTransformModel( state )
  }

}


