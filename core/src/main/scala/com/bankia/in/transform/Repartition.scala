package com.modelicious.in.transform

import org.apache.spark.sql.DataFrame

import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._

case class repartition() extends TransformerState {
  var num_partitions: Int = 0
}


// TODO: Should throw?
class RepartitionTransformModel(theState: TransformerState) extends DataTransformerModel[repartition](theState) {

  def doTransform( data: DataWrapper): DataWrapper = {
    try {
      if( state.num_partitions <= 0 ) {
        rslt = <result>{"WARNING: Repartition Transform with state " + state.toStringWithFields + " was be ignored. A valid number of partitions must be set"}</result>
        return data
      }
      else{
        rslt = <result>{state.toStringWithFields}</result>
        return data.repartition( state.num_partitions )
      }
      
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

class RepartitionTransform extends DataTransformer[repartition] {
  
  def fit( data: DataWrapper ): RepartitionTransformModel = {
    if( state.num_partitions <= 0 ) {
      log.file( "WARNING: Repartition Transform with state " + state.toStringWithFields + " will be ignored. A valid number of partitions must be set "  ) 
    }
    new RepartitionTransformModel( state )
  }

}


