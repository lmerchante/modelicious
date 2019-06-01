package com.modelicious.in.transform

import org.apache.spark.sql.DataFrame

import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._

case class mathT() extends TransformerState {
  var column: String = ""
  var transforms: List[String] = List()
  var drop_original: Boolean = true
}


class MathTransformModel(theState: TransformerState) extends DataTransformerModel[mathT](theState) {

  def doTransform( data: DataWrapper): DataWrapper = {
    try {
      data { df =>
        var new_df = df
        for( t <- state.transforms ) {
          log.file( "Applying math formula " +  t )
          new_df = new_df.withColumn( state.column + "_" + t, MathTransform.transforms( t )( state.column ) ) 
        }
        if ( state.drop_original ) {
         new_df = new_df.drop( state.column ) 
        }
        rslt = <result> state.toXML </result>
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

class MathTransform extends DataTransformer[mathT] {
  
  def fit( data: DataWrapper ): MathTransformModel = {
    if( !data.data.columns.contains(state.column) ) {
      throw new RuntimeException("[math] columns " + state.column + " does not esxist!") 
    }
    
    val allTransformsExist = state.transforms.forall { x => MathTransform.transforms.keySet.contains(x) }
    if( !allTransformsExist ) {
      throw new RuntimeException("[math] Please check the formulae!") 
    }
    
    new MathTransformModel( state )
  }

}


private object MathTransform {
  import org.apache.spark.sql.functions._
  
  // TODO -> should be easy to add all the functions by reflection
  val transforms = Map( 
    "exp" ->  ( ( s: String) => exp(s) ),
    "log2" ->  ( ( s: String) => log2(s) ),
    "log10" ->  ( ( s: String) => log10(s) ),
    "log" ->  ( ( s: String) => log(s) ),
    "log1p" ->  ( ( s: String) => log1p(s) ),
    "atan" ->  ( ( s: String) => atan(s) ),
    "acos" ->  ( ( s: String) => acos(s) ),
    "asin" ->  ( ( s: String) => asin(s) ),
    "pow2" ->  ( ( s: String) => pow(s, 2) ),
    "pow3" ->  ( ( s: String) => pow(s, 3) ),
    "sinh" ->  ( ( s: String) => sinh(s) ),
    "cosh" ->  ( ( s: String) => cosh(s) ),
    "tanh" ->  ( ( s: String) => tanh(s) ),
    "sin" ->  ( ( s: String) => sin(s) ),
    "cos" ->  ( ( s: String) => cos(s) ),
    "tan" ->  ( ( s: String) => tan(s) ),
    "sqrt" ->  ( ( s: String) => sqrt(s) ),
    "cbrt" ->  ( ( s: String) => cbrt(s) ),
    "sign" ->  ( ( s: String) => signum(s) ),
    "abs" ->  ( ( s: String) => abs(col(s)) )
  )
    
    
}