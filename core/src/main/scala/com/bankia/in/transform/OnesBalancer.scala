package com.modelicious.in.transform

import org.apache.spark.sql.DataFrame

import com.modelicious.in.app.Application
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._


private object Contants {
  val kONES : String = "label = 1.0"
  val kZEROS: String = "label = 0.0"
}

case class ones_balancer() extends TransformerState {
  var ratio: Double = 1.0
  var tolerance: Double = 0.05
  var zeros_sample_ratio: Double = 1.0
  var ones_sample_ratio: Double = 1.0
}


class OnesBalancerTransformModel(theState: TransformerState) extends DataTransformerModel[ones_balancer](theState) {

  // TODO: Sampling DataFrames is not exact, but deterministic
  // Posible solution, that coud be not deterministic
  // https://stackoverflow.com/questions/37416825/dataframe-sample-in-apache-spark-scala
  
  def doTransform( data: DataWrapper): DataWrapper = {
    try {
      val ratio = state.ratio
      val tolerance = state.tolerance
      
      log.file( "Balancing Dataframe to have a " + ratio  + " 1 to 0 ratio ")
      
      data { df => 
        var df_ones = df.filter( Contants.kONES )
        var df_zeros = df.filter( Contants.kZEROS )
        
        if( state.ones_sample_ratio != 1.0 ) {
           df_ones = df_ones.sample( false, state.ones_sample_ratio, Application.getNewSeed() )
        }
        else if ( state.zeros_sample_ratio != 1.0 ) {
           df_zeros = df_zeros.sample( false, state.zeros_sample_ratio, Application.getNewSeed() )
        }
        
        val ones_count = df_ones.count
        val zeros_count = df_zeros.count
        
        rslt = <result><zeros>zeros_count</zeros><ones>ones_count</ones></result>
        log.file("Ones Balancer result:\tZeros: " + zeros_count + "\tOnes: " + ones_count)
        df_zeros.unionAll( df_ones )
      }
      .addToMetaDataCommented( state.toXML )
      
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

class OnesBalancerTransform extends DataTransformer[ones_balancer] {
  
  override def configure(conf: scala.xml.NodeSeq): this.type = {
    state = TransformerState.fromXML[ones_balancer](conf.asInstanceOf[scala.xml.Node])
    if (state.tolerance < 0.0) {
      log.file(s"No es posible una tolerancia negativa, se configura a 0.0")
      state.tolerance = 0.0
    }
    if (state.tolerance > 0.5) {
      log.file(s"No es posible una tolerancia mayor del 50%, se configura a 0.5")
      state.tolerance = 0.5
    }
    
    this
  }
  
  def fit( data: DataWrapper ): OnesBalancerTransformModel = {
    val df = data.data
    val total = df.count()
    val ones = df.filter( Contants.kONES ).cache.count()
    // zeros = total - ones ? 
    val zeros = df.filter( Contants.kZEROS ).cache.count()
    var ones_sample_ratio = 1.0
    var zeros_sample_ratio = 1.0
    val desired_ratio = state.ratio
    val tolerance = state.tolerance
      
    val original_ratio = ones.toDouble / zeros.toDouble
    if( original_ratio > desired_ratio * ( 1.0 + tolerance ) ) {
      ones_sample_ratio = desired_ratio / original_ratio 
    } 
    else if ( ( original_ratio < desired_ratio * ( 1.0 - tolerance ) ) ) {
      zeros_sample_ratio = original_ratio / desired_ratio
    }
    state.ones_sample_ratio =  ones_sample_ratio
    state.zeros_sample_ratio = zeros_sample_ratio
      // else, we are on the desired ratio, do nothing
    
    log.file( "Creating OnesBalancerTransformModel with " + state.toStringWithFields )
    log.file( "Ones Balancer Original count: " + total )
    new OnesBalancerTransformModel( state )
  }

}


