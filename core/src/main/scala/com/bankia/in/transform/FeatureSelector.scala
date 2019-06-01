package com.modelicious.in.transform

import com.modelicious.in.app.Application
import com.modelicious.in.Constants
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.SchemaTools

import org.apache.log4j.Logger
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.{InfoThCriterionFactory,InfoThSelector, InfoThSelectorModel} 
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Row,Column}
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, IntegerType}
import org.apache.spark.ml.feature.{ BQuantileDiscretizer => QuantileDiscretizer, Bucketizer }


import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls

case class feature_selection() extends TransformerState{
   var method: String = "mifs"
   var num_features: Int = 10
   var num_partitions: Int = 100
   var selected_features: List[String] = List() // TODO: Sería mejor quedarse con los nombres
}


/**
 * Feature selector based on Work by https://github.com/sramirez/spark-infotheoretic-feature-selection
 */
class FeatureSelectorTransformModel(theState: TransformerState) extends DataTransformerModel[feature_selection](theState) {

  def doTransform( data: DataWrapper): DataWrapper = {

   // Recibe un DataFrame y un bloque que recibe el RDD[LabeledPoint] y su schema y devuelve el RDD[LabeledPoint] y schema tras la transformacion 
   var selected_schema = new StructType()
   withRDDAndVaryingSchema(data){ (rdd, schema) =>
     val schemaWithoutLabel = schema.drop(1)
     
     log.file( "indices de variables seleccionadas: " + schemaWithoutLabel.zipWithIndex.mkString(",") )
     
     val indexes_of_selected_features = schemaWithoutLabel.zipWithIndex
         .filter{ case(column, index) => state.selected_features.contains(column.name) }
         .map{ case(column, index) => index }.toArray
         
     log.file( "indices de variables seleccionadas: " + indexes_of_selected_features.mkString(",") )
     val featureSelector = new InfoThSelectorModel( indexes_of_selected_features )
     selected_schema = new StructType( Array(schema("label")) ++ indexes_of_selected_features.map( i => schemaWithoutLabel(i) ) )
          
      rslt = <selectedFeatures>{ SchemaTools.schemaToXML( selected_schema )}</selectedFeatures>  
      ( rdd.map(i => LabeledPoint(i.label, featureSelector.transform(i.features))), selected_schema)
    }.addToMetaData( state.toXML ) 
  } 
  
}

class FeatureSelectorTransform extends DataTransformer[feature_selection] {

  override def fit( data: DataWrapper ) = {
    
    val schema = data.schema
    val rdd = SchemaTools.DFtoLabeledRDD(data.data)
 
    val criterion = new InfoThCriterionFactory(state.method)
      
    log.file(s"Seleccion de variable ACTIVA con metodo: "+state.method+" Numero de variables: "+state.num_features+" Y Particiones: "+state.num_partitions)
    
    val data_discretized= prepara_datos_para_seleccion_variable(rdd)
    //data_discretized.collect().toList.foreach { x => log.file(x.toString())}
    val featureSelector = new InfoThSelector(criterion, state.num_features, state.num_partitions).fit(data_discretized)
    
    val schemaWithoutLabel = schema.drop(1)
    state.selected_features= featureSelector.selectedFeatures.map{ i => schemaWithoutLabel(i).name }.toList
    //
    log.file(s"Variables seleccionadas: "+state.selected_features.toString())
      
    //val schemaWithoutLabel = schema.drop(1)  
    //state.selected_schema = selected_features.map( i => schemaWithoutLabel(i).name ).toList
    new FeatureSelectorTransformModel( state )
  }
  
  protected def prepara_datos_para_seleccion_variable(data: RDD[LabeledPoint]) : RDD[LabeledPoint] =  {
    // discretiza los datos de entrada por columnas
    val rddRow=data.map(x=> Row.fromSeq(Array(x.label) ++ x.features.toArray))
    val schema = StructType((1 to rddRow.first.size).map(i => StructField(s"val_$i", DoubleType, false)))
    var df=Application.sqlContext.createDataFrame(rddRow, schema)
    df.cache()
    val columns = df.columns
    val df_length = df.count() 
    for (i <- columns) {
      var discretizer = new QuantileDiscretizer()
      .setInputCol(i)
      .setOutputCol(i+"_cat")
      .setNumBuckets(127)
      //var result = discretizer.fit(df).transform(df)
      var result = discretizer.fitfast(df,df_length).transform(df)
      df=result.drop(i)
    }
    val rdd_quantified = df.rdd.map{x=> 
      val label= x.toSeq(0).asInstanceOf[Number].doubleValue()
      val vector=Vectors.dense(x.toSeq.drop(1).toArray.map(_.asInstanceOf[Number].doubleValue()))
      LabeledPoint(label, vector)  
    }
    df.unpersist
    rdd_quantified
  }
  
}
