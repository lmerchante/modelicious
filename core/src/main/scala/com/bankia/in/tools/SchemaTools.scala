
import com.modelicious.in.app.Application
import com.modelicious.in.tools.Implicits._
import org.apache.log4j.Logger
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.feature.{VectorIndexer, BVectorIndexerModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Row}
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, IntegerType, LongType}
import com.fasterxml.jackson.databind.ObjectMapper

//
//import net.liftweb.json._
//import net.liftweb.json.JsonDSL._

package com.modelicious.in.tools {

object SchemaTools {
  
   @transient lazy val log = Logger.getLogger(getClass.getName)
   
   def DFtoLabeledRDD(df: DataFrame) : RDD[LabeledPoint] = {
    df.rdd.map{x=> 
      val label= x.toSeq(0).asInstanceOf[Number].doubleValue()
      val vector=Vectors.dense(x.toSeq.drop(1).toArray.map(_.asInstanceOf[Number].doubleValue()))
      LabeledPoint(label, vector)  
     }
   }
   
    
   def LabeledRDDtoDF(rdd: RDD[LabeledPoint], schema: StructType ) : DataFrame = {
     val sqlcontext  =Application.sqlContext 
     import sqlcontext.implicits._
     
     val rowRDD: RDD[Row] = rdd.map { l => Row.fromSeq( Array( l.label) ++ l.features.toArray ) }
     
     rowRDD.take(10).foreach( l=> log.file( l.mkString(", ") ) )
     
     sqlcontext.createDataFrame(rowRDD,schema)
   }
   
   def LabeledRDDtoVectorRDD(rdd: RDD[LabeledPoint]) : RDD[Vector] = {
    rdd.map{ x=> 
        x.features
    }
   }
   
   def DataFrametoVectorRDD(df: DataFrame) : RDD[Vector] = {
      df.rdd.map{x=> 
        Vectors.dense(x.toSeq.drop(1).toArray.map(_.asInstanceOf[Number].doubleValue()))
      }
   }
   
  // Returns a [label, feature1, feature2, ...., featureN] to a [ label, features] DataFrame
  // Features has metadata by ml_attr key
  def convertToVectorOfFeatures( input: DataFrame ) : DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols( input.columns.filter( _ != "label" ) )
      .setOutputCol("features")

    val output = assembler.transform(input)
    output.select("label", "features")
  }
  
  def DFWithVectorToLabeledRDD(df: DataFrame) : RDD[LabeledPoint] = {
//    println( "=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X" )
//    df.show()
//    println( "=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X=X" )
    df.rdd.map{x=> 
      val label= x.toSeq(0).asInstanceOf[Number].doubleValue()
      val vector=x.toSeq(1).asInstanceOf[Vector].toDense
      LabeledPoint(label, vector)  
    }
  }
   
  
//   def schemaToJson( schema: StructType ) : JValue = {
//    schema.map{ x => (
//          x.name ->
//              ( "dataType" -> x.dataType.typeName) ~
//              ( "nullable" -> x.nullable) ~
//              ( "metadata" -> parse( x.metadata.json ) )
//          )
//      }
//  }
//
//    // TODO: Mover a Tools
//  def schemaToJsonString( schema: StructType ) : String = {
//    pretty( JsonAST.render( schemaToJson( schema ) ) )
//  }
   
   def schemaToXML( schema: StructType, label: String = "schema" ) : scala.xml.Node = {
     <a> {
      schema.map{ x => 
        <column name={x.name} dataType={x.dataType.typeName} nullable={x.nullable.toString()}>{ scala.xml.Unparsed("<![CDATA[%s]]>".format(x.metadata.json)) }</column>
      }.toSeq }
			</a>.copy( label = label )
   }
   
   def schemaToXMLString( schema: StructType, label: String = "schema" ) : String = {
     val p = new scala.xml.PrettyPrinter(180, 2)
     p.format( schemaToXML( schema, label ) ) 
   }

   /**
    * Adds categorical metadata to schema as it is expected by ML Tree Algorithms.
    * It basically applies VectorIndexer Transformer with the given nCategories.
    * 
    * It expects a DF with a features DenseVector and returns a new DataFrame in which features has the needed metadata added. 
    * 
    * @return the transformed categorical data and the model used to do the transform
    */
   def createCategoricMetadata( input: DataFrame , nCategories: Int) = {
      val vim = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(nCategories)
        .fit(input)
        
        val bvim = new BVectorIndexerModel(vim)
        
        ( bvim.transform(input).drop("features").withColumnRenamed("indexedFeatures", "features"), bvim )
   }
   
   
   def getCategoricalFeatures(featuresSchema: StructField): Map[Int, Int] = {
     org.apache.spark.ml.utils.MetadataUtilsProxy.getCategoricalFeatures(featuresSchema)
   }
   
   def MetadataToColumnList( md: String ): List[String] = {
     
     val mapper = new ObjectMapper();
     val jsonObj = mapper.readTree(md);
     
     
     val indexColumnsMap = scala.collection.mutable.Map[Int, String]()

     import scala.collection.JavaConversions._
     
     jsonObj
       //.get("ml_attr")
       .get("attrs")
       .elements
       .toList
       .flatten
       .map{ j => (j.get("idx").asInt, j.get("name").asText) }
       .sortBy(_._1)
       .map{ _._2 }
       .toList
     
   }
   
}

}

// We want to use the same method that ml uses internally to transform vectorindexer metadata into categorical features
package org.apache.spark.ml.utils {
  object MetadataUtilsProxy {
    /**
   * Examine a schema to identify categorical (Binary and Nominal) features.
   *
   * @param featuresSchema  Schema of the features column.
   *                        If a feature does not have metadata, it is assumed to be continuous.
   *                        If a feature is Nominal, then it must have the number of values
   *                        specified.
   * @return  Map: feature index to number of categories.
   *          The map's set of keys will be the set of categorical feature indices.
   */
  def getCategoricalFeatures(featuresSchema: StructField): Map[Int, Int] = 
    org.apache.spark.ml.util.MetadataUtils.getCategoricalFeatures(featuresSchema)
  
  }
}

