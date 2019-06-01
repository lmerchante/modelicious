package com.modelicious.in.stage

import com.modelicious.in.app.Application
import com.modelicious.in.data.{DataManager, DataWrapper}
import com.modelicious.in.modelos.predictor.Predictor
import com.modelicious.in.modelos.predictor.classifier.Classifier
import com.modelicious.in.modelos.predictor.scorers.Scorer
import com.modelicious.in.tools.{SchemaTools, PredictionStats, AddChildrenTo, FileUtils}
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.transform.DataTransformerModel
//import com.modelicious.in.Constants
//import com.modelicious.in.tools.Implicits._
import java.util.UUID

import scala.xml.transform.RuleTransformer

import org.apache.log4j.Logger

import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls

import org.apache.spark.sql.types.{LongType, StringType, DoubleType}

abstract class RunPredictor extends Stage {
  
  def execute(conf: scala.xml.Elem): scala.xml.Elem = {

    implicit val sc = Application.sc
    implicit val sqlContext = Application.sqlContext

    var xml_out: Node = conf.asInstanceOf[Elem].copy()
    
    val in_uri = conf \ "data" \ "input" \ "@url" text
    val out_uri = conf \ "data" \ "output" \ "@url" text
    
    var input_data = Application.dataManager.read(in_uri).cache
    
    val m =  (conf \ "model" ).head
    
    val model_path = m \ "@url" text

    val model = getModel(model_path)
    
    val transforms = model.transforms
    
     log.file( "Transforms associated to model " + transforms )
    
    val transformed_data = transformData( input_data, transforms ).data.cache()
    
    var featured_data = SchemaTools.convertToVectorOfFeatures( transformed_data )
      
    // cuando todos los modelos estén entrenados de nuevo, nos saltamos este paso, 
    import com.modelicious.in.modelos.predictor.MLTreesBase
    
    if( model.isInstanceOf[MLTreesBase[_,_]]  && !model.asInstanceOf[MLTreesBase[_,_] ].getModelIndexer.isDefined ) {
      log.file( " VectorIndexer for tree based Model is not loaded, so we generate it. " )
      val nCategories = (conf \ "model" \ "@nCategories").textAsIntOrElse(0)
      if( nCategories > 0 ) {
        val (d,  vim) = SchemaTools.createCategoricMetadata(featured_data, nCategories)
        featured_data = d
      }
      else {
        throw new RuntimeException( "Cannot run a Tree Based model that wasnt saved with a VectorIndexerModel if numCategories is not provided " )
      }
        
    }
    // ====================================
    featured_data.show()
    
    val balancing = (conf \ "model" \ "@balancing").textAsDoubleOrElse(-1.0)
        
    log.file( "Predict ")
    val predictionAndLabel = model.predict(featured_data)
    
    log.file( "Prediction finished. ")
    
    predictionAndLabel.take(10).foreach( t => 
      log.file( "prediction: " + t._1 + ", client: " + t._2 )
      )
    
    log.file( "Converting result to DataFrame ")
    
    val stats = PredictionStats.compute( predictionAndLabel )
    
    import sqlContext.implicits._
    var prd_df = predictionAndLabel.toDF( "prediction", "client" )
    
    if( balancing >= 0.0 ) {
      prd_df = prd_df.withColumn("efficiency", (prd_df( "prediction" )*balancing).cast(DoubleType).cast(StringType)   )
    }
    
    val pred = new DataWrapper( prd_df.withColumn( "client" , prd_df("client").cast(LongType).cast(StringType) ).withColumn( "prediction" , prd_df("prediction").cast(DoubleType).cast(StringType) ) )
    
    log.file( "Writing predictions to output uri: " + out_uri )
    
    var result = <result><stats>{ stats.toXMLWithFields }</stats></result>
        
    val resultseq: NodeSeq = NodeSeq.fromSeq( Seq( model.conf % Attribute(None, "URL", Text(model_path), Null) , result ) )
    
    xml_out = new RuleTransformer(new AddChildrenTo(m, resultseq)).transform(xml_out).head
    Application.document.addXml(Left(xml_out),1)
    Application.document.addSubSection("Stage Results: ","")
    Application.document.addTable(stats.toMap,"Table with prediction Stats")
    Application.document.addPlotDeciles(stats.toMap,"Plot deciles")
    Application.dataManager.write(out_uri, pred)
    
    xml_out.asInstanceOf[Elem] 
  }

  protected def transformData( dw: DataWrapper, transforms: scala.xml.Node ) : DataWrapper = {
    
    var data = dw 
    // TODO :Generalize the way the transforms are filtered here ( by adding an attribute to the configs) 
    transforms.child.filterNot( _.label == "#PCDATA" ).foreach { n => 
       log.file( "Applying transform " + n.label + " with Result ")
       Application.document.addItem( "Applying transform " + n.label )
       val t = DataTransformerModel( n )
       if( t.useForPrediction ) {
         data = t.transform( data )
         log.file( t.result.toString() )
         Application.document.addXml( Left(t.result) ,1)
         log.file( "Columns after transform: "  +  data.data.columns.mkString("[",",","]") ) 
       }
       else {
         log.file ( "Transformer " + n.label + " skipped as is marked to only use for training." )
         Application.document.add( "Transformer " + n.label + " skipped as is marked to only use for training." ,1)
       }
    }
    data
  }
  
  def getModel( path: String ) : Predictor 
  
}

class RunClassifier extends RunPredictor {
  def getModel( path: String ) : Predictor = {
    Classifier.load( path )
  }
}

class RunScorer extends RunPredictor {
  def getModel( path: String ) : Predictor = {
    Scorer.load( path )
  }
}
