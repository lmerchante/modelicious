package com.modelicious.in.stage

import com.modelicious.in.app.Application
import com.modelicious.in.data.{DataManager, DataWrapper}
import com.modelicious.in.Constants
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools._
import com.modelicious.in.transform.OnesBalancerTransform

import org.apache.log4j.Logger
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.{BVectorIndexerModel => VectorIndexerModel}
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.DataFrame

import com.modelicious.in.modelos.predictor.{Predictor, Ensemble, VectorIndexedModel}


import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls

import scala.xml.transform.RuleTransformer


abstract class SelectPredictor[T <: StatsBase] extends Stage {

  
  
  def execute(conf: scala.xml.Elem): scala.xml.Elem = {
       
    implicit val sc = Application.sc
    implicit val sqlContext = Application.sqlContext

    var xml_out: Node = conf.asInstanceOf[Elem].copy()

    // Kfolds conf
    val training_rate = (conf \ "options" \ "training_rate").textAsDoubleOrElse(Constants.DEFAULT_TRAINING_RATE)
    val kfolds = (conf \ "options" \ "kfolds").textAsIntOrElse(Constants.DEFAULT_KFOLDS)
    if (kfolds == 0) log.file(s"Solo se realiza un entrenamiento con porcentaje: " + training_rate)
    else if (kfolds > 1) log.file(s"Se realiza valildacion cruzada con " + kfolds + " kfolds, es decir " + kfolds + " repeticiones con un porcentaje de training de: " + (kfolds - 1) * 100.0 / kfolds + "% cada una")
    else throw new RuntimeException("El valor de kfolds no es adecuado")

    val overfitting_splits = (conf \ "options" \ "overfitting_splits").textOrElse(Constants.DEFAULT_OVERFITTING_SPLITS).split(",").map(_.toDouble)
    log.file(s"Los splits para el análisis del overfiting son: " + overfitting_splits.toList.toString)
    if (overfitting_splits.size < 3) {
      log.file(s"Solamente " + overfitting_splits.size + " splits no ser suficientes para analizar el comportamiento de los modelos, elegir un mínimo de 3 splits: [0.1,0.5,1]")
      throw new RuntimeException("El número de overfitting_splits no es adecuado")
    }

    val modelos = (conf \ "models" \ "model").sortBy { x => (x \ "@id").contains( "Ensemble" ) }
    
    val saveLocal = (conf \ "options" \ "save" \ "local").textOrElse("")
    val saveRemote = (conf \ "options" \ "save" \ "remote").textOrElse("")

    val nCategories = (conf \ "options" \ "nCategories").textAsIntOrElse(0)
    
    val force_repartition = (conf \ "options" \ "force_repartition").textAsIntOrElse(0)
    
    log.file(s"El archivo de configuracion contiene ${modelos.size} modelos")

    val data_xml =  conf \ "data" \ "input"
    val in_uri = data_xml \ "@url" text

    val dw = Application.dataManager.read(in_uri)

    xml_out = new RuleTransformer(new AddChildrenTo(data_xml.head,  <columns>{dw.data.columns.mkString("[",",","]")}</columns>  )).transform(xml_out).head
    
    var data = SchemaTools.convertToVectorOfFeatures( dw.data )
    
    log.file( " Schema of Vector of features: " )
    log.file( data.schema.toString() )
    
    
    var vim: Option[VectorIndexerModel] = None
    
    // Esto debería desaparecer cuando se reentrenen todos los modelos de forest de ml
    if( nCategories > 0) {
      val indexed_data = SchemaTools.createCategoricMetadata(data, nCategories)
      data = indexed_data._1
      vim = Some( indexed_data._2 )
    }
    
    data = data.cache()
      
    log.file(s"Datos de Entrada: ")
    //my_RDD.collect().toList.foreach { x => log.file(x.toString())}
    val array_splits = if (kfolds == 0) Array(training_rate, 1 - training_rate) else Array.fill[Double](kfolds)(1.0 / kfolds)
    val splits = data.randomSplit(array_splits, seed = 11L)
    val list_train_folds = if (kfolds == 0) List(List(0)) else (0 to kfolds - 1).toSet[Int].subsets.map(_.toList).toList.filter(_.length == kfolds - 1)
    log.file(s"Numero de KFOLDS: " + splits.length)

    val learning_curves = new OverfittingTest()

    // iterate models
    for (m <- modelos) {
      try {
        val id = m \ "@id" text
        val name = m \ "@name" text
        val test_overfitting = m \ "@test_overfitting" text
        
        val model_conf = (m \\ "conf").lift(0).getOrElse(<conf/>).asInstanceOf[Elem]

        val ones_balancer = getBalancer( model_conf )
        Application.document.addSubSubSection("Training model: "+id+" with Name: "+name,"")
        Application.document.add("Configuration: ")
        Application.document.addXml(Right(model_conf))
        Application.document.add("Test overfitting enabled: "+test_overfitting)
        Application.document.add("Ones balancer per model use: "+ones_balancer.isDefined.toString)
        Application.document.add("Number of KFOLDS: " + splits.length)
            
        
        val model = getModel(id)
        // We are training, and the data is indexed. 
        // The vim is set back when the model is saved
        if( model.isInstanceOf[VectorIndexedModel]) {
          model.asInstanceOf[VectorIndexedModel].setModelIndexer(None)
        }
        log.file(s"    - Modelo ID: " + id + " Class: " + model.getClass + " Name: " + name + " Test Overfitting: " + test_overfitting)
        log.file(s"    - Parametros del modelo: " + model.configuration(model_conf))
        
        val cvstats = getCV
        
        for (index_train <- list_train_folds) {
          // iterate KFOLDS
          
          val emptyTrainData: DataFrame = sqlContext.createDataFrame( sc.emptyRDD[org.apache.spark.sql.Row], data.schema )
          val trainingDW = new DataWrapper(index_train.foldLeft(emptyTrainData)((a, b) => splits(b).unionAll(a) ) )
          val trainingData = if(ones_balancer.isDefined) 
            ones_balancer.get.fit(trainingDW).transform(trainingDW).data
          else 
            trainingDW.data
          
          val index_test = ((0 to splits.length - 1).toSet diff index_train.toSet).toList(0)
          val testData = splits(index_test)
          val train_size=trainingData.count()
          val test_size=testData.count()
          log.file(s"  * Train index splits: " + index_train.toString + " Size: " + train_size)
          log.file(s"  * Test index split: " + index_test.toString + " Size: " + test_size)
          Application.document.addItem("Train split: "+index_train.toString+" and Test split:"+index_test.toString)
          Application.document.add("Train size: "+train_size,1)
          Application.document.add("Test size: "+test_size,1)
          //log.file(s"    - Datos de Train: ")
          //trainingData.collect().toList.foreach { x => log_file(s"       "+x.toString())}
          //log.file(s"    - Datos de Test: ")
          //testData.collect().toList.foreach { x => log_file(s"       "+x.toString())}

          val t0 = System.nanoTime()
          log.file( " Running model training ")
          model.train(trainingData, model_conf )
          val t1 = System.nanoTime()
          log.file( " Model training finished: " + (t1 - t0)/ 1000000000.0 + " s elapsed" )
          log.file( " Model prediction" )
          val predictionAndLabel = model.predict(testData)
          val t2 = System.nanoTime()
          log.file( " Model prediction finished: " + (t2 - t1)/ 1000000000.0 + " s elapsed" )

          //log.file(s"    - Prediccion and Label: ")    
          //predictionAndLabel.collect().toList.foreach { x => log.file(s"      "+ x.toString())}
          
          predictionAndLabel.take(10).foreach( t => 
            log.file( "prediction: " + t._1 + ", label: " + t._2 )
          )

          log.file("  * Despues de prediccion: "  + predictionAndLabel.count() )
          
          val stats = computeStats(predictionAndLabel, testData.count(), (t2-t0)/ 1000000000.0).asInstanceOf[T] //  Stats.compute(predictionAndLabel, testData.count(), tiempo)
          log.file(s"    - Stats: " + stats.toStringWithFields)
          Application.document.addTable(stats.toMap ,"Table with Training Stats for model: "+id+" with Name: "+name)
          cvstats.append(stats)

          if (test_overfitting == "true") {
            log.file("OVERFITTING Test name: " + name + " id: " + id)
            log.file("OVERFITTING Train porcentaje: 100%. Count: " + trainingData.count())
            val ECM_test = 1.0 * predictionAndLabel.map(x => (x._1 - x._2) * (x._1 - x._2)).sum() / testData.count()
            val ECM_train = 1.0 * model.predict(trainingData).map(x => (x._1 - x._2) * (x._1 - x._2)).sum() / trainingData.count()
            learning_curves.append(m, 1, ECM_train, ECM_test)
            val curves = LearningCurve.compute(m, model, trainingData, testData, overfitting_splits)
            curves.map(x => learning_curves.append(m, x.porcentage, x.ECM_train, x.ECM_test))
          }
        } // for index_train

        val meanAndStd = cvstats.getStats
        log.file(meanAndStd.toStringWithFields)
        
        var result = <result><stats>{ meanAndStd.toXMLWithFields }</stats></result> 
        
        if (test_overfitting == "true") {
          val avgGraph = learning_curves.getAverageCurveForModel(m)
          log.file("OVERFITTING GRAPH in HTML: \n" + Graphs.plotGraphHTML(avgGraph))
          Application.document.addPlotLearningCurve(avgGraph.porcentages, avgGraph.ECM_train, avgGraph.ECM_test, avgGraph.ECM_train_std, avgGraph.ECM_test_std, "Learning Curves for model "+id+" - "+name)
          
          result = <result><stats>{ meanAndStd.toXMLWithFields }</stats><gd>{Graphs.graphData( avgGraph )}</gd></result>
          
        }
        xml_out = new RuleTransformer(new AddChildrenTo(m, result)).transform(xml_out).head

        model.transforms = dw.getMetaData()
        
        // Guardar el modelo que añade la metadata de categorías si es un modelo que lo utiliza
        // Y hubo que generarlo 
        if( model.isInstanceOf[VectorIndexedModel] && vim.isDefined ) {
          model.asInstanceOf[VectorIndexedModel].setModelIndexer(vim)
        }
        
        val save_suffix = "/models/" + id + "/" + name
        
        if( !saveRemote.isEmpty ) {
          val save_path = saveRemote + save_suffix
          log.file("Saving model " + name + " in HDFS to " + save_path)
          model.save(save_path)
        }
        if( !saveLocal.isEmpty ) {
          if( !saveRemote.isEmpty ) {
            val save_path = saveRemote + save_suffix
            // We already saved it remote, so just copy
            log.file("Copying model " + name + " from HDFS to local in " + save_path )
            FileUtils.copyToLocal( save_path, saveLocal+ save_suffix )  
          }
          else {
            // Its not saved in remote ( and we dont want it there) So copy to temp folder and then MOVE to local
            val remote_temp_save = Application.get().getPath( "tempModels" + save_suffix )
            log.file("Saving model " + name + " temporaly in HDFS to " + remote_temp_save )
            model.save(remote_temp_save )
            log.file("Saving model " + name + " to local system to " + saveLocal+ save_suffix)
            FileUtils.copyToLocal( remote_temp_save, saveLocal+ save_suffix, true )  
          }
        }
        
        // TODO: save only models, not ensembles
        addModelToEnsembleObject( name, model )
        
      
      } catch {
        case nse: java.util.NoSuchElementException => {
          val msg = s"Modelo ${(m \ "@id").text}:${(m \ "@name").text} no encontrado. " + nse.getMessage() + "\n" + nse.getStackTrace.mkString("\n")
          log.file(msg)
          val error = <error>{ msg }</error>
          xml_out = new RuleTransformer(new AddChildrenTo(m, error)).transform(xml_out).head
        }
        case e: Exception => {
          val msg = e.getStackTraceString + "\n" + e.getMessage()
          log.file("Error:" + msg)
          val error = <error>{ msg }</error>
          xml_out = new RuleTransformer(new AddChildrenTo(m, error)).transform(xml_out).head
        }
      }
    } // for modelos

    data.unpersist
    
    log.file("Loop ended")

    xml_out.asInstanceOf[Elem]
  }
  
  private def getBalancer( model: scala.xml.Node): Option[OnesBalancerTransform] = {
    val balanceo = model \ "ones_balancer"
    if( balanceo.isEmpty ) {
      return None
    }
    else
    {
       return Some( (new OnesBalancerTransform).configure(balanceo.head) )
    }
  }
  
  def getModel( id: String ) : Predictor
    
  def getCV : CrossValidator[T]
  
  def computeStats(rdd:RDD[(Double,Double)], n:Long, tiempo: Double= 0.0) : T
  
  def addModelToEnsembleObject( id: String, model: Predictor ): Unit
  
}