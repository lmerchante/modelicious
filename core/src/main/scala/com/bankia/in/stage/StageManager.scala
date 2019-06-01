package com.modelicious.in.stage

import org.apache.log4j.MDC

object StageManager {
  
  type StageCreator = () => Stage
  
  private val stages = Map[String, StageCreator] (
    "SelectClassifier"   -> (() => new SelectClassifier),
    "SelectScorer"       -> (() => new SelectScorer),
    "PrepareData"        -> (() => new PrepareData) ,
    "CompareData"        -> (() => new CompareData) ,    
		"LoadData"           -> (() => new LoadData),
		"RunSQL"             -> (() => new RunSQL),
		"RunClassifier"      -> (() => new RunClassifier), 
		"RunScorer"          -> (() => new RunScorer) /*,
		"TrainCluster" -> new TrainCluster ,
		"PredictClassifier" -> new PredictClassifier
		// .....
    */
  )
  
  // INPUT, FE, LOAD MODEL, SCORING, OUTPUT
  
  private val log_names = Map[String, String] (
    "SelectClassifier"   -> "TRAIN",
    "SelectScorer"       -> "TRAIN",
    "PrepareData"        -> "FE" ,
		"LoadData"           -> "INPUT",
		"RunSQL"             -> "INPUT",
		"RunClassifier"      -> "SCORING",
	  "RunScorer"          -> "SCORING",
	  "CompareData"        -> "ASSESSMENT" 
  )
  
  def apply( conf: scala.xml.Elem  ) : Stage = {
    val root = conf.head.label
    
    MDC.put("Phase", log_names(conf.head.label) )
    stages(root)()
  }
  
  def getStageIndex( stage_name: String ): Int = { stages.keys.toList.zipWithIndex.filter{ _._1 == stage_name }(0)._2 }
  
  
}