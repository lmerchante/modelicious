package com.modelicious.in.modelos.predictor

import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, IntegerType, MetadataBuilder}

import com.modelicious.in.tools.Implicits._

import scala.language.reflectiveCalls

import scala.reflect.runtime.{ universe => ru }

trait SVMBase extends ModeloMLLIB {

  type T = SVMModel
  val tag = ru.typeTag[SVMModel]

  val name = "SVMBase"

  def doTrain(input: RDD[LabeledPoint], vector_schema: StructType, conf: scala.xml.Elem ) = {

    val numIterations = (conf \\ "numIterations").textAsIntOrElse(100)
    val regParam = (conf \\ "regParam").textAsDoubleOrElse(1)
    val regType = (conf \\ "regType").textOrElse("L2")

    val svmAlg = new SVMWithSGD()
    svmAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
    if (regType == "L1") svmAlg.optimizer.setUpdater(new L1Updater)
    modelo = Some(svmAlg.run(input))
  }

  def configuration(conf: scala.xml.Node): String = {
    val numIterations = (conf \\ "numIterations").textAsIntOrElse(100)
    val regParam = (conf \\ "regParam").textAsDoubleOrElse(1)
    val regType = (conf \\ "regType").textOrElse("L2")
    return "numIterations: " + numIterations + "; regParam: " + regParam + "; regType: " + regType
  }
  
}