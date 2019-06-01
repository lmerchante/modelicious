package test.scala.h2o

import com.modelicious.in.tools.Implicits._
import com.modelicious.in.Constants
import com.modelicious.in.data.DataManager

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.h2o.{H2OContext, H2OConf}

import scala.language.reflectiveCalls
import scala.language.postfixOps


class TestH2OConfig(xml_conf: scala.xml.NodeSeq, dm: com.modelicious.in.data.DataManager = new com.modelicious.in.data.DataManager() ) extends com.modelicious.in.h2o.H2OAppConfig(xml_conf,dm) {
  
}