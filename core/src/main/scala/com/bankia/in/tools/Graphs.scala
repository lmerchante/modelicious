package com.modelicious.in.tools

import org.apache.log4j.Logger
import com.modelicious.in.tools.Implicits._

object Graphs {

  def container(data:String):String =
    """
    <div id="container" style="min-width: 400px; height: 400px; margin: 0 auto"></div>
    <script type="text/javascript">
        $(function() {
            $('#container').highcharts(		"""+data+"""  );
        });
    </script>
    """.stripMargin
    
  def reloadJs =
    """
      |<script type="text/javascript">$.ajax({url: '/check', dataType: 'jsonp', complete: function(){location.reload()}})</script>
    """.stripMargin

  val wispJsImports: String =
    """
      |<script type="text/javascript" src="http://code.jquery.com/jquery-1.8.2.min.js"></script>
      |<script type="text/javascript" src="http://code.highcharts.com/4.0.4/highcharts.js"></script>
      |<script type="text/javascript" src="http://code.highcharts.com/4.0.4/modules/exporting.js"></script>
      |<script type="text/javascript" src="http://code.highcharts.com/4.0.4/highcharts-more.js"></script>
    """.stripMargin

  val jsHeader =
    """
      |<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
      |<html>
      |  <head>
      |    <title>
      |      Highchart
      |    </title>
      |    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    """.stripMargin + wispJsImports
    
  val plotoptions="\"exporting\":{\"filename\":\"chart\"},\"yAxis\":[{\"title\":{\"text\":\"MSError\"}}],\"plotOptions\":{\"line\":{\"turboThreshold\":0}},\"credits\":{\"href\":\"\",\"text\":\"Overfitting Learning Curves\"},\"chart\":{\"zoomType\":\"xy\"},\"title\":{\"text\":\"\"},\"xAxis\":[{\"title\":{\"text\":\"Training Set Size\"}}]"

  def graphData(avgGraph: CurveGraph): String = {
    @transient lazy val log = Logger.getLogger(getClass.getName)
    val index=avgGraph.porcentages
    val msetrain=avgGraph.ECM_train
    val msetrainstd=avgGraph.ECM_train_std
    val msetest=avgGraph.ECM_test
    val mseteststd=avgGraph.ECM_test_std
    log.file("OVERFITTING GRAPH: Index:" + index.mkString(","))
    log.file("OVERFITTING GRAPH: Train:" + msetrain.mkString(","))
    log.file("OVERFITTING GRAPH: Test:" + msetest.mkString(","))
    log.file("OVERFITTING GRAPH: Train Std Dev:" + msetrainstd.mkString(","))
    log.file("OVERFITTING GRAPH: Test Std Dev:" + mseteststd.mkString(","))
    val graph_train= "{\"data\":["+index.zip(msetrain).map(x=>"["+x._1+","+x._2+"]").mkString(",")+"],\"name\":\"Training MSE\",\"type\":\"spline\"}"
    val graph_errortrain= "{\"data\":["+index.zip(msetrain.zip(msetrainstd).map(x=> (x._1-x._2,x._1+x._2))).map(x=> "["+x._1+","+x._2._1+","+x._2._2+"]").toList.mkString(",")+"],\"name\":\"Margin Training MSE\",\"type\":\"errorbar\"}"
    val graph_test= "{\"data\":["+index.zip(msetest).map(x=>"["+x._1+","+x._2+"]").mkString(",")+"],\"name\":\"Test MSE\",\"type\":\"spline\"}"
    val graph_errortest= "{\"data\":["+index.zip(msetest.zip(mseteststd).map(x=> (x._1-x._2,x._1+x._2))).map(x=> "["+x._1+","+x._2._1+","+x._2._2+"]").toList.mkString(",")+"],\"name\":\"Margin Test MSE\",\"type\":\"errorbar\"}"
    "{\"series\":["+graph_train+","+graph_errortrain+","+graph_test+","+graph_errortest+"],"+plotoptions+"}"
  }

  def plotGraphHTML(avgGraph: CurveGraph): String = {
    buildHtmlFile(graphData( avgGraph ) )
  }
  
  def buildHtmlFile(data: String): String = {
    val sb = new StringBuilder()
    sb.append(jsHeader)
    //sb.append(reloadJs)
    sb.append("</head>")
    sb.append("<body>")
    sb.append(container(data))
    sb.append("</body>")
    sb.append("</html>")
    sb.toString()
  }
} //Graphs


