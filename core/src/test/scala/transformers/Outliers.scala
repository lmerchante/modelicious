package test.scala.transformers

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, IntegerType, MetadataBuilder}

import org.scalatest._
import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls

import scala.language.reflectiveCalls
import scala.language.postfixOps

import com.modelicious.in.transform._

 
import com.modelicious.in.app.{Application, TestAppConfig}
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Logs

class OutliersTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local"
  private val appName = "test-memory"
  
  com.modelicious.in.Init.initialize
  //Logs.config_logger(getClass.getName, "test.log")
  
  override def beforeAll {
    val conf = <spark App={appName} Master={master} logLevel="ERROR"></spark>
    val appconfig = new TestAppConfig(conf)
    
    Application.init(appconfig)
    Application.setSeed( Some(11) )
    
  }

   override def afterAll {
    Application.reset()
        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
     System.clearProperty("spark.driver.port")
  }
  
  "Remove Outliers " should " remove all items that exceed 3*std in one column in single column DF" in {
    val conf= <remove_outliers><columns>number</columns><times>3.0</times>
<max_percentage_of_outliers>10</max_percentage_of_outliers></remove_outliers>
    val my_transform = new RemoveOutliers 
    var my_dw = getSingleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw )
    out.data.count() should be (10L)
  }
   
   it should " remove all items that exceed 3*std in one column in multicolumn DF" in {
    val conf= <remove_outliers><columns>dim1</columns><times>3.0</times>
<max_percentage_of_outliers>10</max_percentage_of_outliers></remove_outliers>
    val my_transform = new RemoveOutliers
    var my_dw = getMultipleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw )
    out.data.count() should be (10L)
  }
  
  it should " remove all items that exceed 3*std in all columns in multicolumn DF" in {
    val conf= <remove_outliers><columns>all</columns><times>3.0</times>
		<max_percentage_of_outliers>10</max_percentage_of_outliers></remove_outliers>
    val my_transform = new RemoveOutliers
    var my_dw = getMultipleDW()
    val out = my_transform.exec(conf, my_dw)
    out.data.count() should be (8L)
  }
  
  it should " not remove items when we exceed the max percentage for outliers that defaults to 0.75" in {
    val conf= <remove_outliers><columns>all</columns><times>3.0</times></remove_outliers>
    val my_transform = new RemoveOutliers
    var my_dw = getMultipleDW()
    val out = my_transform.exec(conf, my_dw)
    out.data.count() should be (11L)
  }
  
    "Bulk mode Remove Outliers " should " remove all items that exceed 3*std in one column in single column DF" in {
    val conf= <remove_outliers><columns>number</columns><times>3.0</times>
<max_percentage_of_outliers>10</max_percentage_of_outliers><bulk>true</bulk></remove_outliers>
    val my_transform = new RemoveOutliers 
    var my_dw = getSingleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw )
    out.data.count() should be (10L)
  }
   
   it should " remove all items that exceed 3*std in one column in multicolumn DF" in {
    val conf= <remove_outliers><columns>dim1</columns><times>3.0</times>
<max_percentage_of_outliers>10</max_percentage_of_outliers><bulk>true</bulk></remove_outliers>
    val my_transform = new RemoveOutliers
    var my_dw = getMultipleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw )
    out.data.count() should be (10L)
  }
  
  it should " remove all items that exceed 3*std in all columns in multicolumn DF" in {
    val conf= <remove_outliers><columns>all</columns><times>3.0</times>
		<max_percentage_of_outliers>10</max_percentage_of_outliers><bulk>true</bulk></remove_outliers>
    val my_transform = new RemoveOutliers
    var my_dw = getMultipleDW()
    val out = my_transform.exec(conf, my_dw)
    out.data.count() should be (8L)
  }
  
  it should " not remove items when we exceed the max percentage for outliers that defaults to 0.75" in {
    val conf= <remove_outliers><columns>all</columns><times>3.0</times><bulk>true</bulk></remove_outliers>
    val my_transform = new RemoveOutliers
    var my_dw = getMultipleDW()
    val out = my_transform.exec(conf, my_dw)
    out.data.count() should be (11L)
  }

   
  private def getSingleDW() : DataWrapper = {
    val sqlContext = Application.sqlContext
    import sqlContext.implicits._
    new DataWrapper( Application.sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000)).map(_.toInt).toDF("number") )
  }
  
  private def getMultipleDW() : DataWrapper = {
    val sqlContext = Application.sqlContext
    import sqlContext.implicits._
    val df = sqlContext.createDataFrame(Seq((1,1,1,1,1),(2,2,2,2,2),
                                            (3,3,3,3,3),(4,4,4,4,4),
                                            (5,5,5,5,5),(6,6,6,6,6),
                                            (7,7,7,7,7),(8,8,8,8,1500),
                                            (9,9,9,9,9),(10,10,10,1500,10),
                                            (1500,11,11,11,11))).toDF("dim1","dim2","dim3","dim4","dim5")
    new DataWrapper(df)
  }

}
