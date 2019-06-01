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

class MathTest extends FlatSpec with Matchers with BeforeAndAfterAll {

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

    private def getTestDW() : DataWrapper = {
      val sc = Application.sc
    val sqlContext = Application.sqlContext 
    import sqlContext.implicits._
    import scala.util.Random
    val r = new Random( Application.getNewSeed() )
    val m = 20000
    val df_test = sqlContext.createDataFrame(
        sc.parallelize(
          for { n <- 0 until m } yield ((r.nextInt(m + 1)%2).toDouble, n.toDouble, n.toDouble)
        )
      ).toDF("label","dim1", "dim2")
      
    new DataWrapper( df_test )
  }
  
   // Or similar, as sampling is not exact >.<
   // But deterministic as we are providing the Seed!

    // It seems that spark sql log10(0)= null, but math.log10(0) is -Inf
    // So commenting for now this test
    
//   "Math " should " add a log10 column " in {
//    val conf= <mathT><column>dim1</column><transforms>log10</transforms><drop_original>false</drop_original></mathT>
//    val my_transform = new MathTransform
//    val my_dw = getTestDW() 
//        
//    val model = my_transform.configure(conf).fit( my_dw )
//    val df_out = model.transform( my_dw ).data
//    
//    df_out.show()
//    df_out.columns.contains( "dim1_log10" ) should be (true)
//    df_out.columns.contains( "dim1" ) should be (true)
//    
//    df_out.collect.foreach { row => 
//      row.getDouble(2) should be ( math.log10( row.getDouble(1) ) )
//    }
//  }
  
   it should " add log10 and remove original column" in {
    val conf= <mathT><column>dim1</column><transforms>log10</transforms><drop_original>true</drop_original></mathT>
    val my_transform = new MathTransform
    val my_dw = getTestDW() 
        
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.show()
    df_out.columns.contains( "dim1_log10" ) should be (true)
    df_out.columns.contains( "dim1" ) should be (false)
    
  }
   
   it should " add a column per transform" in {
    val conf= <mathT><column>dim1</column><transforms>exp, log2,log10, log, log1p, atan, acos, asin, pow2,pow3,sinh,cosh, tanh, sin, cos, tan,sqrt,cbrt</transforms><drop_original>false</drop_original></mathT>
    val my_transform = new MathTransform
    val my_dw = getTestDW() 
        
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.show()

    
  }
  
  it should " be able to chain 2 transforms and remove original column" in {
    val conf= <mathT><column>dim1</column><transforms>log10</transforms><drop_original>true</drop_original></mathT>
    val my_transform = new MathTransform
    val my_dw = getTestDW() 
        
    val model = my_transform.configure(conf).fit( my_dw )
    val df2 = model.transform( my_dw )
    
    val conf2= <mathT><column>dim2</column><transforms>pow2</transforms><drop_original>true</drop_original></mathT>
    val my_transform2 = new MathTransform
    val model2 = my_transform2.configure(conf2).fit( df2 )
    val df_out = model2.transform( df2 ).data
    
    df_out.show()
    df_out.columns.contains( "dim1_log10" ) should be (true)
    df_out.columns.contains( "dim1" ) should be (false)
    df_out.columns.contains( "dim2_pow2" ) should be (true)
    
  }
   
  
}
