package test.scala.transformers

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
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

class OnesBalancerTest extends FlatSpec with Matchers with BeforeAndAfterAll {

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
          for { n <- 0 until m } yield ((r.nextInt(m + 1)%2).toDouble, n.toDouble)
        )
      ).toDF("label","dim1")
      
    println( "=================ORIGINAL DF=======================" )
    println( "Number of ZEROS: " +  df_test.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  df_test.filter( "label = 1.0" ).count )
    println( "==============================================" )
      
    new DataWrapper( df_test )
  }
  
   // Or similar, as sampling is not exact >.<
   // But deterministic as we are providing the Seed!
    
   "Ones Balancer " should " create a ratio 1:1 by dropping zeroes " in {
    val conf= <ones_balancer><ratio>1.0</ratio><tolerance>0.0</tolerance></ones_balancer>
    val my_transform = new OnesBalancerTransform
    val my_dw = getTestDW() 
    { df => 
      val df_ones  = df.filter( "label = 1.0" ).sample(false, 0.5, Application.getNewSeed())
      val df_zeros = df.filter( "label = 0.0" )
      
      df_ones.unionAll(df_zeros)
    }
    
    println( "=================BEFORE=======================" )
    println( "Number of ZEROS: " +  my_dw.data.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  my_dw.data.filter( "label = 1.0" ).count )
    println( "==============================================" )
    
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    println( "=================AFTER=======================" )
    println( "Number of ZEROS: " +  df_out.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  df_out.filter( "label = 1.0" ).count )
    println( "==============================================" )
    df_out.count() should be (10031)
    df_out.filter( "label = 0.0" ).count should be (5022)
    df_out.filter( "label = 1.0" ).count should be (5009)
    
  }
  
  it should " create a ratio 1:1 by dropping ones " in {
    val conf= <ones_balancer><ratio>1.0</ratio><tolerance>0.0</tolerance></ones_balancer>
    val my_transform = new OnesBalancerTransform
    val my_dw = getTestDW() 
    { df => 
      val df_ones  = df.filter( "label = 0.0" ).sample(false, 0.6, Application.getNewSeed())
      val df_zeros = df.filter( "label = 1.0" )
      
      df_ones.unionAll(df_zeros)
    }
    
    println( "=================BEFORE=======================" )
    println( "Number of ZEROS: " +  my_dw.data.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  my_dw.data.filter( "label = 1.0" ).count )
    println( "==============================================" )
    
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    println( "=================AFTER=======================" )
    println( "Number of ZEROS: " +  df_out.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  df_out.filter( "label = 1.0" ).count )
    println( "==============================================" )
    df_out.count() should be (12375)
    df_out.filter( "label = 0.0" ).count should be (6138)
    df_out.filter( "label = 1.0" ).count should be (6237)
    
  }
   
  it should " create a ratio 2:1 by dropping zeroes " in {
    val conf= <ones_balancer><ratio>2.0</ratio><tolerance>0.0</tolerance></ones_balancer>
    val my_transform = new OnesBalancerTransform
    val my_dw = getTestDW() 
    { df => 
      val df_ones  = df.filter( "label = 0.0" )//.sample(false, 0.6, Application.getNewSeed())
      val df_zeros = df.filter( "label = 1.0" )
      
      df_ones.unionAll(df_zeros)
    }
    
    println( "=================BEFORE=======================" )
    println( "Number of ZEROS: " +  my_dw.data.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  my_dw.data.filter( "label = 1.0" ).count )
    println( "==============================================" )
    
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    println( "=================AFTER=======================" )
    println( "Number of ZEROS: " +  df_out.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  df_out.filter( "label = 1.0" ).count )
    println( "==============================================" )
    df_out.count() should be (14937)
    df_out.filter( "label = 0.0" ).count should be (5006)
    df_out.filter( "label = 1.0" ).count should be (9931)
    
  }
  
  
    it should " create a ratio 1:2 by dropping ones " in {
    val conf= <ones_balancer><ratio>0.5</ratio><tolerance>0.0</tolerance></ones_balancer>
    val my_transform = new OnesBalancerTransform
    val my_dw = getTestDW() 
    { df => 
      val df_ones  = df.filter( "label = 0.0" )//.sample(false, 0.6, Application.getNewSeed())
      val df_zeros = df.filter( "label = 1.0" )
      
      df_ones.unionAll(df_zeros)
    }
    
    println( "=================BEFORE=======================" )
    println( "Number of ZEROS: " +  my_dw.data.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  my_dw.data.filter( "label = 1.0" ).count )
    println( "==============================================" )
    
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    println( "=================AFTER=======================" )
    println( "Number of ZEROS: " +  df_out.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  df_out.filter( "label = 1.0" ).count )
    println( "==============================================" )
    df_out.count() should be (15126)
    df_out.filter( "label = 0.0" ).count should be (10069)
    df_out.filter( "label = 1.0" ).count should be (5057)
    
  }
    
  it should " do nothing when a tolerance is met " in {
    val conf= <ones_balancer><ratio>1.0</ratio><tolerance>0.10</tolerance></ones_balancer>
    val my_transform = new OnesBalancerTransform
    val my_dw = getTestDW() 
    { df => 
      val df_ones  = df.filter( "label = 0.0" )//.sample(false, 0.98, Application.getNewSeed())
      val df_zeros = df.filter( "label = 1.0" )
      
      df_ones.unionAll(df_zeros)
    }
    
    println( "=================BEFORE=======================" )
    println( "Number of ZEROS: " +  my_dw.data.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  my_dw.data.filter( "label = 1.0" ).count )
    println( "==============================================" )
    
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    println( "=================AFTER=======================" )
    println( "Number of ZEROS: " +  df_out.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  df_out.filter( "label = 1.0" ).count )
    println( "==============================================" )
    df_out.count() should be (20000)
    df_out.filter( "label = 0.0" ).count should be (10069)
    df_out.filter( "label = 1.0" ).count should be (9931)
    
  }
  
  it should " do nothing when a tolerance is met - take 2 " in {
    val conf= <ones_balancer><ratio>10.0</ratio><tolerance>0.10</tolerance></ones_balancer>
    val my_transform = new OnesBalancerTransform
    val my_dw = getTestDW() 
    { df => 
      val df_ones  = df.filter( "label = 0.0" ).sample(false, 0.1, Application.getNewSeed())
      val df_zeros = df.filter( "label = 1.0" )
      
      df_ones.unionAll(df_zeros)
    }
    
    println( "=================BEFORE=======================" )
    println( "Number of ZEROS: " +  my_dw.data.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  my_dw.data.filter( "label = 1.0" ).count )
    println( "==============================================" )
    
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    println( "=================AFTER=======================" )
    println( "Number of ZEROS: " +  df_out.filter( "label = 0.0" ).count )
    println( "Number of ONES: "  +  df_out.filter( "label = 1.0" ).count )
    println( "==============================================" )
    df_out.count() should be (10943)
    df_out.filter( "label = 0.0" ).count should be (1012)
    df_out.filter( "label = 1.0" ).count should be (9931)
    
  }
}
