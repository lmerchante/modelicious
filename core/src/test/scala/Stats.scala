//package test.scala.transformers
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.hive.HiveContext
//
//import org.scalatest._
//import scala.xml._
//
//import scala.language.reflectiveCalls
//import scala.language.postfixOps
//import org.apache.spark.rdd.RDD
//
//import com.modelicious.in.tools.{Stats, CVStats}
//import com.modelicious.in.tools.Implicits._
//
//
//import com.modelicious.in.app.{Application, TestAppConfig}
//
//class StatsTest extends FlatSpec with Matchers with BeforeAndAfterAll {
//
//  private val master = "local"
//  private val appName = "test-memory"
//  
//  override def beforeAll {
//    val conf = <spark App={appName} Master={master}></spark>
//    val appconfig = new TestAppConfig(conf)
//    
//    Application.init(appconfig)
//    Application.setSeed( Some(11) )
//  }
//
//   override def afterAll {
//    Application.reset()
//        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
//     System.clearProperty("spark.driver.port")
//  }
//  
//  "Remove Outliers " should " remove all items that exceed 3*std " in {
//    val my_rdd= getRDDDoubleDouble()
//    val cvstats = new CVStats()
//    val stats = Stats.compute( my_rdd, my_rdd.count() )
//    cvstats.append(stats)
//    cvstats.append(stats)
//    cvstats.append(stats)
//    val meanAndStd = Stats.convertMapToStatistics(cvstats.meanANDstd())
//    println( meanAndStd.toStringWithFields )
//    
//  }
//
//  private def getRDDDoubleDouble() : RDD[(Double,Double)] = {
//    val sqlContext = Application.sqlContext
//    import sqlContext.implicits._
//    Application.sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000)).map( i => (i.toDouble, i.toDouble) )
//      .toDF("value").rdd.map{x=> x.asInstanceOf[(Double, Double)] }
//  }
//
//}