//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.hive.HiveContext
//
//import org.scalatest._
//import scala.xml._
//
//import scala.language.reflectiveCalls
//import scala.language.postfixOps
//
//import com.modelicious.in.data.DataManager
//import com.modelicious.in.app.Application
//
//class MemoryDataWrapperTest extends FlatSpec with Matchers with BeforeAndAfter {
//
//  private val master = "local[2]"
//  private val appName = "example-spark"
//  
//  before {
//    val conf = <spark App="test-memory" Master="local[2]"></spark>
//    Application.init( conf )
//  }
//
//  after {
//
//    Application.stop()
//    
//  }
//  
//  "An RDD " should " be able to be read after be saved to Memory " in {
//    var my_rdd = test.scala.tools.Data.calcula_datos_entrada(Application.sqlContext)
//    
//    Application.dataManager.write("memory:my_empty_rdd", my_rdd)
//    val read = Application.dataManager.read( "memory:my_empty_rdd" )
//    
//    println( read.id )
//    println(my_rdd.id)
//    
//    val v = my_rdd.first()
//    val x = read.first().features(0)
//    
//    x should be (v)
//  }
//  
//
//  
//  it should "throw where read twice" in {
//    a [RuntimeException] should be thrownBy {
//      var my_rdd = test.scala.tools.Data.calcula_datos_entrada(Application.sqlContext)
//    
//      Application.dataManager.write("memory:my_empty_rdd", my_rdd)
//      
//      val read = Application.dataManager.read( "memory:my_empty_rdd" )
//      read.first().features(0) should be (0.8652863)
//      
//      Application.dataManager.read( "memory:my_empty_rdd" ) 
//    } 
//  }
//  
//  
//
//}