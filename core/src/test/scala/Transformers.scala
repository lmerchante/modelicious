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

class FilterAndDropTest extends FlatSpec with Matchers with BeforeAndAfterAll {

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

  "FilterTransform numbers " should " return a Dataframe with size 5 for 'dim1 > 6' " in {
    var my_dw = getMultipleDW()
    
    val conf = <filter><condicion>dim1 > 6</condicion></filter>
    
    val my_transform = new com.modelicious.in.transform.FilterTransform    
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform(my_dw).data
    out.count() should be (5)
  }
  
  it should " return the original Dataframe with size 11 for 'dim8 > 6' " in {
    var my_dw = getMultipleDW()
    
    val conf = <filter><condicion>dim8 > 6</condicion></filter>
    
    val my_transform = new com.modelicious.in.transform.FilterTransform   
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform(my_dw).data
    out.count() should be (11)
  }
  
  it should " return the original Dataframe with size 11 for 'dim2 >= 1' " in {
    var my_dw = getMultipleDW()
    
    val conf = <filter><condicion>dim2 >= 1</condicion></filter>
    
    val my_transform = new com.modelicious.in.transform.FilterTransform  
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform(my_dw).data
    out.count() should be (11)
  }
  
  it should " return the original Dataframe with size 10 for 'dim3 <= 10' " in {
    var my_dw = getMultipleDW()
    
    val conf = <filter><condicion><![CDATA[dim3 <= 10]]></condicion></filter>
    val my_transform = new com.modelicious.in.transform.FilterTransform   
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform(my_dw).data
    out.count() should be (10)
  }
  
  it should " return the original Dataframe with size 5 for 'dim2 >= 6 and dim3 <= 10' " in {
    var my_dw = getMultipleDW()
    
    val conf = <filter><condicion><![CDATA[dim2 >= 6 and dim3 <= 10]]></condicion></filter>
    val my_transform = new com.modelicious.in.transform.FilterTransform    
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform(my_dw).data

    out.count() should be (5)
  }
  
  it should " create a model from a state" in {
    var my_dw = getMultipleDW()   
    val conf = <filter><condicion>dim1 > 6</condicion></filter>
    
    val my_transform = new FilterTransform
    val modelState = my_transform.configure(conf).fit( my_dw ).state
    val model = new FilterTransformModel( modelState )
    val out = model.transform( my_dw )
    out.data.count() should be (5)
  }
  
  it should " create a model from a xml state" in {
    var my_dw = getMultipleDW()   
    val conf = <filter><condicion>dim1 > 6</condicion></filter>
    
    val my_transform = new FilterTransform
    val modelState = my_transform.configure(conf).fit( my_dw ).state.toXML
    val model = DataTransformerModel( modelState )
    val out = model.transform( my_dw )
    out.data.count() should be (5)
  }

  "Drop number" should " return an empty Dataframe in single column DF" in {
    var my_dw = getSingleDW()
    
    val conf = <drop><columna>number</columna></drop>
    
    val my_transform = new com.modelicious.in.transform.DropTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw ).data

    out.columns.size should be (0)
  }
  
  it should " return an smaller Dataframe in multicolumn DF" in {
    var my_dw = getMultipleDW()
    
    val conf = <drop><columna>dim2</columna></drop>
    
    val my_transform = new com.modelicious.in.transform.DropTransform   
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw ).data

    out.columns.size should be (4)
  }
  
  it should " do nothing if we gave a wrong column in a single column DF" in {
    var my_dw = getSingleDW()
    
    val conf = <drop><columna>numberz</columna></drop>
    
    val my_transform = new com.modelicious.in.transform.DropTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw ).data
    
    out.columns.size should be (1)
  }
  
   it should " do nothing if we gave a wrong column in a multiple column DF" in {
    var my_dw = getMultipleDW()
    
    val conf = <drop><columna>numberz</columna></drop>
    
    val my_transform = new com.modelicious.in.transform.DropTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw ).data
    out.columns.size should be (5)
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

class PurgeInvariantTransformTest extends FlatSpec with Matchers with BeforeAndAfterAll {

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
  
  "Purge invariant " should " remove the single column in a single column DF" in {
    val conf= <purge_invariant><min_relative_std>0.1</min_relative_std><auto>false</auto></purge_invariant>
    val my_transform = new PurgeInvariantTransform
    var my_dw = getSingleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw )
    out.data.columns.size should be (0)
  }
    
  it should " do nothing in a single column DF" in {
    val conf= <purge_invariant><min_relative_std>0.05</min_relative_std><auto>false</auto></purge_invariant>
    val my_transform = new PurgeInvariantTransform
    var my_dw = getSingleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw ).data
    out.count() should be (10L)
    out.columns.size should be (1)
  }
  
  it should " remove two columns in a multiple column DF" in {
    val conf= <purge_invariant><min_relative_std>0.1</min_relative_std><auto>false</auto></purge_invariant>
    val my_transform = new PurgeInvariantTransform
    var my_dw = getMultipleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw ).data
    out.count() should be (9L)
    out.columns.size should be (3)
  }
    
  it should " do nothing in a multiple column DF" in {
    val conf= <purge_invariant><min_relative_std>0.02</min_relative_std><auto>false</auto></purge_invariant>
    val my_transform = new PurgeInvariantTransform
    var my_dw = getMultipleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw ).data
    out.count() should be (9L)
    out.columns.size should be (5)
  }
  
  "Purge invariant AUTO " should " remove three columns in a multiple column DF" in {
    val conf= <purge_invariant><min_relative_std>0.5</min_relative_std><auto>true</auto></purge_invariant>
    val my_transform = new PurgeInvariantTransform
    var my_dw = getLabelMultipleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw ).data
    out.count() should be (10L)
    out.columns.size should be (3)
  }
    
  private def getSingleDW() : DataWrapper = {
    val sqlContext = Application.sqlContext
    import sqlContext.implicits._
    new DataWrapper( Application.sc.parallelize(List(1,1,1,1,1.2,1,1.2,1,1,1)).map(Tuple1(_)).toDF("dim1") )
  }
  
  private def getMultipleDW() : DataWrapper = {
    val sqlContext = Application.sqlContext
    import sqlContext.implicits._
    val df=sqlContext.createDataFrame(Seq((1.0   , 2.0   , 5.0  , 3.0   , 0.0),
                 (1.0   , 3.0   , 7.0  , 3.0   , 2.0),
                 (1.0   , 4.0   , 3.0  , 3.0   , 2.0),
                 (1.2   , 5.0   , 4.0  , 3.0   , 4.0),
                 (1.0   , 6.0   , 2.0  , 3.0   , 4.0),
                 (1.2   , 7.0   , 1.0  , 3.0   , 6.0),
                 (1.0   , 8.0   , 9.0  , 3.0   , 6.0),
                 (1.0   , 9.0   , 4.0  , 3.2   , 9.0),
                 (1.0   , 10.0  , 2.0  , 3.2   , 9.0))).toDF("dim1","dim2","dim3","dim4","dim5")
    new DataWrapper(df)
  }
  private def getLabelMultipleDW() : DataWrapper = {
    val sqlContext = Application.sqlContext
    import sqlContext.implicits._
    val df = sqlContext.createDataFrame(Seq((1.0   , 1.0   , 9.0  , 3.0   , 0.0  ,1.0),
                                            (1.0   , 2.0   , 9.0  , 3.0   , 0.0  ,1.0),
                                            (1.0   , 3.0   , 9.0  , 3.0   , 2.0  ,1.0),
                                            (1.0   , 4.0   , 9.0  , 3.0   , 2.0  ,1.0),
                                            (1.2   , 5.0   , 9.0  , 3.0   , 4.0  ,1.0),
                                            (1.0   , 6.0   , 9.0  , 3.0   , 4.0  ,0.0),
                                            (1.2   , 7.0   , 9.0  , 3.0   , 6.0  ,0.0),
                                            (1.0   , 8.0   , 9.0  , 3.0   , 6.0  ,0.0),
                                            (1.0   , 9.0   , 9.0  , 3.2   , 9.0  ,0.0),
                                            (1.0   , 10.0  , 9.0  , 3.2   , 9.0  ,0.0))).toDF("dim1","dim2","dim3","dim4","dim5","label")
    new DataWrapper(df)
  }

}

class PurgeCorrelatedTransformTest extends FlatSpec with Matchers with BeforeAndAfterAll {

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
  
  "Purge correlated " should " do nothing in a single column DF" in {
    val conf= <test><max_correlation>0.85</max_correlation></test>
    val my_transform = new PurgeCorrelatedTransform
    var my_dw = getSingleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw )
    out.data.columns.size should be (1)
  }
  
  it should " remove dim5 in a multicolumn DF" in {
    val conf= <test><max_correlation>0.85</max_correlation></test>
    val my_transform = new PurgeCorrelatedTransform
    var my_dw = getLabelMultipleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw )
    out.data.columns.size should be (5)
    out.schema.toList.toString should be ("List(StructField(dim1,DoubleType,false), StructField(dim2,DoubleType,false), StructField(dim3,DoubleType,false), StructField(dim4,DoubleType,false), StructField(label,DoubleType,false))")
  }
  
  it should " remove dim5 and dim4 in a multicolumn DF" in {
    val conf= <test><max_correlation>0.65</max_correlation></test>
    val my_transform = new PurgeCorrelatedTransform
    var my_dw = getLabelMultipleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw )
    out.data.columns.size should be (4)
    out.schema.toList.toString should be ("List(StructField(dim1,DoubleType,false), StructField(dim2,DoubleType,false), StructField(dim3,DoubleType,false), StructField(label,DoubleType,false))")
  }
  
  it should " remove nothing when max_correlation is too high in a multicolumn DF" in {
    val conf= <test><max_correlation>0.99</max_correlation></test>
    val my_transform = new PurgeCorrelatedTransform
    var my_dw = getLabelMultipleDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val out = model.transform( my_dw )
    out.data.columns.size should be (6)
    out.schema.toList.toString should be ("List(StructField(dim1,DoubleType,false), StructField(dim2,DoubleType,false), StructField(dim3,DoubleType,false), StructField(dim4,DoubleType,false), StructField(dim5,DoubleType,false), StructField(label,DoubleType,false))")
  }

  private def getSingleDW() : DataWrapper = {
    val sqlContext = Application.sqlContext
    import sqlContext.implicits._
    new DataWrapper( Application.sc.parallelize(List(1,1,1,1,1.2,1,1.2,1,1,1)).map(Tuple1(_)).toDF("dim1") )
  }
  
  private def getLabelMultipleDW() : DataWrapper = {
    val sqlContext = Application.sqlContext
    import sqlContext.implicits._
    val df = sqlContext.createDataFrame(Seq((1.0   , 1.0   , 9.0  , 3.0   , 0.0  ,1.0),
                                            (1.0   , 2.0   , 9.0  , 3.0   , 0.0  ,1.0),
                                            (1.0   , 3.0   , 9.0  , 3.0   , 2.0  ,1.0),
                                            (1.0   , 4.0   , 9.0  , 3.0   , 2.0  ,1.0),
                                            (1.2   , 5.0   , 9.0  , 3.0   , 4.0  ,1.0),
                                            (1.0   , 6.0   , 9.0  , 3.0   , 4.0  ,0.0),
                                            (1.2   , 7.0   , 9.0  , 3.0   , 6.0  ,0.0),
                                            (1.0   , 8.0   , 9.0  , 3.0   , 6.0  ,0.0),
                                            (1.0   , 9.0   , 9.0  , 3.2   , 9.0  ,0.0),
                                            (1.0   , 10.0  , 9.0  , 3.2   , 9.0  ,0.0))).toDF("dim1","dim2","dim3","dim4","dim5","label")
    new DataWrapper(df)
  }

}


class PCATransformTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local"
  private val appName = "test-pca"
   
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
   
  "Proyect PCA " should " create 3 projections " in {
    val conf= <pca><components>3</components></pca>
    val my_transform = new PCATransform
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,pca_1,pca_2,pca_3")
  }
  
  "Check that PCA projection vectors " should " be created properly " in {

    val conf= <pca><components>3</components></pca>
    val my_transform = new PCATransform
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data

    df_out.count() should be (40)
    println( "==================>" + df_out.select("pca_1").collect().toList.toString() )
    println( "==================>" + df_out.select("pca_2").collect().toList.toString() )
    println( "==================>" + df_out.select("pca_3").collect().toList.toString() )
    df_out.select("pca_1").collect().toList.toString() should be ("List([-6.497719626066329], [0.1095697163939604], [-0.6155593757512539], [-0.08227404884383516], [-1.452891361852523], [-5.509286145178464], [-1.6828008180064038], [-1.2642540723445495], [-1.7212309682476956], [-3.768235530494053], [-4.900934802974037], [-8.389981874247159], [-4.2918201079428915], [-5.22147399547946], [-2.585395784041663], [-2.6732838772271785], [-3.5841935668332185], [-2.2086669667615384], [-0.3764881465839529], [0.7200219760413822], [6.531227984773746], [5.367023384704357], [4.580369246781583], [-0.8562764613628975], [4.359252030574351], [4.637394543611608], [7.411140596440678], [2.207086217144785], [3.75873330718846], [2.758684097414638], [2.971469446603947], [0.5986777392858424], [-0.6339211874186856], [0.3990859049201363], [1.9444012257757843], [0.5855466534398741], [0.35999022714280404], [-0.8886528280642703], [0.0453678934400541], [1.1848141484919927])")
    df_out.select("pca_2").collect().toList.toString() should be ("List([-3.967732914605124], [4.50796359709271], [3.640809026586444], [2.938199955511967], [1.053224606148335], [-1.2775791749620646], [1.3159708065683828], [1.438178985874691], [4.498423844967751], [2.135605256646908], [-0.9575757519334127], [-1.8445326319671485], [-1.6892911848096426], [7.290006684029848], [2.4580342990785127], [-1.2627314325843508], [1.534059737868872], [-0.3403587790808005], [3.4264952059571043], [-0.04754674683749682], [0.6635198792977932], [1.2855190506352536], [-0.25455905091853737], [-4.0073273679258685], [-0.27838199392315155], [1.5017706279448775], [-0.39671081840311606], [-0.6149654318853675], [-0.7146078632659798], [-1.1510756965379028], [-0.27973711197618645], [-2.6473766396949685], [-0.5120207142017423], [-2.8250162482799968], [-1.0820957381995444], [-3.2753628853340566], [1.5683354437561543], [-0.8715401687316413], [1.858454975377472], [2.9720645702051196])")
    df_out.select("pca_3").collect().toList.toString() should be ("List([-3.3112778513686205], [-3.5208785252079116], [-3.2609250211571377], [-0.9634044251590989], [-0.06115111713652688], [0.48256412837412815], [-2.0489312755844624], [-3.0366156243439875], [2.5582987949138207], [0.5148229282851084], [0.6273863632232446], [1.6148991053576776], [-3.4997759945890348], [-0.14680652857899518], [2.166588558015828], [1.0139842767853127], [0.3579473259083477], [0.38837790914768217], [-0.738497186135965], [-2.99371491561968], [-1.7466944964609263], [-0.5643744631276164], [-0.2320635236294334], [1.2954511779608304], [1.160175101540183], [-0.8797832871384144], [0.5405610661395421], [4.724439376192289], [0.6280407839901052], [-3.0143191826534768], [-0.6265215575463419], [0.5521433248404468], [0.6848581590616467], [-0.010314685443284233], [0.4774661930301154], [-2.3034233852257606], [0.41068651004523915], [-1.3369312852861626], [-0.021907326373097807], [2.0312559446717944])")
  }
  
  "Check that schema " should " be created properly " in {

    val conf= <pca><components>3</components></pca>
    val my_transform = new PCATransform
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data

    df_out.count() should be (40)
    df_out.schema.map(x=> "("+x.name+","+x.dataType+","+x.nullable+","+x.metadata+")").mkString(";") should be ("(label,DoubleType,true,{});(pca_1,DoubleType,false,{});(pca_2,DoubleType,false,{});(pca_3,DoubleType,false,{})")
  }
  
  private def getTestDW() : DataWrapper = {
    val sqlContext  =Application.sqlContext 
    import sqlContext.implicits._
    val df_test = sqlContext.createDataFrame(Seq(
    (1.0,  0.8652863,  1.73472324,  2.61196898, 2.2862033,-1.07150287, 7.32487144),
    (1.0,  0.7515075,  0.94158552, -0.17548133, 4.7106130,-1.07631864,-2.86748436),
    (1.0,  1.3789220, -0.34581290, -0.47652206, 4.6195929,-0.60751062,-1.60555254),
    (1.0,  1.6219938,  1.34690527,  2.91987706, 1.2940924, 0.07250009,-2.53486595),
    (1.0,  1.5506300,  0.36159535,  1.27591763, 0.8131502, 0.98732621, 0.03395256),
    (1.0,  0.5021004,  2.12439235, -0.12971772, 1.3152237, 2.41643484, 4.65498439),
    (1.0,  0.6439939,  0.17226697, -0.31509725, 2.9848058,-0.13856282, 0.59436067),
    (1.0,  0.9599655, -0.78778014,  0.32350202, 3.5587566,-0.91632953, 0.32382101),
    (1.0,  0.5261070,  0.14599000,  1.72316457, 1.7065800, 4.61386056,-2.40938685),
    (1.0,  0.2825298,  0.42797728,  0.35422214, 2.9299874, 3.22235285, 1.17267871),
    (1.0,  1.3539028,  0.17406932,  2.64341921, 0.9531033, 2.55819471, 3.73921624),
    (1.0,  1.0304805,  0.76241591,  1.29864746, 1.8620801, 4.80124998, 6.97996889),
    (1.0,  1.2747662,  0.40467350,  1.23990399, 3.0535887,-1.18629745, 4.47835939),
    (1.0,  0.4619717,  0.58925781,  0.98099229, 7.3039264, 5.58129539,-1.30186541),
    (1.0,  0.8477440,  1.32799144,  0.63554369, 0.9064648, 3.74242406,-0.33115695),
    (1.0,  0.4762017, -0.16364661,  0.79463582,-0.1572651, 1.68652433, 2.60717633),
    (1.0,  0.6290744,  1.76017767,  2.41639121, 1.7721620, 2.39605099, 0.97343291),
    (1.0,  1.1025067,  0.99744416, -0.63964435, 0.2970901, 1.13716364, 1.72845550),
    (1.0,  0.6355241,  0.96177223,  2.33101448, 2.1003427, 0.75218600,-2.47201623),
    (1.0,  0.5557177, -1.03623742,  0.57050133, 1.5910596,-2.52456539,-0.12849196),
    (0.0, -0.7392406, -4.25322007, -0.59648357,-0.6900998,-3.72822898,-4.45579691),
    (0.0, -0.7748912, -1.32993049,  1.30207421,-1.4893851,-2.58517764,-4.79459413),
    (0.0, -1.3323905, -0.54291017,  1.50203011,-2.3290005,-2.67409847,-3.28288105),
    (0.0, -1.3653188, -2.30410376,  0.62716710,-1.9983614, 0.28814851, 3.50782898),
    (0.0, -0.1635837, -2.29117344, -1.26903804,-2.5845768,-1.11649794,-2.73464080),
    (0.0, -1.3699588, -0.09362027, -2.27379276,-0.3592954,-2.40947677,-3.94516735),
    (0.0, -0.5748284, -3.79596150, -0.76368574,-3.5421039,-3.00874051,-4.71861788),
    (0.0, -1.1190359, -0.76643943, -1.68002275,-4.0230556, 2.38246845,-1.39856573),
    (0.0, -1.4915245, -1.26903594, -0.66320632,-2.1559392,-1.52938751,-2.04244431),
    (0.0, -1.6861229, -1.65854506, -1.67423847, 0.8780263,-3.79240934,-0.33308727),
    (0.0, -1.5394782, -0.93665421,  0.12830833,-0.7872494,-1.96153011,-1.74797093),
    (0.0, -1.8720778, -0.94552316,  1.68152500,-1.8560651,-0.79137612, 1.29563272),
    (0.0, -1.3953547, -0.75299068,  0.18064768, 0.1597608, 0.86273650, 0.90518203),
    (0.0, -0.5646796, -0.87596445, -0.09146163,-1.6087792,-1.18991479, 1.79409753),
    (0.0, -1.2598183, -1.53525670, -0.95424774,-1.1991519,-0.74988900,-0.35575410),
    (0.0, -0.2232940, -0.74151741,  1.67096487,-0.8735829,-3.32424943, 1.94832647),
    (0.0, -1.0772227,  0.38770887, -1.09747157, 0.8858129, 0.78085555,-1.20253944),
    (0.0, -1.4194830, -0.33282955,  0.68561028, 1.2387947,-0.77660926, 1.46828768),
    (0.0, -1.1272742,  0.49704945,  0.92936102, 1.2213747, 0.64789210,-1.41798039),
    (0.0, -1.2634169, -1.27488040, -0.80357912, 0.6957749, 2.45893182,-2.80310469)
    )).toDF("label","dim1","dim2","dim3","dim4","dim5","dim6")
    new DataWrapper( df_test )
  }
}

class StandarTransformTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local"
  private val appName = "test-pca"
    
  //Logs.config_logger(getClass.getName, "test.log")
  
  com.modelicious.in.Init.initialize
  
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
   

  "Standarizer " should " standarize by mean and std " in {
    val conf= <standarize><withMean>true</withMean><withSTD>true</withSTD></standarize>
    val my_transform = new StandarTransform 
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5,dim6")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.9023317769077543], [0.7984173316311628], [1.3714364681134434], [1.593434513965418], [1.5282577878105488], [0.5706332414092284], [0.7002249082298845], [0.9888025126443898], [0.5925585255744422], [0.3700988967154156], [1.3485863739898976], [1.05320402776565], [1.2763107313387576], [0.5339836054474257], [0.8863103531154878], [0.5469798978699676], [0.6865988938378879], [1.1189857253963233], [0.6924894199965433], [0.6196020474322187], [-0.5630864094755546], [-0.5956461861894736], [-1.1048116062992668], [-1.1348851000292164], [-0.03733761063831605], [-1.139122822927361], [-0.41292836105437203], [-0.9099543490052096], [-1.2501490622633402], [-1.427876238043998], [-1.293945289104018], [-1.5977092849733388], [-1.1623169580456763], [-0.4036594381774997], [-1.0385312455861826], [-0.0918711678721669], [-0.8717662552369576], [-1.1843533911067314], [-0.9174784077448177], [-1.0418178554183513])")
    df_out.select("dim2").collect().toList.toString() should be ("List([1.4898771299877291], [0.9155848155657018], [-0.016590013044216666], [1.209067285808647], [0.49562761434808067], [1.7720273409758935], [0.3585394002911662], [-0.33660806635066604], [0.3395128661385625], [0.5436932040359611], [0.35984443941211297], [0.7858523272394724], [0.5268194866303866], [0.6604726315040069], [1.1953722276473926], [0.11531230058395656], [1.5083080823285073], [0.9560307391892394], [0.9302015361101065], [-0.5165101221578237], [-2.8458513764408853], [-0.7291663464766097], [-0.15930349709659947], [-1.4345422323648445], [-1.4251796926548213], [0.16601672158804623], [-2.514761231775206], [-0.32115576162695514], [-0.6850740392097356], [-0.9671084052127639], [-0.44440426877781447], [-0.45082606627093236], [-0.31141783898272224], [-0.40046024511932876], [-0.8778382118777448], [-0.30311031480862444], [0.514535798211163], [-0.007189075562071403], [0.59370673305013], [-0.6893058748358953])")
    df_out.select("dim3").collect().toList.toString() should be ("List([1.6864348968744707], [-0.46855921918937005], [-0.7012955659210769], [1.9244804320031363], [0.6535257943587919], [-0.4331791049946742], [-0.5764971024332296], [-0.08279227675952487], [0.9992950160270188], [-0.05904237255234452], [1.7107492533871989], [0.671098358695959], [0.6256834389203405], [0.4255172913558709], [0.15844896094229977], [0.28144401589704215], [1.5352325805243412], [-0.8274063611118472], [1.4692273318787532], [0.10816433251607822], [-0.7940385099931933], [0.6737475991364897], [0.8283346733621284], [0.1519729702859213], [-1.313994299118071], [-2.0907760418883568], [-0.9233034842424046], [-1.631728978966841], [-0.8456222577435158], [-1.627257118313057], [-0.2336976586182563], [0.9671032212447008], [-0.1932338014208859], [-0.40360309836314845], [-1.0706280795085916], [0.9589391230603392], [-1.1813552590471312], [0.19715573410855608], [0.3856008549423718], [-0.9541452893362858])")
    df_out.select("dim4").collect().toList.toString() should be ("List([0.7040460081780179], [1.739098070148043], [1.700238902827553], [0.2804845809374253], [0.07515615983830161], [0.2895061566377317], [1.0023000360027763], [1.2473365737667301], [0.4565877127294928], [0.9788964429739669], [0.13490626920888243], [0.5229752952991983], [1.0316654862702286], [2.846260107160391], [0.11499491693530052], [-0.33914277537560233], [0.4845866043063241], [-0.14516512934209444], [0.6246966320107036], [0.40726864416628655], [-0.5666256364300601], [-0.9078641234181835], [-1.266320720515525], [-1.1251611292868178], [-1.3754337868036486], [-0.4253954737522103], [-1.7842303690179413], [-1.9895628459474755], [-1.1924357433137704], [0.10285368186915866], [-0.6081016682243411], [-1.0644106386722547], [-0.20379506104625325], [-0.9588369888509753], [-0.7839550035560748], [-0.6449599876709631], [0.10617801124539229], [0.2568763605911695], [0.24943924840690546], [0.02504517971421189])")
    df_out.select("dim5").collect().toList.toString() should be ("List([-0.48817913712019434], [-0.4901761271703477], [-0.2957721271096913], [-0.01378718558056177], [0.3655703541815994], [0.9581891284790284], [-0.10131016346912758], [-0.42383228998483113], [1.8694114872692382], [1.2923849574346082], [1.016973715346972], [1.947117608459414], [-0.5357818328617967], [2.2705846599252886], [1.5080466339144296], [0.6555118772871359], [0.9497364105058239], [0.4277045210646843], [0.2680630687919366], [-1.0907310436046813], [-1.589862898523396], [-1.1158655621068958], [-1.1527390034800056], [0.0756372987846171], [-0.5068375670485246], [-1.043006416177316], [-1.2915074297634512], [0.9441040407631223], [-0.6780534604782581], [-1.6164770309124556], [-0.8572531466026811], [-0.3720169589987717], [0.3139058412954991], [-0.5372818613075119], [-0.3548131959765511], [-1.4223417872960242], [0.2799516776940176], [-0.3658934787470138], [0.22481476569779135], [0.9758116574248807])")
    df_out.select("dim6").collect().toList.toString() should be ("List([2.4910787563729118], [-0.9359657703904776], [-0.5116579242221204], [-0.8241272392541095], [0.03960393219341639], [1.5933646281260396], [0.22803373034953214], [0.13706835597794395], [-0.7819365552438245], [0.42248550505215104], [1.2854497272115981], [2.3751098460014513], [1.5339768132364155], [-0.40954715246953083], [-0.08315930061236566], [0.9048163139193326], [0.35549174602633354], [0.6093580832515527], [-0.8029948540468925], [-0.015015884274205932], [-1.4700148104825732], [-1.583930884574458], [-1.0756374008588847], [1.2076488283315006], [-0.8912988843292822], [-1.2983223898940377], [-1.5583848776614628], [-0.4420613531994807], [-0.6585569903993892], [-0.08380834512383198], [-0.5595442199458189], [0.46382716102280613], [0.3325432923674861], [0.6314293456811019], [-0.0914297662373469], [0.6832867822803541], [-0.3761501117329855], [0.5218801025897157], [-0.44858927526161796], [-0.9143189597769457])")
  }
  

  it should " standarize only by mean " in {
    val conf= <standarize><withMean>true</withMean><withSTD>false</withSTD></standarize>
    val my_transform = new StandarTransform    
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (40)

    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5,dim6")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.9879880174999999], [0.8742092174999999], [1.5016237175], [1.7446955175], [1.6733317175], [0.6248021174999999], [0.7666956174999999], [1.0826672175], [0.6488087174999999], [0.4052315174999999], [1.4766045175], [1.1531822175], [1.3974679175], [0.5846734174999999], [0.9704457175], [0.5989034175], [0.7517761175], [1.2252084175], [0.7582258175], [0.6784194174999999], [-0.6165388825000001], [-0.6521894825000001], [-1.2096887825], [-1.2426170825], [-0.040881982500000066], [-1.2472570825], [-0.4521266825000001], [-0.9963341825000002], [-1.3688227825], [-1.5634211825], [-1.4167764825], [-1.7493760825], [-1.2726529825], [-0.44197788250000003], [-1.1371165825], [-0.10059228250000006], [-0.9545209825000002], [-1.2967812825], [-1.0045724825], [-1.1407151825])")
    df_out.select("dim2").collect().toList.toString() should be ("List([2.0576241755], [1.2644864555000002], [-0.02291196449999988], [1.6698062055], [0.6844962855000001], [2.4472932855], [0.4951679055000001], [-0.46487920449999987], [0.4688909355000001], [0.7508782155000001], [0.49697025550000007], [1.0853168455], [0.7275744355000001], [0.9121587455000001], [1.6508923755], [0.1592543255000001], [2.0830786055], [1.3203450955], [1.2846731655], [-0.7133364844999999], [-3.9303191345000004], [-1.0070295544999999], [-0.22000923449999987], [-1.9812028244999997], [-1.9682725045], [0.2292806655000001], [-3.4730605645], [-0.44353849449999994], [-0.9461350044999999], [-1.3356441245], [-0.6137532745], [-0.6226222244999999], [-0.4300897444999999], [-0.5530635144999999], [-1.2123557644999998], [-0.41861647449999995], [0.7106098055000001], [-0.009928614499999877], [0.8199503855000001], [-0.9519794644999999])")
    df_out.select("dim3").collect().toList.toString() should be ("List([2.1813764785], [-0.6060738314999998], [-0.9071145614999999], [2.4892845585], [0.8453251285000001], [-0.5603102214999999], [-0.7456897514999998], [-0.10709048149999989], [1.2925720685000002], [-0.07637036149999987], [2.2128267084999997], [0.8680549585000001], [0.8093114885000001], [0.5503997885], [0.2049511885000001], [0.3640433185000001], [1.9857987085000002], [-1.0702368514999998], [1.9004219785], [0.13990882850000014], [-1.0270760714999998], [0.8714817085000002], [1.0714376085000001], [0.1965745985000001], [-1.6996305415], [-2.7043852615], [-1.1942782415], [-2.1106152515], [-1.0937988214999999], [-2.1048309714999998], [-0.30228417149999987], [1.2509324985], [-0.2499448214999999], [-0.5220541314999999], [-1.3848402414999998], [1.2403723685], [-1.5280640714999998], [0.2550177785000001], [0.4987685185000001], [-1.2341716214999998])")
    df_out.select("dim4").collect().toList.toString() should be ("List([1.6490919], [4.0735016], [3.9824814999999996], [0.656981], [0.17603880000000005], [0.6781123], [2.3476944], [2.9216452000000004], [1.0694686], [2.2928759999999997], [0.3159919], [1.2249687], [2.4164773000000004], [6.666815], [0.26935339999999997], [-0.7943765], [1.1350506], [-0.34002129999999997], [1.4632313000000001], [0.9539481999999999], [-1.3272112], [-2.1264965], [-2.9661118999999996], [-2.6354728], [-3.2216882], [-0.9964067999999999], [-4.1792153], [-4.660167], [-2.7930506], [0.24091490000000004], [-1.4243608], [-2.4931764999999997], [-0.47735059999999996], [-2.2458906], [-1.8362633], [-1.5106943], [0.24870150000000002], [0.6016832999999999], [0.5842632999999999], [0.05866349999999998])")
    df_out.select("dim5").collect().toList.toString() should be ("List([-1.17725095475], [-1.18206672475], [-0.7132587047500001], [-0.03324799475000005], [0.8815781252499999], [2.31068675525], [-0.24431090475000006], [-1.02207761475], [4.50811247525], [3.11660476525], [2.45244662525], [4.69550189525], [-1.2920455347500002], [5.47554730525], [3.6366759752499997], [1.5807762452499998], [2.29030290525], [1.03141555525], [0.6464379152499999], [-2.63031347475], [-3.83397706475], [-2.69092572475], [-2.77984655475], [0.18240042524999997], [-1.22224602475], [-2.51522485475], [-3.11448859475], [2.27672036525], [-1.6351355947500001], [-3.89815742475], [-2.06727819475], [-0.8971242047500001], [0.7569884152499999], [-1.29566287475], [-0.8556370847500001], [-3.42999751475], [0.6751074652499999], [-0.88235734475], [0.5421440152499999], [2.35318373525])")
    df_out.select("dim6").collect().toList.toString() should be ("List([7.4087047345], [-2.7836510655], [-1.5217192455], [-2.4510326554999997], [0.11778585450000009], [4.7388176845], [0.6781939645000001], [0.4076543045000001], [-2.3255535555], [1.2565120045000002], [3.8230495345000004], [7.0638021845], [4.562192684499999], [-1.2180321154999998], [-0.2473236554999999], [2.6910096245000004], [1.0572662045], [1.8122887945000001], [-2.3881829354999997], [-0.04465866549999989], [-4.3719636155], [-4.7107608355], [-3.1990477554999996], [3.5916622745000004], [-2.6508075055], [-3.8613340555], [-4.6347845855], [-1.3147324354999999], [-1.9586110154999998], [-0.2492539754999999], [-1.6641376355], [1.3794660145000002], [0.9890153245000001], [1.8779308245], [-0.2719208054999999], [2.0321597645000002], [-1.1187061455], [1.5521209745000002], [-1.3341470955], [-2.7192713955])")
  }
  

  it should " standarize only by std " in {
    val conf= <standarize><withMean>false</withMean><withSTD>true</withSTD></standarize>
    val my_transform = new StandarTransform    
   
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (40)

    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5,dim6")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.7902680101208174], [0.6863535648442257], [1.2593727013265064], [1.481370747178481], [1.4161940210236115], [0.4585694746222915], [0.5881611414429475], [0.8767387458574527], [0.48049475878750525], [0.2580351299284786], [1.2365226072029605], [0.941140260978713], [1.1642469645518205], [0.4219198386604887], [0.7742465863285508], [0.4349161310830305], [0.5745351270509509], [1.0069219586093863], [0.5804256532096064], [0.5075382806452816], [-0.6751501762624915], [-0.7077099529764106], [-1.2168753730862039], [-1.2469488668161537], [-0.14940137742525308], [-1.251186589714298], [-0.524992127841309], [-1.0220181157921466], [-1.3622128290502775], [-1.539940004830935], [-1.4060090558909553], [-1.7097730517602758], [-1.2743807248326133], [-0.5157232049644367], [-1.15059501237312], [-0.20393493465910392], [-0.9838300220238947], [-1.2964171578936685], [-1.0295421745317548], [-1.1538816222052883])")
    df_out.select("dim2").collect().toList.toString() should be ("List([1.2560721792191127], [0.681779864797085], [-0.2503949638128334], [0.9752623350400305], [0.261822663579464], [1.538222390207277], [0.12473444952254947], [-0.5704130171192827], [0.10570791536994585], [0.3098882532673444], [0.12603948864349626], [0.5520473764708558], [0.29301453586176984], [0.42666768073539024], [0.961567276878776], [-0.11849265018466013], [1.274503131559891], [0.7222257884206228], [0.6963965853414897], [-0.7503150729264403], [-3.0796563272095017], [-0.9629712972452263], [-0.39310844786521615], [-1.668347183133461], [-1.6589846434234379], [-0.06778822918057045], [-2.748566182543823], [-0.5549607123955719], [-0.9188789899783524], [-1.2009133559813805], [-0.6782092195464311], [-0.684631017039549], [-0.545222789751339], [-0.6342651958879455], [-1.1116431626463616], [-0.5369152655772411], [0.2807308474425463], [-0.2409940263306881], [0.3599017822815133], [-0.923110825604512])")
    df_out.select("dim3").collect().toList.toString() should be ("List([2.0193284748603366], [-0.1356656412035044], [-0.3684019879352112], [2.257374009989002], [0.9864193723446575], [-0.10028552700880858], [-0.2436035244473639], [0.2501013012263408], [1.3321885940128844], [0.27385120543352115], [2.043642831373065], [1.0039919366818246], [0.9585770169062061], [0.7584108693417366], [0.49134253892816543], [0.6143375938829079], [1.868126158510207], [-0.49451278312598157], [1.802120909864619], [0.4410579105019439], [-0.4611449320073276], [1.0066411771223553], [1.1612282513479941], [0.484866548271787], [-0.9811007211322055], [-1.7578824639024908], [-0.5904099062565389], [-1.298835400980975], [-0.5127286797576502], [-1.2943635403271916], [0.09919591936760933], [1.2999967992305665], [0.1396597765649798], [-0.07070952037728274], [-0.737734501522726], [1.2918327010462047], [-0.8484616810612655], [0.5300493120944217], [0.7184944329282374], [-0.6212517113504202])")
    df_out.select("dim4").collect().toList.toString() should be ("List([0.9760476703865998], [2.011099732356625], [1.972240565036135], [0.5524862431460071], [0.3471578220468835], [0.5615078188463135], [1.2743016982113584], [1.5193382359753118], [0.7285893749380746], [1.250898105182549], [0.4069079314174643], [0.7949769575077802], [1.3036671484788103], [3.118261769368973], [0.3869965791438824], [-0.06714111316702047], [0.756588266514906], [0.1268365328664874], [0.8966982942192854], [0.6792703063748684], [-0.29462397422147824], [-0.6358624612096015], [-0.9943190583069432], [-0.8531594670782359], [-1.1034321245950667], [-0.15339381154362847], [-1.5122287068093592], [-1.7175611837388935], [-0.9204340811051887], [0.3748553440777405], [-0.3361000060157592], [-0.7924089764636729], [0.06820660116232861], [-0.6868353266423934], [-0.5119533413474929], [-0.3729583254623812], [0.3781796734539742], [0.5288780227997514], [0.5214409106154874], [0.29704684192279374])")
    df_out.select("dim5").collect().toList.toString() should be ("List([-0.4443278167563633], [-0.44632480680651665], [-0.25192080674586026], [0.030064134783269268], [0.40942167454543044], [1.0020404488428594], [-0.05745884310529654], [-0.37998096962100014], [1.9132628076330693], [1.3362362777984393], [1.060825035710803], [1.990968928823245], [-0.49193051249796566], [2.3144359802891197], [1.5518979542782607], [0.699363197650967], [0.9935877308696549], [0.47155584142851537], [0.31191438915576764], [-1.0468797232408502], [-1.546011578159565], [-1.0720142417430647], [-1.1088876831161745], [0.11948861914844813], [-0.4629862466846935], [-0.9991550958134849], [-1.2476561093996201], [0.9879553611269534], [-0.6342021401144271], [-1.5726257105486248], [-0.81340182623885], [-0.32816563863494064], [0.35775716165933014], [-0.49343054094368083], [-0.31096187561272004], [-1.3784904669321933], [0.3238029980578487], [-0.32204215838318273], [0.2686660860616224], [1.0196629777887118])")
    df_out.select("dim6").collect().toList.toString() should be ("List([2.4628909224006357], [-0.964153604362754], [-0.5398457581943968], [-0.8523150732263859], [0.011416098221139964], [1.5651767941537633], [0.19984589637725572], [0.10888052200566752], [-0.810124389216101], [0.3942976710798746], [1.2572618932393218], [2.346922012029175], [1.5057889792641392], [-0.4377349864418073], [-0.1113471345846421], [0.8766284799470562], [0.3273039120540571], [0.5811702492792763], [-0.8311826880191691], [-0.043203718246482364], [-1.4982026444548495], [-1.6121187185467343], [-1.1038252348311612], [1.179460994359224], [-0.9194867183015587], [-1.3265102238663142], [-1.5865727116337391], [-0.4702491871717571], [-0.6867448243716656], [-0.11199617909610841], [-0.5877320539180954], [0.43563932705052966], [0.30435545839520967], [0.6032415117088256], [-0.11961760020962334], [0.6550989483080777], [-0.40433794570526194], [0.49369226861743926], [-0.4767771092338944], [-0.9425067937492221])")
  }  
  

  it should " leave the data as it is " in {
    val conf= <standarize><withMean>false</withMean><withSTD>false</withSTD></standarize>
    val my_transform = new StandarTransform
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (40)

    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5,dim6")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.8652863], [0.7515075], [1.378922], [1.6219938], [1.55063], [0.5021004], [0.6439939], [0.9599655], [0.526107], [0.2825298], [1.3539028], [1.0304805], [1.2747662], [0.4619717], [0.847744], [0.4762017], [0.6290744], [1.1025067], [0.6355241], [0.5557177], [-0.7392406], [-0.7748912], [-1.3323905], [-1.3653188], [-0.1635837], [-1.3699588], [-0.5748284], [-1.1190359], [-1.4915245], [-1.6861229], [-1.5394782], [-1.8720778], [-1.3953547], [-0.5646796], [-1.2598183], [-0.223294], [-1.0772227], [-1.419483], [-1.1272742], [-1.2634169])")
    df_out.select("dim2").collect().toList.toString() should be ("List([1.73472324], [0.94158552], [-0.3458129], [1.34690527], [0.36159535], [2.12439235], [0.17226697], [-0.78778014], [0.14599], [0.42797728], [0.17406932], [0.76241591], [0.4046735], [0.58925781], [1.32799144], [-0.16364661], [1.76017767], [0.99744416], [0.96177223], [-1.03623742], [-4.25322007], [-1.32993049], [-0.54291017], [-2.30410376], [-2.29117344], [-0.09362027], [-3.7959615], [-0.76643943], [-1.26903594], [-1.65854506], [-0.93665421], [-0.94552316], [-0.75299068], [-0.87596445], [-1.5352567], [-0.74151741], [0.38770887], [-0.33282955], [0.49704945], [-1.2748804])")
    df_out.select("dim3").collect().toList.toString() should be ("List([2.61196898], [-0.17548133], [-0.47652206], [2.91987706], [1.27591763], [-0.12971772], [-0.31509725], [0.32350202], [1.72316457], [0.35422214], [2.64341921], [1.29864746], [1.23990399], [0.98099229], [0.63554369], [0.79463582], [2.41639121], [-0.63964435], [2.33101448], [0.57050133], [-0.59648357], [1.30207421], [1.50203011], [0.6271671], [-1.26903804], [-2.27379276], [-0.76368574], [-1.68002275], [-0.66320632], [-1.67423847], [0.12830833], [1.681525], [0.18064768], [-0.09146163], [-0.95424774], [1.67096487], [-1.09747157], [0.68561028], [0.92936102], [-0.80357912])")
    df_out.select("dim4").collect().toList.toString() should be ("List([2.2862033], [4.710613], [4.6195929], [1.2940924], [0.8131502], [1.3152237], [2.9848058], [3.5587566], [1.70658], [2.9299874], [0.9531033], [1.8620801], [3.0535887], [7.3039264], [0.9064648], [-0.1572651], [1.772162], [0.2970901], [2.1003427], [1.5910596], [-0.6900998], [-1.4893851], [-2.3290005], [-1.9983614], [-2.5845768], [-0.3592954], [-3.5421039], [-4.0230556], [-2.1559392], [0.8780263], [-0.7872494], [-1.8560651], [0.1597608], [-1.6087792], [-1.1991519], [-0.8735829], [0.8858129], [1.2387947], [1.2213747], [0.6957749])")
    df_out.select("dim5").collect().toList.toString() should be ("List([-1.07150287], [-1.07631864], [-0.60751062], [0.07250009], [0.98732621], [2.41643484], [-0.13856282], [-0.91632953], [4.61386056], [3.22235285], [2.55819471], [4.80124998], [-1.18629745], [5.58129539], [3.74242406], [1.68652433], [2.39605099], [1.13716364], [0.752186], [-2.52456539], [-3.72822898], [-2.58517764], [-2.67409847], [0.28814851], [-1.11649794], [-2.40947677], [-3.00874051], [2.38246845], [-1.52938751], [-3.79240934], [-1.96153011], [-0.79137612], [0.8627365], [-1.18991479], [-0.749889], [-3.32424943], [0.78085555], [-0.77660926], [0.6478921], [2.45893182])")
    df_out.select("dim6").collect().toList.toString() should be ("List([7.32487144], [-2.86748436], [-1.60555254], [-2.53486595], [0.03395256], [4.65498439], [0.59436067], [0.32382101], [-2.40938685], [1.17267871], [3.73921624], [6.97996889], [4.47835939], [-1.30186541], [-0.33115695], [2.60717633], [0.97343291], [1.7284555], [-2.47201623], [-0.12849196], [-4.45579691], [-4.79459413], [-3.28288105], [3.50782898], [-2.7346408], [-3.94516735], [-4.71861788], [-1.39856573], [-2.04244431], [-0.33308727], [-1.74797093], [1.29563272], [0.90518203], [1.79409753], [-0.3557541], [1.94832647], [-1.20253944], [1.46828768], [-1.41798039], [-2.80310469])")
  }  
  
  private def getTestDW() : DataWrapper = {
    val sqlContext  =Application.sqlContext 
    import sqlContext.implicits._
    val df_test = sqlContext.createDataFrame(Seq(
    (1.0,  0.8652863,  1.73472324,  2.61196898, 2.2862033,-1.07150287, 7.32487144),
    (1.0,  0.7515075,  0.94158552, -0.17548133, 4.7106130,-1.07631864,-2.86748436),
    (1.0,  1.3789220, -0.34581290, -0.47652206, 4.6195929,-0.60751062,-1.60555254),
    (1.0,  1.6219938,  1.34690527,  2.91987706, 1.2940924, 0.07250009,-2.53486595),
    (1.0,  1.5506300,  0.36159535,  1.27591763, 0.8131502, 0.98732621, 0.03395256),
    (1.0,  0.5021004,  2.12439235, -0.12971772, 1.3152237, 2.41643484, 4.65498439),
    (1.0,  0.6439939,  0.17226697, -0.31509725, 2.9848058,-0.13856282, 0.59436067),
    (1.0,  0.9599655, -0.78778014,  0.32350202, 3.5587566,-0.91632953, 0.32382101),
    (1.0,  0.5261070,  0.14599000,  1.72316457, 1.7065800, 4.61386056,-2.40938685),
    (1.0,  0.2825298,  0.42797728,  0.35422214, 2.9299874, 3.22235285, 1.17267871),
    (1.0,  1.3539028,  0.17406932,  2.64341921, 0.9531033, 2.55819471, 3.73921624),
    (1.0,  1.0304805,  0.76241591,  1.29864746, 1.8620801, 4.80124998, 6.97996889),
    (1.0,  1.2747662,  0.40467350,  1.23990399, 3.0535887,-1.18629745, 4.47835939),
    (1.0,  0.4619717,  0.58925781,  0.98099229, 7.3039264, 5.58129539,-1.30186541),
    (1.0,  0.8477440,  1.32799144,  0.63554369, 0.9064648, 3.74242406,-0.33115695),
    (1.0,  0.4762017, -0.16364661,  0.79463582,-0.1572651, 1.68652433, 2.60717633),
    (1.0,  0.6290744,  1.76017767,  2.41639121, 1.7721620, 2.39605099, 0.97343291),
    (1.0,  1.1025067,  0.99744416, -0.63964435, 0.2970901, 1.13716364, 1.72845550),
    (1.0,  0.6355241,  0.96177223,  2.33101448, 2.1003427, 0.75218600,-2.47201623),
    (1.0,  0.5557177, -1.03623742,  0.57050133, 1.5910596,-2.52456539,-0.12849196),
    (0.0, -0.7392406, -4.25322007, -0.59648357,-0.6900998,-3.72822898,-4.45579691),
    (0.0, -0.7748912, -1.32993049,  1.30207421,-1.4893851,-2.58517764,-4.79459413),
    (0.0, -1.3323905, -0.54291017,  1.50203011,-2.3290005,-2.67409847,-3.28288105),
    (0.0, -1.3653188, -2.30410376,  0.62716710,-1.9983614, 0.28814851, 3.50782898),
    (0.0, -0.1635837, -2.29117344, -1.26903804,-2.5845768,-1.11649794,-2.73464080),
    (0.0, -1.3699588, -0.09362027, -2.27379276,-0.3592954,-2.40947677,-3.94516735),
    (0.0, -0.5748284, -3.79596150, -0.76368574,-3.5421039,-3.00874051,-4.71861788),
    (0.0, -1.1190359, -0.76643943, -1.68002275,-4.0230556, 2.38246845,-1.39856573),
    (0.0, -1.4915245, -1.26903594, -0.66320632,-2.1559392,-1.52938751,-2.04244431),
    (0.0, -1.6861229, -1.65854506, -1.67423847, 0.8780263,-3.79240934,-0.33308727),
    (0.0, -1.5394782, -0.93665421,  0.12830833,-0.7872494,-1.96153011,-1.74797093),
    (0.0, -1.8720778, -0.94552316,  1.68152500,-1.8560651,-0.79137612, 1.29563272),
    (0.0, -1.3953547, -0.75299068,  0.18064768, 0.1597608, 0.86273650, 0.90518203),
    (0.0, -0.5646796, -0.87596445, -0.09146163,-1.6087792,-1.18991479, 1.79409753),
    (0.0, -1.2598183, -1.53525670, -0.95424774,-1.1991519,-0.74988900,-0.35575410),
    (0.0, -0.2232940, -0.74151741,  1.67096487,-0.8735829,-3.32424943, 1.94832647),
    (0.0, -1.0772227,  0.38770887, -1.09747157, 0.8858129, 0.78085555,-1.20253944),
    (0.0, -1.4194830, -0.33282955,  0.68561028, 1.2387947,-0.77660926, 1.46828768),
    (0.0, -1.1272742,  0.49704945,  0.92936102, 1.2213747, 0.64789210,-1.41798039),
    (0.0, -1.2634169, -1.27488040, -0.80357912, 0.6957749, 2.45893182,-2.80310469)
    )).toDF("label","dim1","dim2","dim3","dim4","dim5","dim6")
    new DataWrapper( df_test ) 
  }
}

class DiscretizeTransformTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local"
  private val appName = "test-pca"
  
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
   

  "Discretizer " should " discretize nothing " in {
    val conf= <discretize><limit>20</limit><nCategories>3</nCategories><exclude></exclude></discretize>
    val my_transform = new DiscretizeTransform 
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (20)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    df_out.select("label").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim1").collect().toList.toString() should be ("List([1.0], [2.0], [3.0], [4.0], [5.0], [6.0], [7.0], [8.0], [9.0], [10.0], [11.0], [12.0], [13.0], [14.0], [15.0], [16.0], [17.0], [18.0], [19.0], [20.0])")
    df_out.select("dim2").collect().toList.toString() should be ("List([-3.0], [-3.0], [-3.0], [-1.0], [-1.0], [-1.0], [-1.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [3.0], [3.0], [3.0], [4.0], [4.0])")
    df_out.select("dim3").collect().toList.toString() should be ("List([1.0], [1.0], [2.0], [2.0], [3.0], [3.0], [4.0], [4.0], [5.0], [5.0], [4.0], [4.0], [3.0], [3.0], [2.0], [2.0], [1.0], [1.0], [1.0], [1.0])")
    df_out.select("dim4").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim5").collect().toList.toString() should be ("List([7.32487144], [-2.86748436], [-1.60555254], [-2.53486595], [1.94832647], [-1.20253944], [1.46828768], [-1.41798039], [-2.80310469], [7.32487144], [-2.86748436], [-1.60555254], [-2.53486595], [1.94832647], [-1.20253944], [1.46828768], [-1.41798039], [-2.80310469], [-2.80310469], [-2.80310469])")
  }
  
  it should " discretize only dim1 " in {
    val conf= <discretize><limit>6</limit><nCategories>3</nCategories><exclude>dim5,dim4</exclude></discretize>
    val my_transform = new DiscretizeTransform 
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (20)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    df_out.select("label").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim2").collect().toList.toString() should be ("List([-3.0], [-3.0], [-3.0], [-1.0], [-1.0], [-1.0], [-1.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [3.0], [3.0], [3.0], [4.0], [4.0])")
    df_out.select("dim3").collect().toList.toString() should be ("List([1.0], [1.0], [2.0], [2.0], [3.0], [3.0], [4.0], [4.0], [5.0], [5.0], [4.0], [4.0], [3.0], [3.0], [2.0], [2.0], [1.0], [1.0], [1.0], [1.0])")
    df_out.select("dim4").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim5").collect().toList.toString() should be ("List([7.32487144], [-2.86748436], [-1.60555254], [-2.53486595], [1.94832647], [-1.20253944], [1.46828768], [-1.41798039], [-2.80310469], [7.32487144], [-2.86748436], [-1.60555254], [-2.53486595], [1.94832647], [-1.20253944], [1.46828768], [-1.41798039], [-2.80310469], [-2.80310469], [-2.80310469])")
  }

  it should " discretize only dim1 and dim2 " in {
    val conf= <discretize><limit>5</limit><nCategories>3</nCategories><exclude>dim5,dim4</exclude></discretize>
    val my_transform = new DiscretizeTransform 
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (20)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    df_out.select("label").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim2").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim3").collect().toList.toString() should be ("List([1.0], [1.0], [2.0], [2.0], [3.0], [3.0], [4.0], [4.0], [5.0], [5.0], [4.0], [4.0], [3.0], [3.0], [2.0], [2.0], [1.0], [1.0], [1.0], [1.0])")
    df_out.select("dim4").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim5").collect().toList.toString() should be ("List([7.32487144], [-2.86748436], [-1.60555254], [-2.53486595], [1.94832647], [-1.20253944], [1.46828768], [-1.41798039], [-2.80310469], [7.32487144], [-2.86748436], [-1.60555254], [-2.53486595], [1.94832647], [-1.20253944], [1.46828768], [-1.41798039], [-2.80310469], [-2.80310469], [-2.80310469])")
  }

  it should " discretize only dim1, dim2 and dim5 " in {
    val conf= <discretize><limit>5</limit><nCategories>3</nCategories><exclude></exclude></discretize>
    val my_transform = new DiscretizeTransform 
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (20)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    df_out.select("label").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim2").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim3").collect().toList.toString() should be ("List([1.0], [1.0], [2.0], [2.0], [3.0], [3.0], [4.0], [4.0], [5.0], [5.0], [4.0], [4.0], [3.0], [3.0], [2.0], [2.0], [1.0], [1.0], [1.0], [1.0])")
    df_out.select("dim4").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim5").collect().toList.toString() should be ("List([2.0], [0.0], [1.0], [1.0], [2.0], [2.0], [2.0], [1.0], [0.0], [2.0], [0.0], [1.0], [1.0], [2.0], [2.0], [2.0], [1.0], [0.0], [0.0], [0.0])")
  }
 
  it should " discretize only dim1, dim2, dim3 and dim5 " in {
    val conf= <discretize><limit>4</limit><nCategories>3</nCategories><exclude></exclude></discretize>
    val my_transform = new DiscretizeTransform 
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    df_out.select("label").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim2").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim3").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0])")
    df_out.select("dim4").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim5").collect().toList.toString() should be ("List([2.0], [0.0], [1.0], [1.0], [2.0], [2.0], [2.0], [1.0], [0.0], [2.0], [0.0], [1.0], [1.0], [2.0], [2.0], [2.0], [1.0], [0.0], [0.0], [0.0])")
  }
 
  it should " discretize only dim1, dim2, dim3, dim4 and dim5 " in {
    val conf= <discretize><limit>1</limit><nCategories>3</nCategories><exclude></exclude></discretize>
    val my_transform = new DiscretizeTransform 
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    df_out.select("label").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim2").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim3").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0])")
    df_out.select("dim4").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [1.0], [1.0], [1.0], [1.0], [1.0])")
    df_out.select("dim5").collect().toList.toString() should be ("List([2.0], [0.0], [1.0], [1.0], [2.0], [2.0], [2.0], [1.0], [0.0], [2.0], [0.0], [1.0], [1.0], [2.0], [2.0], [2.0], [1.0], [0.0], [0.0], [0.0])")
  }
  
  it should " discretize only dim1, dim3 and dim5 even though dim1, dim2, dim3, dim4 and dim5 fulfil limit parameter" in {
    val conf= <discretize><limit>1</limit><nCategories>3</nCategories><columns>dim1,dim3,dim5</columns><exclude></exclude></discretize>
    val my_transform = new DiscretizeTransform 
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    df_out.select("label").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim2").collect().toList.toString() should be ("List([-3.0], [-3.0], [-3.0], [-1.0], [-1.0], [-1.0], [-1.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [3.0], [3.0], [3.0], [4.0], [4.0])")
    df_out.select("dim3").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0])")
    df_out.select("dim4").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim5").collect().toList.toString() should be ("List([2.0], [0.0], [1.0], [1.0], [2.0], [2.0], [2.0], [1.0], [0.0], [2.0], [0.0], [1.0], [1.0], [2.0], [2.0], [2.0], [1.0], [0.0], [0.0], [0.0])")
  }
  
  it should " discretize only dim1, dim3 even though dim1, dim2, dim3, dim4 and dim5 fulfil limit parameter" in {
    val conf= <discretize><limit>1</limit><nCategories>3</nCategories><columns>dim1,dim3,dim5</columns><exclude>dim5</exclude></discretize>
    val my_transform = new DiscretizeTransform 
    
    val my_dw = getTestDW()
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    df_out.select("label").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim1").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0])")
    df_out.select("dim2").collect().toList.toString() should be ("List([-3.0], [-3.0], [-3.0], [-1.0], [-1.0], [-1.0], [-1.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [3.0], [3.0], [3.0], [4.0], [4.0])")
    df_out.select("dim3").collect().toList.toString() should be ("List([1.0], [1.0], [1.0], [1.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [2.0], [1.0], [1.0], [1.0], [1.0], [1.0], [1.0])")
    df_out.select("dim4").collect().toList.toString() should be ("List([0.0], [0.0], [0.0], [0.0], [0.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [10.0], [0.0], [0.0], [0.0], [0.0], [0.0])")
    df_out.select("dim5").collect().toList.toString() should be ("List([7.32487144], [-2.86748436], [-1.60555254], [-2.53486595], [1.94832647], [-1.20253944], [1.46828768], [-1.41798039], [-2.80310469], [7.32487144], [-2.86748436], [-1.60555254], [-2.53486595], [1.94832647], [-1.20253944], [1.46828768], [-1.41798039], [-2.80310469], [-2.80310469], [-2.80310469])")
  }
  
  
  private def getTestDW() : DataWrapper = {
    val sqlContext  =Application.sqlContext 
    import sqlContext.implicits._
    val df_test = sqlContext.createDataFrame(Seq(
    (1.0,    1.0,   -3.0,  1.0,   0.0,  7.32487144),
    (1.0,    2.0,   -3.0,  1.0,   0.0, -2.86748436),
    (1.0,    3.0,   -3.0,  2.0,   0.0, -1.60555254),
    (1.0,    4.0,   -1.0,  2.0,   0.0, -2.53486595),
    (0.0,    5.0,   -1.0,  3.0,   0.0,  1.94832647),
    (0.0,    6.0,   -1.0,  3.0,  10.0, -1.20253944),
    (0.0,    7.0,   -1.0,  4.0,  10.0,  1.46828768),
    (0.0,    8.0,    0.0,  4.0,  10.0, -1.41798039),
    (0.0,    9.0,    0.0,  5.0,  10.0, -2.80310469),
    (1.0,   10.0,    0.0,  5.0,  10.0,  7.32487144),
    (1.0,   11.0,    0.0,  4.0,  10.0, -2.86748436),
    (1.0,   12.0,    1.0,  4.0,  10.0, -1.60555254),
    (1.0,   13.0,    1.0,  3.0,  10.0, -2.53486595),
    (0.0,   14.0,    1.0,  3.0,  10.0,  1.94832647),
    (0.0,   15.0,    1.0,  2.0,  10.0, -1.20253944),
    (0.0,   16.0,    3.0,  2.0,   0.0,  1.46828768),
    (0.0,   17.0,    3.0,  1.0,   0.0, -1.41798039),
    (0.0,   18.0,    3.0,  1.0,   0.0, -2.80310469),
    (0.0,   19.0,    4.0,  1.0,   0.0, -2.80310469),
    (0.0,   20.0,    4.0,  1.0,   0.0, -2.80310469)
    )).toDF("label","dim1","dim2","dim3","dim4","dim5")
    new DataWrapper( df_test ) 
  }
}

class SamplingTransformTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local"
  private val appName = "test-pca"
  
  //Logs.config_logger(getClass.getName, "test.log")
    
  com.modelicious.in.Init.initialize
  
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
   

  "Sampling with no sampling rate " should " discretize nothing " in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate></sampling_rate></sampling>
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (20)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    my_dw.data.except(df_out).rdd.count() should be (0)  }

  "Sampling with 0.00 sampling rate " should " return empty DF" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate>0.0</sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (0)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    my_dw.data.except(df_out).rdd.count() should be (20)  }  
  
  "Sampling with 0.25 sampling rate " should " return 1/4 the original DF" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate>0.25</sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data

    df_out.count() should be (7)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    my_dw.data.except(df_out).rdd.count() should be (13)  }  

  "Sampling with 0.5 sampling rate " should " return half the original DF" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate>0.5</sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (11)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    my_dw.data.except(df_out).rdd.count() should be (9)  } 
  
  "Sampling with 0.75 sampling rate " should " return 3/4 the original DF" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate>0.75</sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (14)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    my_dw.data.except(df_out).rdd.count() should be (6)  }  

  "Sampling with 1.0 sampling rate " should " return the original DF" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate>1.0</sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.count() should be (20)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5")
    my_dw.data.except(df_out).rdd.count() should be (0)  }  
  
  private def getTestDW() : DataWrapper = {
    val sqlContext  =Application.sqlContext 
    import sqlContext.implicits._
    val df_test = sqlContext.createDataFrame(Seq(
    (1.0,    1.0,   -3.0,  1.0,   0.0,  7.32487144),
    (1.0,    2.0,   -3.0,  1.0,   0.0, -2.86748436),
    (1.0,    3.0,   -3.0,  2.0,   0.0, -1.60555254),
    (1.0,    4.0,   -1.0,  2.0,   0.0, -2.53486595),
    (0.0,    5.0,   -1.0,  3.0,   0.0,  1.94832647),
    (0.0,    6.0,   -1.0,  3.0,  10.0, -1.20253944),
    (0.0,    7.0,   -1.0,  4.0,  10.0,  1.46828768),
    (0.0,    8.0,    0.0,  4.0,  10.0, -1.41798039),
    (0.0,    9.0,    0.0,  5.0,  10.0, -2.80310469),
    (1.0,   10.0,    0.0,  5.0,  10.0,  7.32487144),
    (1.0,   11.0,    0.0,  4.0,  10.0, -2.86748436),
    (1.0,   12.0,    1.0,  4.0,  10.0, -1.60555254),
    (1.0,   13.0,    1.0,  3.0,  10.0, -2.53486595),
    (0.0,   14.0,    1.0,  3.0,  10.0,  1.94832647),
    (0.0,   15.0,    1.0,  2.0,  10.0, -1.20253944),
    (0.0,   16.0,    3.0,  2.0,   0.0,  1.46828768),
    (0.0,   17.0,    3.0,  1.0,   0.0, -1.41798039),
    (0.0,   18.0,    3.0,  1.0,   0.0, -2.80310469),
    (0.0,   19.0,    4.0,  1.0,   0.0, -2.80310469),
    (0.0,   20.0,    4.0,  1.0,   0.0, -2.80310469)
    )).toDF("label","dim1","dim2","dim3","dim4","dim5")
    new DataWrapper( df_test ) 
  }
}

class FeatureSelectorTransformTest extends FlatSpec with Matchers with BeforeAndAfterAll {
    
  private val master = "local"
  private val appName = "test-spark"
  
  com.modelicious.in.Init.initialize
  
  override def beforeAll {
    val conf = <spark App={appName} Master={master}></spark>
    val appconfig = new TestAppConfig(conf)
    
    Application.init(appconfig)
    Application.setSeed( Some(11) )
  }

  override def afterAll {
    Application.reset()
        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
     System.clearProperty("spark.driver.port")
  }
  
  "Test FeatureSelector MIFS selecting 2 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>mifs</method><num_features>2</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (3)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
  }
  
  "Test FeatureSelector MIM selecting 2 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>mim</method><num_features>2</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (3)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
  }

  "Test FeatureSelector JMI selecting 2 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>jmi</method><num_features>2</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (3)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
  }  

  "Test FeatureSelector MRMR selecting 2 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>mrmr</method><num_features>2</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (3)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
  }  
  
  "Test FeatureSelector ICAP selecting 2 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>icap</method><num_features>2</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (3)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
  }  

  "Test FeatureSelector CMIM selecting 2 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>cmim</method><num_features>2</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (3)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
  } 
  
  "Test FeatureSelector IF selecting 2 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>if</method><num_features>2</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (3)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
  } 

  "Test FeatureSelector NONE selecting 2 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method></method><num_features>2</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val tmp = my_transform.configure(conf)
    println(">>>>"+tmp.state.num_features+"-"+tmp.state.num_partitions+"-"+tmp.state.method)
    val model = tmp.fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (3)
    model.state.method should be ("mifs") 
    df_out.columns.toList.toString should be ("List(label, dim1, dim2)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
  }   
  
    "Test FeatureSelector MIFS selecting 4 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>mifs</method><num_features>4</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (5)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2, dim3, dim4)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (my_dw.data.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (my_dw.data.select("dim4").collect().toList.toString)
  }
  
  "Test FeatureSelector MIM selecting 4 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>mim</method><num_features>4</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (5)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2, dim3, dim4)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (my_dw.data.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (my_dw.data.select("dim4").collect().toList.toString)
  }

  "Test FeatureSelector JMI selecting 4 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>jmi</method><num_features>4</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (5)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2, dim3, dim4)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (my_dw.data.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (my_dw.data.select("dim4").collect().toList.toString)
  }  

  "Test FeatureSelector MRMR selecting 4 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>mrmr</method><num_features>4</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (5)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2, dim3, dim4)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (my_dw.data.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (my_dw.data.select("dim4").collect().toList.toString)
  }  
  
  "Test FeatureSelector ICAP selecting 4 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>icap</method><num_features>4</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (5)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2, dim3, dim4)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (my_dw.data.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (my_dw.data.select("dim4").collect().toList.toString)
  }  

  "Test FeatureSelector CMIM selecting 4 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>cmim</method><num_features>4</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (5)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2, dim3, dim4)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (my_dw.data.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (my_dw.data.select("dim4").collect().toList.toString)
  } 
  
  "Test FeatureSelector IF selecting 4 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>if</method><num_features>4</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (5)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2, dim3, dim4)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (my_dw.data.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (my_dw.data.select("dim4").collect().toList.toString)
  } 

  "Test FeatureSelector NONE selecting 4 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method></method><num_features>4</num_features><num_partitions></num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.columns.size should be (5)
    df_out.columns.toList.toString should be ("List(label, dim1, dim2, dim3, dim4)")
    df_out.select("label").collect().toList.toString should be (my_dw.data.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (my_dw.data.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (my_dw.data.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (my_dw.data.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (my_dw.data.select("dim4").collect().toList.toString)
  }   
  
  private def getTestDW() : DataWrapper = {
    val sqlContext  =Application.sqlContext 
    import sqlContext.implicits._
    val df_test = sqlContext.createDataFrame(Seq(
  (1.0,  0.8652863,  1.73472324, 2.61196898, 2.2862033,-1.07150287, 7.32487144 ),
  (1.0,  0.7515075,  0.94158552,-0.17548133, 4.7106130,-1.07631864,-2.86748436 ),
  (1.0,  1.3789220, -0.34581290,-0.47652206, 4.6195929,-0.60751062,-1.60555254 ),
  (1.0,  1.6219938,  1.34690527, 2.91987706, 1.2940924, 0.07250009,-2.53486595 ),
  (1.0,  1.5506300,  0.36159535, 1.27591763, 0.8131502, 0.98732621, 0.03395256 ),
  (1.0,  0.5021004,  2.12439235,-0.12971772, 1.3152237, 2.41643484, 4.65498439 ),
  (1.0,  0.6439939,  0.17226697,-0.31509725, 2.9848058,-0.13856282, 0.59436067 ),
  (1.0,  0.9599655, -0.78778014, 0.32350202, 3.5587566,-0.91632953, 0.32382101 ),
  (1.0,  0.5261070,  0.14599000, 1.72316457, 1.7065800, 4.61386056,-2.40938685 ),
  (1.0,  0.2825298,  0.42797728, 0.35422214, 2.9299874, 3.22235285, 1.17267871 ),
  (1.0,  1.3539028,  0.17406932, 2.64341921, 0.9531033, 2.55819471, 3.73921624 ),
  (1.0,  1.0304805,  0.76241591, 1.29864746, 1.8620801, 4.80124998, 6.97996889 ),
  (1.0,  1.2747662,  0.40467350, 1.23990399, 3.0535887,-1.18629745, 4.47835939 ),
  (1.0,  0.4619717,  0.58925781, 0.98099229, 7.3039264, 5.58129539,-1.30186541 ),
  (1.0,  0.8477440,  1.32799144, 0.63554369, 0.9064648, 3.74242406,-0.33115695 ),
  (1.0,  0.4762017, -0.16364661, 0.79463582,-0.1572651, 1.68652433, 2.60717633 ),
  (1.0,  0.6290744,  1.76017767, 2.41639121, 1.7721620, 2.39605099, 0.97343291 ),
  (1.0,  1.1025067,  0.99744416,-0.63964435, 0.2970901, 1.13716364, 1.72845550 ),
  (1.0,  0.6355241,  0.96177223, 2.33101448, 2.1003427, 0.75218600,-2.47201623 ),
  (1.0,  0.5557177, -1.03623742, 0.57050133, 1.5910596,-2.52456539,-0.12849196 ),
  (0.0, -0.7392406, -4.25322007,-0.59648357,-0.6900998,-3.72822898,-4.45579691 ),
  (0.0, -0.7748912, -1.32993049, 1.30207421,-1.4893851,-2.58517764,-4.79459413 ),
  (0.0, -1.3323905, -0.54291017, 1.50203011,-2.3290005,-2.67409847,-3.28288105 ),
  (0.0, -1.3653188, -2.30410376, 0.62716710,-1.9983614, 0.28814851, 3.50782898 ),
  (0.0, -0.1635837, -2.29117344,-1.26903804,-2.5845768,-1.11649794,-2.73464080 ),
  (0.0, -1.3699588, -0.09362027,-2.27379276,-0.3592954,-2.40947677,-3.94516735 ),
  (0.0, -0.5748284, -3.79596150,-0.76368574,-3.5421039,-3.00874051,-4.71861788 ),
  (0.0, -1.1190359, -0.76643943,-1.68002275,-4.0230556, 2.38246845,-1.39856573 ),
  (0.0, -1.4915245, -1.26903594,-0.66320632,-2.1559392,-1.52938751,-2.04244431 ),
  (0.0, -1.6861229, -1.65854506,-1.67423847, 0.8780263,-3.79240934,-0.33308727 ),
  (0.0, -1.5394782, -0.93665421, 0.12830833,-0.7872494,-1.96153011,-1.74797093 ),
  (0.0, -1.8720778, -0.94552316, 1.68152500,-1.8560651,-0.79137612, 1.29563272 ),
  (0.0, -1.3953547, -0.75299068, 0.18064768, 0.1597608, 0.86273650, 0.90518203 ),
  (0.0, -0.5646796, -0.87596445,-0.09146163,-1.6087792,-1.18991479, 1.79409753 ),
  (0.0, -1.2598183, -1.53525670,-0.95424774,-1.1991519,-0.74988900,-0.35575410 ),
  (0.0, -0.2232940, -0.74151741, 1.67096487,-0.8735829,-3.32424943, 1.94832647 ),
  (0.0, -1.0772227,  0.38770887,-1.09747157, 0.8858129, 0.78085555,-1.20253944 ),
  (0.0, -1.4194830, -0.33282955, 0.68561028, 1.2387947,-0.77660926, 1.46828768 ),
  (0.0, -1.1272742,  0.49704945, 0.92936102, 1.2213747, 0.64789210,-1.41798039 ),
  (0.0, -1.2634169, -1.27488040,-0.80357912, 0.6957749, 2.45893182,-2.80310469 )
    )).toDF("label","dim1","dim2","dim3","dim4","dim5","dim6")
    new DataWrapper( df_test ) 
  }
  
}

class OneHotTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local"
  private val appName = "test-onehot"

  //Logs.config_logger(getClass.getName, "test.log") 
  
  com.modelicious.in.Init.initialize
  
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
   
  "Encode category1 variable " should " create 3 new extra columns and remove the old one " in {
    val my_dw = getTestDW()
    val conf= <onehot><columns>category1</columns></onehot>	
    val my_transform = new OneHotTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    val a="label,dim1,dim2,dim3,dim4,dim5,dim6,category2,category1_hot_1,category1_hot_2,category1_hot_3"
    df_out.schema.map(x=> x.name).mkString(",") should be (a)
  }
  
  "Encode category2 variable " should " create 9 new extra columns and remove the old one " in {
    val my_dw = getTestDW()
    val conf= <onehot><columns>category2</columns></onehot>	
    val my_transform = new OneHotTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,category1,dim3,dim4,dim5,dim6,category2_hot_1,category2_hot_2,category2_hot_3,category2_hot_4,category2_hot_5,category2_hot_6,category2_hot_7,category2_hot_8,category2_hot_9")
  }
  
  "Encode category1 and category2 variable " should " create 12 new extra columns and remove the old ones" in {
    val my_dw = getTestDW()
    val conf= <onehot><columns>category1,category2</columns></onehot>	
    val my_transform = new OneHotTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5,dim6,category1_hot_1,category1_hot_2,category1_hot_3,category2_hot_1,category2_hot_2,category2_hot_3,category2_hot_4,category2_hot_5,category2_hot_6,category2_hot_7,category2_hot_8,category2_hot_9")
  }
  
  "Encode category1 and category2 variable " should " work with small datasets that have missing values for a category" in {
    val my_dw = getTestDW()
    val conf= <onehot><columns>category1,category2</columns></onehot>	
    val my_transform = new OneHotTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5,dim6,category1_hot_1,category1_hot_2,category1_hot_3,category2_hot_1,category2_hot_2,category2_hot_3,category2_hot_4,category2_hot_5,category2_hot_6,category2_hot_7,category2_hot_8,category2_hot_9")
    
    val sqlContext  =Application.sqlContext 
    import sqlContext.implicits._
    val df_small = sqlContext.createDataFrame(Seq(
    (0.0, -0.2232940, -0.74151741, "e",  1.67096487,-0.8735829,-3.32424943, 1.94832647, "dos"),
    (0.0, -1.0772227,  0.38770887, "b", -1.09747157, 0.8858129, 0.78085555,-1.20253944, "dos"),
    (0.0, -1.4194830, -0.33282955, "e",  0.68561028, 1.2387947,-0.77660926, 1.46828768, "uno"),
    (0.0, -1.1272742,  0.49704945, "b",  0.92936102, 1.2213747, 0.64789210,-1.41798039, "uno"),
    (0.0, -1.2634169, -1.27488040, "b", -0.80357912, 0.6957749, 2.45893182,-2.80310469, "uno")
    )).toDF("label","dim1","dim2","category1","dim3","dim4","dim5","dim6", "category2")
    val dw_small = new DataWrapper( df_small ) 
    
    
    val df_out_small = model.transform( dw_small ).data
    df_out_small.count() should be (5)
    df_out_small.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5,dim6,category1_hot_1,category1_hot_2,category1_hot_3,category2_hot_1,category2_hot_2,category2_hot_3,category2_hot_4,category2_hot_5,category2_hot_6,category2_hot_7,category2_hot_8,category2_hot_9")
    
  }
  
  
  "Non existing columns " should " not fail execution " in {
    val my_dw = getTestDW()
    val conf= <onehot><columns>category1,dim10,category10,category2</columns></onehot>	
    val my_transform = new OneHotTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,dim3,dim4,dim5,dim6,category1_hot_1,category1_hot_2,category1_hot_3,category2_hot_1,category2_hot_2,category2_hot_3,category2_hot_4,category2_hot_5,category2_hot_6,category2_hot_7,category2_hot_8,category2_hot_9")
  }
  
  private def getTestDW() : DataWrapper = {
    val sqlContext  =Application.sqlContext 
    import sqlContext.implicits._
    val df_test = sqlContext.createDataFrame(Seq(
    (1.0,  0.8652863,  1.73472324, "a",  2.61196898, 2.2862033,-1.07150287, 7.32487144, "uno"),
    (1.0,  0.7515075,  0.94158552, "b", -0.17548133, 4.7106130,-1.07631864,-2.86748436, "dos"),
    (1.0,  1.3789220, -0.34581290, "c", -0.47652206, 4.6195929,-0.60751062,-1.60555254, "tres"),
    (1.0,  1.6219938,  1.34690527, "a",  2.91987706, 1.2940924, 0.07250009,-2.53486595, "cuatro"),
    (1.0,  1.5506300,  0.36159535, "a",  1.27591763, 0.8131502, 0.98732621, 0.03395256, "cinco"),
    (1.0,  0.5021004,  2.12439235, "c", -0.12971772, 1.3152237, 2.41643484, 4.65498439, "seis"),
    (1.0,  0.6439939,  0.17226697, "a", -0.31509725, 2.9848058,-0.13856282, 0.59436067, "siete"),
    (1.0,  0.9599655, -0.78778014, "b",  0.32350202, 3.5587566,-0.91632953, 0.32382101, "ocho"),
    (1.0,  0.5261070,  0.14599000, "e",  1.72316457, 1.7065800, 4.61386056,-2.40938685, "nueve"),
    (1.0,  0.2825298,  0.42797728, "e",  0.35422214, 2.9299874, 3.22235285, 1.17267871, "diez"),
    (1.0,  1.3539028,  0.17406932, "a",  2.64341921, 0.9531033, 2.55819471, 3.73921624, "diez"),
    (1.0,  1.0304805,  0.76241591, "c",  1.29864746, 1.8620801, 4.80124998, 6.97996889, "nueve"),
    (1.0,  1.2747662,  0.40467350, "a",  1.23990399, 3.0535887,-1.18629745, 4.47835939, "ocho"),
    (1.0,  0.4619717,  0.58925781, "c",  0.98099229, 7.3039264, 5.58129539,-1.30186541, "siete"),
    (1.0,  0.8477440,  1.32799144, "e",  0.63554369, 0.9064648, 3.74242406,-0.33115695, "seis"),
    (1.0,  0.4762017, -0.16364661, "b",  0.79463582,-0.1572651, 1.68652433, 2.60717633, "cinco"),
    (1.0,  0.6290744,  1.76017767, "e",  2.41639121, 1.7721620, 2.39605099, 0.97343291, "cuatro"),
    (1.0,  1.1025067,  0.99744416, "b", -0.63964435, 0.2970901, 1.13716364, 1.72845550, "tres"),
    (1.0,  0.6355241,  0.96177223, "c",  2.33101448, 2.1003427, 0.75218600,-2.47201623, "dos"),
    (1.0,  0.5557177, -1.03623742, "a",  0.57050133, 1.5910596,-2.52456539,-0.12849196, "uno"),
    (0.0, -0.7392406, -4.25322007, "c", -0.59648357,-0.6900998,-3.72822898,-4.45579691, "uno"),
    (0.0, -0.7748912, -1.32993049, "a",  1.30207421,-1.4893851,-2.58517764,-4.79459413, "dos"),
    (0.0, -1.3323905, -0.54291017, "b",  1.50203011,-2.3290005,-2.67409847,-3.28288105, "uno"),
    (0.0, -1.3653188, -2.30410376, "c",  0.62716710,-1.9983614, 0.28814851, 3.50782898, "dos"),
    (0.0, -0.1635837, -2.29117344, "a", -1.26903804,-2.5845768,-1.11649794,-2.73464080, "cinco"),
    (0.0, -1.3699588, -0.09362027, "a", -2.27379276,-0.3592954,-2.40947677,-3.94516735, "cinco"),
    (0.0, -0.5748284, -3.79596150, "c", -0.76368574,-3.5421039,-3.00874051,-4.71861788, "seis"),
    (0.0, -1.1190359, -0.76643943, "a", -1.68002275,-4.0230556, 2.38246845,-1.39856573, "cinco"),
    (0.0, -1.4915245, -1.26903594, "b", -0.66320632,-2.1559392,-1.52938751,-2.04244431, "uno"),
    (0.0, -1.6861229, -1.65854506, "e", -1.67423847, 0.8780263,-3.79240934,-0.33308727, "dos"),
    (0.0, -1.5394782, -0.93665421, "e",  0.12830833,-0.7872494,-1.96153011,-1.74797093, "dos"),
    (0.0, -1.8720778, -0.94552316, "a",  1.68152500,-1.8560651,-0.79137612, 1.29563272, "diez"),
    (0.0, -1.3953547, -0.75299068, "a",  0.18064768, 0.1597608, 0.86273650, 0.90518203, "diez"),
    (0.0, -0.5646796, -0.87596445, "a", -0.09146163,-1.6087792,-1.18991479, 1.79409753, "ocho"),
    (0.0, -1.2598183, -1.53525670, "a", -0.95424774,-1.1991519,-0.74988900,-0.35575410, "uno"),
    (0.0, -0.2232940, -0.74151741, "e",  1.67096487,-0.8735829,-3.32424943, 1.94832647, "dos"),
    (0.0, -1.0772227,  0.38770887, "b", -1.09747157, 0.8858129, 0.78085555,-1.20253944, "dos"),
    (0.0, -1.4194830, -0.33282955, "e",  0.68561028, 1.2387947,-0.77660926, 1.46828768, "uno"),
    (0.0, -1.1272742,  0.49704945, "b",  0.92936102, 1.2213747, 0.64789210,-1.41798039, "uno"),
    (0.0, -1.2634169, -1.27488040, "b", -0.80357912, 0.6957749, 2.45893182,-2.80310469, "uno")
    )).toDF("label","dim1","dim2","category1","dim3","dim4","dim5","dim6", "category2")
    new DataWrapper( df_test ) 
    }
}


class BinaryOneHotTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local"
  private val appName = "test-bonehot"

  //Logs.config_logger(getClass.getName, "test.log") 
  
  com.modelicious.in.Init.initialize
  
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
   
     "Encode category1 variable with binary" should " create only 2 columns as needed to fit in binary 4 variables " in {
    val my_dw = new DataWrapper( getTestDW().data.select("category1", "dim1") )
    val conf= <onehot><columns>category1</columns><binary>true</binary></onehot>	
    val my_transform = new OneHotTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.distinct().orderBy("dim1").show()
    //println( df_out.distinct().orderBy("dim1").collect().map{ _.toSeq.mkString(",") }.mkString("\n") )
    
    df_out.distinct().count() should be (4)
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("dim1,category1_hot_1,category1_hot_2")
  }
   
   "Encode category2 variable with binary" should " create 3 columns as needed to fit in binary 5 variables " in {
    val my_dw = new DataWrapper( getTestDW().data.select("category2", "dim2") )
    val conf= <onehot><columns>category2</columns><binary>true</binary></onehot>	
    val my_transform = new OneHotTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.distinct().orderBy("dim2").show()
    //println( df_out.distinct().orderBy("dim1").collect().map{ _.toSeq.mkString(",") }.mkString("\n") )
    
    df_out.distinct().count() should be (5)
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("dim2,category2_hot_1,category2_hot_2,category2_hot_3")
  }
   
   "Encode category3 variable with binary" should " create 4 columns as needed to fit in binary 10 variables " in {
    val my_dw = new DataWrapper( getTestDW().data.select("category3", "dim3") )
    val conf= <onehot><columns>category3</columns><binary>true</binary></onehot>	
    val my_transform = new OneHotTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    df_out.distinct().orderBy("dim3").show()
    //println( df_out.distinct().orderBy("dim1").collect().map{ _.toSeq.mkString(",") }.mkString("\n") )
    
    df_out.distinct().count() should be (10)
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("dim3,category3_hot_1,category3_hot_2,category3_hot_3,category3_hot_4")
  }
   
  "Encode category1,category3  with binary" should " add 6 columns in total (less 2 from the original columns)" in {
    val my_dw = new DataWrapper( getTestDW().data )
    val conf= <onehot><columns>category1,category3</columns><binary>true</binary></onehot>	
    val my_transform = new OneHotTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    
    
    // Counting by separate categories, it should give same results than in previous tests
    val distinct_1 = df_out.select("dim1", "category1_hot_1", "category1_hot_2" ).distinct()
    distinct_1.orderBy("dim1").show()
    distinct_1.count() should be (4)
    val distinct_3 = df_out.select("dim3", "category3_hot_1","category3_hot_2","category3_hot_3","category3_hot_4").distinct()
    distinct_3.orderBy("dim3").show()
    distinct_3.distinct().count() should be (10)
    //println( df_out.distinct().orderBy("dim1").collect().map{ _.toSeq.mkString(",") }.mkString("\n") )
    
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,category2,dim3,category1_hot_1,category1_hot_2,category3_hot_1,category3_hot_2,category3_hot_3,category3_hot_4")
  }
   
    "Encode category1 and category2 variable " should " work with small datasets that have missing values for a category" in {
    val my_dw = getTestDW()
    val conf= <onehot><columns>category1,category3</columns><binary>true</binary></onehot>	
    val my_transform = new OneHotTransform
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    df_out.count() should be (40)
    df_out.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,category2,dim3,category1_hot_1,category1_hot_2,category3_hot_1,category3_hot_2,category3_hot_3,category3_hot_4")
    
    val sqlContext  =Application.sqlContext 
    import sqlContext.implicits._
    val df_small = sqlContext.createDataFrame(Seq(
    (0.0, "e", "e", "e", "e", "dos", "dos"),
    (0.0, "b", "b", "b", "b", "dos", "dos"),
    (0.0, "e", "e", "e", "e", "uno", "uno"),
    (0.0, "b", "b", "b", "b", "uno", "uno"),
    (0.0, "b", "b", "b", "b", "uno", "uno")
    )).toDF("label","dim1","category1","dim2","category2","dim3","category3")
    val dw_small = new DataWrapper( df_small ) 
    
    val df_out_small = model.transform( dw_small ).data
    df_out_small.count() should be (5)
    df_out_small.schema.map(x=> x.name).mkString(",") should be ("label,dim1,dim2,category2,dim3,category1_hot_1,category1_hot_2,category3_hot_1,category3_hot_2,category3_hot_3,category3_hot_4")
    
  }
  
   private def getTestDW() : DataWrapper = {
    val sqlContext  =Application.sqlContext 
    import sqlContext.implicits._
    val df_test = sqlContext.createDataFrame(Seq(
    (1.0, "a", "a", "a", "a", "uno", "uno"),
    (1.0, "b", "b", "b", "b", "dos", "dos"),
    (1.0, "c", "c", "c", "c", "tres", "tres"),
    (1.0, "a", "a", "d", "d", "cuatro", "cuatro"),
    (1.0, "a", "a", "c", "c", "cinco", "cinco"),
    (1.0, "c", "c", "a", "a", "seis", "seis"),
    (1.0, "a", "a", "b", "b", "siete", "siete"),
    (1.0, "b", "b", "c", "c", "ocho", "ocho"),
    (1.0, "e", "e", "d", "d", "nueve", "nueve"),
    (1.0, "e", "e", "e", "e", "diez", "diez"),
    (1.0, "a", "a", "a", "a", "diez", "diez"),
    (1.0, "c", "c", "b", "b", "nueve", "nueve"),
    (1.0, "a", "a", "e", "e", "ocho", "ocho"),
    (1.0, "c", "c", "d", "d", "siete", "siete"),
    (1.0, "e", "e", "a", "a", "seis", "seis"),
    (1.0, "b", "b", "b", "b", "cinco", "cinco"),
    (1.0, "e", "e", "a", "a", "cuatro", "cuatro"),
    (1.0, "b", "b", "b", "b", "tres", "tres"),
    (1.0, "c", "c", "c", "c", "dos", "dos"),
    (1.0, "a", "a", "a", "a", "uno", "uno"),
    (0.0, "c", "c", "c", "c", "uno", "uno"),
    (0.0, "a", "a", "a", "a", "dos", "dos"),
    (0.0, "b", "b", "d", "d", "uno", "uno"),
    (0.0, "c", "c", "b", "b", "dos", "dos"),
    (0.0, "a", "a", "a", "a", "cinco", "cinco"),
    (0.0, "a", "a", "a", "a", "cinco", "cinco"),
    (0.0, "c", "c", "c", "c", "seis", "seis"),
    (0.0, "a", "a", "e", "e", "cinco", "cinco"),
    (0.0, "b", "b", "b", "b", "uno", "uno"),
    (0.0, "e", "e", "e", "e", "dos", "dos"),
    (0.0, "e", "e", "e", "e", "dos", "dos"),
    (0.0, "a", "a", "a", "a", "diez", "diez"),
    (0.0, "a", "a", "c", "c", "diez", "diez"),
    (0.0, "a", "a", "a", "a", "ocho", "ocho"),
    (0.0, "a", "a", "a", "a", "uno", "uno"),
    (0.0, "e", "e", "e", "e", "dos", "dos"),
    (0.0, "b", "b", "b", "b", "dos", "dos"),
    (0.0, "e", "e", "e", "e", "uno", "uno"),
    (0.0, "b", "b", "b", "b", "uno", "uno"),
    (0.0, "b", "b", "b", "b", "uno", "uno")
    )).toDF("label","dim1","category1","dim2","category2","dim3","category3")
    new DataWrapper( df_test ) 
    }
   
}