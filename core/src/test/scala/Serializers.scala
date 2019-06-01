package test.scala.serializers

import collection.mutable.Stack
import org.scalatest._

import scala.language.reflectiveCalls
import org.apache.log4j.Logger

import com.modelicious.in.app.{Application, TestAppConfig}
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.{Statistics, SchemaTools}
import com.modelicious.in.transform._
import com.modelicious.in.transform.DiscretizeTransform


class TestSerializersDiscretizer extends FlatSpec with Matchers with BeforeAndAfterAll {
    
  private val master = "local"
  private val appName = "test-spark"
  
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
  
  "Test Serialization of Discretizer over dim1, dim2, dim3, dim4 and dim5"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <discretize><limit>1</limit><nCategories>3</nCategories><exclude></exclude></discretize>
    val my_transform = new DiscretizeTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    
    val state_restored = TransformerState.fromXML[discretize](model_serialized)
    val model_restored = new DiscretizeTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Serialization of Discretizer over dim1, dim2, dim3 y dim4"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <discretize><limit>1</limit><nCategories>3</nCategories><exclude>dim5</exclude></discretize>
    val my_transform = new DiscretizeTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[discretize](model_serialized)
    val model_restored = new DiscretizeTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Serialization of Discretizer over dim1, dim2 y dim3"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <discretize><limit>1</limit><nCategories>3</nCategories><exclude>dim4,dim5</exclude></discretize>
    val my_transform = new DiscretizeTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
  
    val state_restored = TransformerState.fromXML[discretize](model_serialized)
    val model_restored = new DiscretizeTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Serialization of Discretizer over dim1 y dim2"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <discretize><limit>1</limit><nCategories>3</nCategories><exclude>dim3,dim4,dim5</exclude></discretize>
    val my_transform = new DiscretizeTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[discretize](model_serialized)
    val model_restored = new DiscretizeTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Serialization of Discretizer over dim1"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <discretize><limit>1</limit><nCategories>3</nCategories><exclude>dim2,dim3,dim4,dim5</exclude></discretize>
    val my_transform = new DiscretizeTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[discretize](model_serialized)
    val model_restored = new DiscretizeTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Serialization of Discretizer over any dimensions"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <discretize><limit>1</limit><nCategories>3</nCategories><exclude>dim1,dim2,dim3,dim4,dim5</exclude></discretize>
    val my_transform = new DiscretizeTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[discretize](model_serialized)
    val model_restored = new DiscretizeTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
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

class TestSerializersOneHot extends FlatSpec with Matchers with BeforeAndAfterAll {
    
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
  
  "Test Serialization of OneHotEncoder over dim2"  should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <onehot><columns>dim2</columns></onehot>
    val my_transform = new OneHotTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    my_dw = getTestDW()
    val state_restored = TransformerState.fromXML[onehot](model_serialized)
    val model_restored = new OneHotTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (10)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
    df_out.select("dim2_hot_1").collect().toList.toString should be (df_out_restored.select("dim2_hot_1").collect().toList.toString)
    df_out.select("dim2_hot_2").collect().toList.toString should be (df_out_restored.select("dim2_hot_2").collect().toList.toString)
    df_out.select("dim2_hot_3").collect().toList.toString should be (df_out_restored.select("dim2_hot_3").collect().toList.toString)
    df_out.select("dim2_hot_4").collect().toList.toString should be (df_out_restored.select("dim2_hot_4").collect().toList.toString)
    df_out.select("dim2_hot_5").collect().toList.toString should be (df_out_restored.select("dim2_hot_5").collect().toList.toString)
  }

  "Test Serialization of OneHotEncoder over dim2 and dim4"  should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <onehot><columns>dim2,dim4</columns></onehot>
    val my_transform = new OneHotTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    my_dw = getTestDW()
    val state_restored = TransformerState.fromXML[onehot](model_serialized)
    val model_restored = new OneHotTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (10)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
    df_out.select("dim2_hot_1").collect().toList.toString should be (df_out_restored.select("dim2_hot_1").collect().toList.toString)
    df_out.select("dim2_hot_2").collect().toList.toString should be (df_out_restored.select("dim2_hot_2").collect().toList.toString)
    df_out.select("dim2_hot_3").collect().toList.toString should be (df_out_restored.select("dim2_hot_3").collect().toList.toString)
    df_out.select("dim2_hot_4").collect().toList.toString should be (df_out_restored.select("dim2_hot_4").collect().toList.toString)
    df_out.select("dim2_hot_5").collect().toList.toString should be (df_out_restored.select("dim2_hot_5").collect().toList.toString)
    df_out.select("dim4_hot_1").collect().toList.toString should be (df_out_restored.select("dim4_hot_1").collect().toList.toString)
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

class TestSerializersPCA extends FlatSpec with Matchers with BeforeAndAfterAll {
    
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
  
  "Test Serialization of PCA with 2 components"  should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <pca><components>2</components></pca>
    val my_transform = new PCATransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    my_dw = getTestDW()
    val state_restored = TransformerState.fromXML[pca](model_serialized)
    val model_restored = new PCATransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (3)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("pca_1").collect().toList.toString should be (df_out_restored.select("pca_1").collect().toList.toString)
    df_out.select("pca_2").collect().toList.toString should be (df_out_restored.select("pca_2").collect().toList.toString)
  }

  "Test Serialization of PCA with 4 components"  should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <pca><components>4</components></pca>
    val my_transform = new PCATransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    my_dw = getTestDW()
    val state_restored = TransformerState.fromXML[pca](model_serialized)
    val model_restored = new PCATransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (5)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("pca_1").collect().toList.toString should be (df_out_restored.select("pca_1").collect().toList.toString)
    df_out.select("pca_2").collect().toList.toString should be (df_out_restored.select("pca_2").collect().toList.toString)
    df_out.select("pca_3").collect().toList.toString should be (df_out_restored.select("pca_3").collect().toList.toString)
    df_out.select("pca_4").collect().toList.toString should be (df_out_restored.select("pca_4").collect().toList.toString)
  }
  
  "Test Serialization of PCA with 5 components"  should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <pca><components>5</components></pca>
    val my_transform = new PCATransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    my_dw = getTestDW()
    val state_restored = TransformerState.fromXML[pca](model_serialized)
    val model_restored = new PCATransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (6)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("pca_1").collect().toList.toString should be (df_out_restored.select("pca_1").collect().toList.toString)
    df_out.select("pca_2").collect().toList.toString should be (df_out_restored.select("pca_2").collect().toList.toString)
    df_out.select("pca_3").collect().toList.toString should be (df_out_restored.select("pca_3").collect().toList.toString)
    df_out.select("pca_4").collect().toList.toString should be (df_out_restored.select("pca_4").collect().toList.toString)
    df_out.select("pca_5").collect().toList.toString should be (df_out_restored.select("pca_5").collect().toList.toString)
  }

  "Test Serialization of PCA with None components"  should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <pca><components></components></pca>
    val my_transform = new PCATransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    my_dw = getTestDW()
    val state_restored = TransformerState.fromXML[pca](model_serialized)
    val model_restored = new PCATransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (4)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("pca_1").collect().toList.toString should be (df_out_restored.select("pca_1").collect().toList.toString)
    df_out.select("pca_2").collect().toList.toString should be (df_out_restored.select("pca_2").collect().toList.toString)
    df_out.select("pca_3").collect().toList.toString should be (df_out_restored.select("pca_3").collect().toList.toString)
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

class TestSerializersPurgeCorrelated extends FlatSpec with Matchers with BeforeAndAfterAll {

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
  
  "Purge correlated that removes dim5 " should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <purge_correlated><max_correlation>0.85</max_correlation></purge_correlated>
    val my_transform = new PurgeCorrelatedTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[purge_correlated](model_serialized)
    val model_restored = new PurgeCorrelatedTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (5)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
  }
  
  "Purge correlated that removes dim4 and dim5 " should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <purge_correlated><max_correlation>0.65</max_correlation></purge_correlated>
    val my_transform = new PurgeCorrelatedTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[purge_correlated](model_serialized)
    val model_restored = new PurgeCorrelatedTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (4)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
  }
  
  "Purge correlated that removes nothing " should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <purge_correlated><max_correlation>0.99</max_correlation></purge_correlated>
    val my_transform = new PurgeCorrelatedTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[purge_correlated](model_serialized)
    val model_restored = new PurgeCorrelatedTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (6)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Purge correlated without max_correlation " should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <purge_correlated><max_correlation></max_correlation></purge_correlated>
    val my_transform = new PurgeCorrelatedTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[purge_correlated](model_serialized)
    val model_restored = new PurgeCorrelatedTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    model_restored.state.max_correlation should be (model.state.max_correlation)
    model_restored.state.max_correlation should be (0.95)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (5)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
  }
  
  private def getTestDW() : DataWrapper = {
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

class TestSerializersPurgeInvariant extends FlatSpec with Matchers with BeforeAndAfterAll {

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

  
  "Purge invariant removing three column serialization " should " save and restore same data" in {
    
    var my_dw = getTestDW()
    val conf= <purge_invariant><min_relative_std>0.1</min_relative_std><auto>false</auto></purge_invariant>
    val my_transform = new PurgeInvariantTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[purge_invariant](model_serialized)
    val model_restored = new PurgeInvariantTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    model_restored.state.min_relative_std should be (model.state.min_relative_std)
    model_restored.state.min_relative_std should be (0.1)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (3)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
  }
    
  "Purge invariant removing one column serialization " should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <purge_invariant><min_relative_std>0.02</min_relative_std><auto>false</auto></purge_invariant>
    val my_transform = new PurgeInvariantTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[purge_invariant](model_serialized)
    val model_restored = new PurgeInvariantTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    model_restored.state.min_relative_std should be (model.state.min_relative_std)
    model_restored.state.min_relative_std should be (0.02)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (5)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
  }
  
 "Purge invariant AUTO removing three column serialization " should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <purge_invariant><min_relative_std>0.5</min_relative_std><auto>true</auto></purge_invariant>
    val my_transform = new PurgeInvariantTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[purge_invariant](model_serialized)
    val model_restored = new PurgeInvariantTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    model_restored.state.min_relative_std should be (model.state.min_relative_std)
    model_restored.state.auto should be (true)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (3)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
  }
 
  "Purge invariant with no min_relative_std provided serialization " should " save and restore same data" in {
    var my_dw = getTestDW()
    val conf= <purge_invariant><min_relative_std></min_relative_std><auto>false</auto></purge_invariant>
    val my_transform = new PurgeInvariantTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[purge_invariant](model_serialized)
    val model_restored = new PurgeInvariantTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    model_restored.state.min_relative_std should be (model.state.min_relative_std)
    model_restored.state.auto should be (false)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.size should be (6)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
  }
  
  private def getTestDW() : DataWrapper = {
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

class TestSerializersRemoveOutliers extends FlatSpec with Matchers with BeforeAndAfterAll {

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
  
  "Remove Outliers with 3*std on one column serialization " should " save and restore same data" in { 
    var my_dw = getTestDW()
    val conf= <remove_outliers><columns>dim1</columns><times>3.0</times><max_percentage_of_outliers>10</max_percentage_of_outliers></remove_outliers>
    val my_transform = new RemoveOutliers 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[remove_outliers](model_serialized)
    val model_restored = new RemoveOutliersModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    model_restored.state.times should be (model.state.times)
    model_restored.state.columns should be (model.state.columns)
    model_restored.state.max_percentage_of_outliers should be (model.state.max_percentage_of_outliers)
    df_out.count() should be (df_out_restored.count())
    df_out_restored.count() should be (10)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Remove Outliers with 3*std in all columns serialization " should " save and restore same data" in { 
    var my_dw = getTestDW()
    val conf= <remove_outliers><columns>all</columns><times>3.0</times><max_percentage_of_outliers>10</max_percentage_of_outliers></remove_outliers>
    val my_transform = new RemoveOutliers 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[remove_outliers](model_serialized)
    val model_restored = new RemoveOutliersModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    model_restored.state.times should be (model.state.times)
    model_restored.state.columns should be (model.state.columns)
    model_restored.state.max_percentage_of_outliers should be (model.state.max_percentage_of_outliers)
    df_out.count() should be (df_out_restored.count())
    df_out_restored.count() should be (8)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }

  "Remove Outliers that does not remove items when we exceed the max percentage for outliers that defaults to 0.75 serialization " should " save and restore same data" in { 
    var my_dw = getTestDW()
    val conf= <remove_outliers><columns>all</columns><times>3.0</times></remove_outliers>
    val my_transform = new RemoveOutliers 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[remove_outliers](model_serialized)
    val model_restored = new RemoveOutliersModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    model_restored.state.times should be (model.state.times)
    model_restored.state.columns should be (model.state.columns)
    model_restored.state.max_percentage_of_outliers should be (model.state.max_percentage_of_outliers)
    df_out.count() should be (df_out_restored.count())
    df_out_restored.count() should be (11)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Remove Outliers that does not remove items with no params at all serialization " should " save and restore same data" in { 
    var my_dw = getTestDW()
    val conf= <remove_outliers><columns></columns><times></times></remove_outliers>
    val my_transform = new RemoveOutliers 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    val state_restored = TransformerState.fromXML[remove_outliers](model_serialized)
    val model_restored = new RemoveOutliersModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    
    model_restored.state.times should be (model.state.times)
    model_restored.state.times should be (3.0)
    model_restored.state.max_percentage_of_outliers should be (0.75)
    model_restored.state.columns should be (model.state.columns)
    model_restored.state.columns should be (Array("_none_"))
    model_restored.state.max_percentage_of_outliers should be (model.state.max_percentage_of_outliers)
    df_out.count() should be (df_out_restored.count())
    df_out_restored.count() should be (11)
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }

  private def getTestDW() : DataWrapper = {
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
  
class TestSerializersStandarizer extends FlatSpec with Matchers with BeforeAndAfterAll {
    
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
  
  "Test Serialization of StandardScaler with MEAN and STD"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <standarize><withMean>true</withMean><withSTD>true</withSTD></standarize>
    val my_transform = new StandarTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[standarize](model_serialized)
    val model_restored = new StandarTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Serialization of StandardScaler with MEAN"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <standarize><withMean>true</withMean><withSTD>false</withSTD></standarize>
    val my_transform = new StandarTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[standarize](model_serialized)
    val model_restored = new StandarTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
 
  "Test Serialization of StandardScaler with STD"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <standarize><withMean>false</withMean><withSTD>true</withSTD></standarize>
    val my_transform = new StandarTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[standarize](model_serialized)
    val model_restored = new StandarTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }

  "Test Serialization of StandardScaler do nothing "  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <standarize><withMean>false</withMean><withSTD>false</withSTD></standarize>
    val my_transform = new StandarTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[standarize](model_serialized)
    val model_restored = new StandarTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
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

class TestSerializersSampling extends FlatSpec with Matchers with BeforeAndAfterAll {
    
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
  
  "Test Sampling with rate 1.0"  should " return the same DF with same number of rows" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate>1.0</sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[sampling](model_serialized)
    val model_restored = new SamplingTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Sampling with rate 0.5"  should " return half the DF" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate>0.5</sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[sampling](model_serialized)
    val model_restored = new SamplingTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (11)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Sampling with rate 0.75"  should " return 3/4 of the DF" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate>0.75</sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[sampling](model_serialized)
    val model_restored = new SamplingTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (14)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Sampling with rate 0.25"  should " return 1/4 of the DF" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate>0.25</sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[sampling](model_serialized)
    val model_restored = new SamplingTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (7)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Sampling with rate 0.0"  should " return an empty DF" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate>0.0</sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[sampling](model_serialized)
    val model_restored = new SamplingTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (0)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Sampling with no rate"  should " return the original DF" in {
    val my_dw = getTestDW()
    val conf= <sampling><sampling_rate></sampling_rate></sampling>
    Application.setSeed( Some(11) )
    val my_transform = new SamplingTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[sampling](model_serialized)
    val model_restored = new SamplingTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (20)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
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

class TestSerializersDrop extends FlatSpec with Matchers with BeforeAndAfterAll {
    
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
  
  "Test Drop dim1"  should " return the same DF without dim1" in {
    var my_dw = getTestDW()
    val conf= <drop><columna>dim1</columna></drop>
    val my_transform = new DropTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    
    val state_restored = TransformerState.fromXML[drop](model_serialized)
    val model_restored = new DropTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.columns.size should be (5)
    df_out_restored.columns.size should be (5)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Drop label"  should " return the same DF without label" in {
    val my_dw = getTestDW()
    val conf= <drop><columna>label</columna></drop>
    val my_transform = new DropTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[drop](model_serialized)
    val model_restored = new DropTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.columns.size should be (5)
    df_out_restored.columns.size should be (5)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
   
  "Test Drop nothing"  should " return the original DF" in {
    val my_dw = getTestDW()
    val conf= <drop><columna></columna></drop>
    val my_transform = new DropTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[drop](model_serialized)
    val model_restored = new DropTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.columns.size should be (6)
    df_out_restored.columns.size should be (6)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Drop wrong column"  should " return the original DF" in {
    val my_dw = getTestDW()
    val conf= <drop><columna>dim8</columna></drop>
    val my_transform = new DropTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML

    val state_restored = TransformerState.fromXML[drop](model_serialized)
    val model_restored = new DropTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.columns.size should be (6)
    df_out_restored.columns.size should be (6)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
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

class TestSerializersFilter extends FlatSpec with Matchers with BeforeAndAfterAll {
    
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
  
  "Test Filter  dim1 > 10 "  should " return the las 10 rows of original DF" in {
    var my_dw = getTestDW()
    val conf= <filter><condicion>dim1 > 10</condicion></filter>
    val my_transform = new FilterTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    
    val state_restored = TransformerState.fromXML[filter](model_serialized)
    val model_restored = new FilterTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (10)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.columns.size should be (6)
    df_out_restored.columns.size should be (6)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }
  
  "Test Filter  dim1 > 10 and dim4 != 10 "  should " return the las 10 rows of original DF" in {
    var my_dw = getTestDW()
    val conf= <filter><condicion>dim1 > 10 and dim4 != 10</condicion></filter>
    val my_transform = new FilterTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    
    val state_restored = TransformerState.fromXML[filter](model_serialized)
    val model_restored = new FilterTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (5)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.columns.size should be (6)
    df_out_restored.columns.size should be (6)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
  }

  "Test Filter without condition "  should " return the original DF" in {
    var my_dw = getTestDW()
    val conf= <filter><condicion></condicion></filter>
    val my_transform = new FilterTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    
    val state_restored = TransformerState.fromXML[filter](model_serialized)
    val model_restored = new FilterTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (20)
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.columns.size should be (6)
    df_out_restored.columns.size should be (6)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
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
class TestSerializersFeatureSelector extends FlatSpec with Matchers with BeforeAndAfterAll {
    
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
    val model_serialized = model.state.toXML
    
    val state_restored = TransformerState.fromXML[feature_selection](model_serialized)
    val model_restored = new FeatureSelectorTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.columns.size should be (3)
    df_out_restored.columns.size should be (3)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
  }
  
  "Test FeatureSelector MIFS selecting 4 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>mifs</method><num_features>4</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    
    val state_restored = TransformerState.fromXML[feature_selection](model_serialized)
    val model_restored = new FeatureSelectorTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.columns.size should be (5)
    df_out_restored.columns.size should be (5)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
  }
  
  "Test FeatureSelector MIFS selecting 6 on dim1, dim2, dim3, dim4 and dim5, dim6"  should " save and restore same data" in {
    val my_dw = getTestDW()
    val conf= <feature_selection><method>mifs</method><num_features>6</num_features><num_partitions>10</num_partitions></feature_selection>	
    val my_transform = new FeatureSelectorTransform 
    val model = my_transform.configure(conf).fit( my_dw )
    val df_out = model.transform( my_dw ).data
    val model_serialized = model.state.toXML
    
    val state_restored = TransformerState.fromXML[feature_selection](model_serialized)
    val model_restored = new FeatureSelectorTransformModel(state_restored)
    val df_out_restored = model_restored.transform( my_dw ).data
    df_out.count() should be (df_out_restored.count())
    df_out.columns.toList should be (df_out_restored.columns.toList)
    df_out.columns.size should be (7)
    df_out_restored.columns.size should be (7)
    df_out.select("label").collect().toList.toString should be (df_out_restored.select("label").collect().toList.toString)
    df_out.select("dim1").collect().toList.toString should be (df_out_restored.select("dim1").collect().toList.toString)
    df_out.select("dim2").collect().toList.toString should be (df_out_restored.select("dim2").collect().toList.toString)
    df_out.select("dim3").collect().toList.toString should be (df_out_restored.select("dim3").collect().toList.toString)
    df_out.select("dim4").collect().toList.toString should be (df_out_restored.select("dim4").collect().toList.toString)
    df_out.select("dim5").collect().toList.toString should be (df_out_restored.select("dim5").collect().toList.toString)
    df_out.select("dim6").collect().toList.toString should be (df_out_restored.select("dim6").collect().toList.toString)
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