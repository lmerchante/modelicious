package test.scala.modelos.h2o

import collection.mutable.Stack
import org.scalatest._

import scala.language.reflectiveCalls
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Row}

import com.modelicious.in.app.Application
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.{Statistics, SchemaTools}
import com.modelicious.in.transform._

import test.scala.h2o.TestH2OConfig


class TestGBM extends FlatSpec with Matchers with BeforeAndAfterAll {
    
  private val master = "local"
  private val appName = "test-h2o-spark"
  
  com.modelicious.in.h2o.H2OInit.initialize
  
  override def beforeAll {
    val conf = <spark App={appName} Master={master}></spark>
    val appconfig = new TestH2OConfig(conf)
    
    Application.init(appconfig)
    Application.setSeed( Some(11) )
  }

   override def afterAll {
    Application.reset()
        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
     System.clearProperty("spark.driver.port")
  }
  
  "GBM WITHOUT feature selection"  should " 32 trees provides this acuracy 1.0" in {
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DF.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0)
    val testData =  splits(1)  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = new com.modelicious.in.modelos.classifier.h2o.GBM
    val model_conf = <conf>
										 <numTrees>32</numTrees>
										 <maxDepth>4</maxDepth>
										 <nBins>32</nBins>
										 </conf>
    model.train(trainingData, model_conf/*, seed=11*/)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, VolCampanha = 4.0,
      PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, 
      Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 2.5) 
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
    
  }
  
  "GBM WITHOUT feature selection"  should " 16 trees provides this acuracy 1.0" in {
    val f = fixture

    val training_rate = 0.75 
    val splits =  f.mock_DF.randomSplit(Array(training_rate,1-training_rate), seed = 11L)
    val trainingData=splits(0)
    val testData =  splits(1)  
    trainingData.count()  should be (30)
    testData.count()  should be (10)
    val model = new com.modelicious.in.modelos.classifier.h2o.GBM
    val model_conf = <conf>
										 <numTrees>16</numTrees>
										 <maxDepth>4</maxDepth>
										 <nBins>16</nBins>
										 </conf>
    model.train(trainingData, model_conf/*, seed=11*/)
    testData.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val predictionAndLabel = model.predict(testData) 
    println("=========>   PREDICTION_AND_LABEL:")
    predictionAndLabel.collect().toList.foreach { x => println("=========>   ",x.toString())}
    val stats = com.modelicious.in.tools.Stats.compute( predictionAndLabel, testData.count() )
    val desired = Statistics(TNR = 1.0, FNR = 0.0, N = 6.0, PrecisionMinimaExclu = 0.6, VolCampaniaExclu = 6.0, VolCampanha = 4.0,
      PrecisionMinima = 0.4, Precision = 1.0, Accuracy = 1.0, TPR = 1.0, P = 4.0, FPR = 0.0, 
      Tiempo = 0.0, PrecisionExclu = 1.0, Ganancia = 2.5) 
    println(s"    - Stats: "+stats.toStringWithFields)
    compareStats(stats, desired)
    
  }
  
  def fixture =
    new {
    
    val mock_RDD=Application.sc.parallelize(List(
  (1.0, Vectors.dense(    0.8652863,  1.73472324, 2.61196898, 2.2862033,-1.07150287, 7.32487144 )),
  (1.0, Vectors.dense(    0.7515075,  0.94158552,-0.17548133, 4.7106130,-1.07631864,-2.86748436 )),
  (1.0, Vectors.dense(    1.3789220, -0.34581290,-0.47652206, 4.6195929,-0.60751062,-1.60555254 )),
  (1.0, Vectors.dense(    1.6219938,  1.34690527, 2.91987706, 1.2940924, 0.07250009,-2.53486595 )),
  (1.0, Vectors.dense(    1.5506300,  0.36159535, 1.27591763, 0.8131502, 0.98732621, 0.03395256 )),
  (1.0, Vectors.dense(    0.5021004,  2.12439235,-0.12971772, 1.3152237, 2.41643484, 4.65498439 )),
  (1.0, Vectors.dense(    0.6439939,  0.17226697,-0.31509725, 2.9848058,-0.13856282, 0.59436067 )),
  (1.0, Vectors.dense(    0.9599655, -0.78778014, 0.32350202, 3.5587566,-0.91632953, 0.32382101 )),
  (1.0, Vectors.dense(    0.5261070,  0.14599000, 1.72316457, 1.7065800, 4.61386056,-2.40938685 )),
  (1.0, Vectors.dense(    0.2825298,  0.42797728, 0.35422214, 2.9299874, 3.22235285, 1.17267871 )),
  (1.0, Vectors.dense(    1.3539028,  0.17406932, 2.64341921, 0.9531033, 2.55819471, 3.73921624 )),
  (1.0, Vectors.dense(    1.0304805,  0.76241591, 1.29864746, 1.8620801, 4.80124998, 6.97996889 )),
  (1.0, Vectors.dense(    1.2747662,  0.40467350, 1.23990399, 3.0535887,-1.18629745, 4.47835939 )),
  (1.0, Vectors.dense(    0.4619717,  0.58925781, 0.98099229, 7.3039264, 5.58129539,-1.30186541 )),
  (1.0, Vectors.dense(    0.8477440,  1.32799144, 0.63554369, 0.9064648, 3.74242406,-0.33115695 )),
  (1.0, Vectors.dense(    0.4762017, -0.16364661, 0.79463582,-0.1572651, 1.68652433, 2.60717633 )),
  (1.0, Vectors.dense(    0.6290744,  1.76017767, 2.41639121, 1.7721620, 2.39605099, 0.97343291 )),
  (1.0, Vectors.dense(    1.1025067,  0.99744416,-0.63964435, 0.2970901, 1.13716364, 1.72845550 )),
  (1.0, Vectors.dense(    0.6355241,  0.96177223, 2.33101448, 2.1003427, 0.75218600,-2.47201623 )),
  (1.0, Vectors.dense(    0.5557177, -1.03623742, 0.57050133, 1.5910596,-2.52456539,-0.12849196 )),
  (0.0, Vectors.dense(   -0.7392406, -4.25322007,-0.59648357,-0.6900998,-3.72822898,-4.45579691 )),
  (0.0, Vectors.dense(   -0.7748912, -1.32993049, 1.30207421,-1.4893851,-2.58517764,-4.79459413 )),
  (0.0, Vectors.dense(   -1.3323905, -0.54291017, 1.50203011,-2.3290005,-2.67409847,-3.28288105 )),
  (0.0, Vectors.dense(   -1.3653188, -2.30410376, 0.62716710,-1.9983614, 0.28814851, 3.50782898 )),
  (0.0, Vectors.dense(   -0.1635837, -2.29117344,-1.26903804,-2.5845768,-1.11649794,-2.73464080 )),
  (0.0, Vectors.dense(   -1.3699588, -0.09362027,-2.27379276,-0.3592954,-2.40947677,-3.94516735 )),
  (0.0, Vectors.dense(   -0.5748284, -3.79596150,-0.76368574,-3.5421039,-3.00874051,-4.71861788 )),
  (0.0, Vectors.dense(   -1.1190359, -0.76643943,-1.68002275,-4.0230556, 2.38246845,-1.39856573 )),
  (0.0, Vectors.dense(   -1.4915245, -1.26903594,-0.66320632,-2.1559392,-1.52938751,-2.04244431 )),
  (0.0, Vectors.dense(   -1.6861229, -1.65854506,-1.67423847, 0.8780263,-3.79240934,-0.33308727 )),
  (0.0, Vectors.dense(   -1.5394782, -0.93665421, 0.12830833,-0.7872494,-1.96153011,-1.74797093 )),
  (0.0, Vectors.dense(   -1.8720778, -0.94552316, 1.68152500,-1.8560651,-0.79137612, 1.29563272 )),
  (0.0, Vectors.dense(   -1.3953547, -0.75299068, 0.18064768, 0.1597608, 0.86273650, 0.90518203 )),
  (0.0, Vectors.dense(   -0.5646796, -0.87596445,-0.09146163,-1.6087792,-1.18991479, 1.79409753 )),
  (0.0, Vectors.dense(   -1.2598183, -1.53525670,-0.95424774,-1.1991519,-0.74988900,-0.35575410 )),
  (0.0, Vectors.dense(   -0.2232940, -0.74151741, 1.67096487,-0.8735829,-3.32424943, 1.94832647 )),
  (0.0, Vectors.dense(   -1.0772227,  0.38770887,-1.09747157, 0.8858129, 0.78085555,-1.20253944 )),
  (0.0, Vectors.dense(   -1.4194830, -0.33282955, 0.68561028, 1.2387947,-0.77660926, 1.46828768 )),
  (0.0, Vectors.dense(   -1.1272742,  0.49704945, 0.92936102, 1.2213747, 0.64789210,-1.41798039 )),
  (0.0, Vectors.dense(   -1.2634169, -1.27488040,-0.80357912, 0.6957749, 2.45893182,-2.80310469 )))
  .map(x=> LabeledPoint(x._1,x._2)))
  
  val sqlContext  =Application.sqlContext 
  import sqlContext.implicits._
  val mock_DF =  mock_RDD.map({ l => ( Array( l.label, l.features.toArray) ) })
                        .map({ case Array(label: Double, Array(f1: Double,f2: Double,f3: Double,f4: Double,f5: Double,f6: Double)) 
                                    => (label,f1,f2,f3,f4,f5,f6) })
                        .toDF("label", "f1","f2","f3","f4","f5","f6" )

  }
  
  def fixtureCAT =
    new {
    
    val mock_RDD=Application.sc.parallelize(List(
      (1.0, Vectors.dense( 2.0198767, 2,  1.26170385, -0.69200420 ,   1 )),
      (1.0, Vectors.dense(-0.0429528, 2, -0.75387434, -0.61591460 ,   3 )),
      (1.0, Vectors.dense( 0.6234482, 0, -0.70078186,  0.79695257 ,   0 )),
      (1.0, Vectors.dense( 1.6674906, 2, -1.51305245,  0.20249420 ,   3 )),
      (1.0, Vectors.dense( 3.3958893, 3, -1.67316553, -0.58759972 ,   2 )),
      (1.0, Vectors.dense(-1.9848325, 2,  1.76823621, -0.58432163 ,   2 )),
      (1.0, Vectors.dense( 0.6610140, 3,  0.28097523,  0.68067488 ,   1 )),
      (1.0, Vectors.dense(-0.5204037, 2,  1.36339252, -0.85965129 ,   1 )),
      (1.0, Vectors.dense( 2.1234145, 2, -1.61250367,  0.92104655 ,   1 )),
      (1.0, Vectors.dense( 2.3885333, 3, -0.64604714,  0.83165390 ,   2 )),
      (1.0, Vectors.dense( 0.5184848, 1, -0.46071354, -0.36927811 ,   0 )),
      (1.0, Vectors.dense(-0.3242116, 2,  1.85925958, -0.87663910 ,   0 )),
      (1.0, Vectors.dense( 0.7570125, 3,  0.69778424, -0.33119782 ,   1 )),
      (1.0, Vectors.dense( 0.6230792, 0, -1.23985916, -0.58664072 ,   1 )),
      (1.0, Vectors.dense( 2.5783919, 1, -0.64585276, -0.69436669 ,   1 )),
      (1.0, Vectors.dense( 2.3244642, 2,  2.50750058, -0.35985066 ,   0 )),
      (1.0, Vectors.dense( 1.1909321, 2, -1.06835358,  0.68480208 ,   3 )),
      (1.0, Vectors.dense( 0.8142680, 3, -1.18872403,  0.26401440 ,   3 )),
      (1.0, Vectors.dense( 2.3232552, 2,  1.29897212,  0.36046118 ,   2 )),
      (1.0, Vectors.dense( 3.2209430, 2,  0.86924274,  1.45599772 ,   1 )),
      (0.0, Vectors.dense(-1.6778886, 1,  2.47798765,  2.10179764 ,   2 )),
      (0.0, Vectors.dense(-1.0347568, 0,  0.73019437, -0.96782229 ,   2 )),
      (0.0, Vectors.dense(-0.7474161, 1, -1.84489142, -0.06222259 ,   0 )),
      (0.0, Vectors.dense( 1.2173158, 1,  0.67972126, -1.00354149 ,   3 )),
      (0.0, Vectors.dense( 0.5017353, 1,  1.84506037,  0.58582540 ,   1 )),
      (0.0, Vectors.dense(-1.3495410, 0,  2.86075196,  1.48140265 ,   1 )),
      (0.0, Vectors.dense(-2.2972778, 0,  0.31270816, -0.02103945 ,   2 )),
      (0.0, Vectors.dense(-0.8151330, 1, -0.48884747, -1.14958061 ,   3 )),
      (0.0, Vectors.dense(-1.8268241, 0,  0.46553495, -0.90528104 ,   2 )),
      (0.0, Vectors.dense(-0.7836532, 0,  0.93780685,  0.09447480 ,   2 )),
      (0.0, Vectors.dense(-0.9645796, 0, -0.58038021, -1.22960880 ,   1 )),
      (0.0, Vectors.dense(-2.4299197, 0,  2.58970433, -2.29359341 ,   3 )),
      (0.0, Vectors.dense(-1.1568476, 2,  1.17306158,  1.90295411 ,   2 )),
      (0.0, Vectors.dense(-2.2255842, 2, -0.38948534, -1.89880227 ,   2 )),
      (0.0, Vectors.dense( 0.1881399, 0,  1.36776217, -0.88548036 ,   2 )),
      (0.0, Vectors.dense(-1.3834669, 1, -0.07828869, -0.99925724 ,   2 )),
      (0.0, Vectors.dense(-0.7772536, 0,  1.12557576, -0.35675595 ,   0 )),
      (0.0, Vectors.dense(-0.6956076, 1, -1.67450720,  0.90475848 ,   1 )),
      (0.0, Vectors.dense( 2.0845002, 2,  0.05034472, -0.87825483 ,   1 )),
      (0.0, Vectors.dense( 0.1050412, 0, -0.41086623,  0.17947587 ,   2 )))
  .map(x=> LabeledPoint(x._1,x._2)))
  
  val sqlContext  =Application.sqlContext 
  import sqlContext.implicits._
  val mock_DF =  mock_RDD.map({ l => ( Array( l.label, l.features.toArray) ) })
                        .map({ case Array(label: Double, Array(f1: Double,f2: Double,f3: Double,f4: Double,f5: Double)) 
                                    => (label,f1,f2,f3,f4,f5) })
                        .toDF("label", "f1","f2","f3","f4","f5")

  }
 
  
  def compareStats( a: Statistics, b: Statistics ) = {
    
    classOf[Statistics].getDeclaredFields.filter(_.getName != "Tiempo").foreach { f =>
      f.setAccessible(true)
      withClue(f.getName() + ": ") {
        if( java.lang.Double.isNaN(f.get(a).asInstanceOf[String].toDouble) ) {
          java.lang.Double.isNaN(f.get(b).asInstanceOf[String].toDouble) should be (true)
        }
        else {
          math.floor(f.get(a).asInstanceOf[String].toDouble*100)/100 should be (f.get(b).asInstanceOf[String].toDouble)
        }
      }
    }
  }
  
}

