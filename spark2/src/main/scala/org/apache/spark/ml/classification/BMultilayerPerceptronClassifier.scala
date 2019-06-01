/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.classification

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.shared.{HasTol, HasMaxIter, HasSeed}
import org.apache.spark.ml.{PredictorParams, PredictionModel, Predictor}
import org.apache.spark.ml.param.{IntParam, ParamValidators, IntArrayParam, ParamMap}
import org.apache.spark.ml.ann.{FeedForwardTrainer, FeedForwardTopology}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.SparkContext

/** Label to vector converter. */
private object LabelConverter {
  // TODO: Use OneHotEncoder instead
  /**
   * Encodes a label as a vector.
   * Returns a vector of given length with zeroes at all positions
   * and value 1.0 at the position that corresponds to the label.
   *
   * @param labeledPoint labeled point
   * @param labelCount total number of labels
   * @return pair of features and vector encoding of a label
   */
  def encodeLabeledPoint(labeledPoint: LabeledPoint, labelCount: Int): (Vector, Vector) = {
    val output = Array.fill(labelCount)(0.0)
    output(labeledPoint.label.toInt) = 1.0
    (labeledPoint.features, Vectors.dense(output))
  }

  /**
   * Converts a vector to a label.
   * Returns the position of the maximal element of a vector.
   *
   * @param output label encoded with a vector
   * @return label
   */
  def decodeLabel(output: Vector): Double = {
    output.argmax.toDouble
  }
}

/**
 * :: Experimental ::
 * Classifier trainer based on the Multilayer Perceptron.
 * Each layer has sigmoid activation function, output layer has softmax.
 * Number of inputs has to be equal to the size of feature vectors.
 * Number of outputs has to be equal to the total number of labels.
 *
 */
@Experimental
class BMultilayerPerceptronClassifier(override val uid: String)
  extends Predictor[Vector, BMultilayerPerceptronClassifier, BMultilayerPerceptronClassificationModel]
  with MultilayerPerceptronParams {

  def this() = this(Identifiable.randomUID("mlpc"))

  /** @group setParam */
  def setLayers(value: Array[Int]): this.type = set(layers, value)

  /** @group setParam */
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-4.
   * @group setParam
   */
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Set the seed for weights initialization.
   * @group setParam
   */
  def setSeed(value: Long): this.type = set(seed, value)

  override def copy(extra: ParamMap): BMultilayerPerceptronClassifier = defaultCopy(extra)

  /**
   * Train a model using the given dataset and parameters.
   * Developers can implement this instead of [[fit()]] to avoid dealing with schema validation
   * and copying parameters into the model.
   *
   * @param dataset Training dataset
   * @return Fitted model
   */
  override protected def train(dataset: DataFrame): BMultilayerPerceptronClassificationModel = {
    val myLayers = $(layers)
    val labels = myLayers.last
    val lpData = extractLabeledPoints(dataset)
    val data = lpData.map(lp => LabelConverter.encodeLabeledPoint(lp, labels))
    val topology = FeedForwardTopology.multiLayerPerceptron(myLayers, true)
    val FeedForwardTrainer = new FeedForwardTrainer(topology, myLayers(0), myLayers.last)
    FeedForwardTrainer.LBFGSOptimizer.setConvergenceTol($(tol)).setNumIterations($(maxIter))
    FeedForwardTrainer.setStackSize($(blockSize))
    val mlpModel = FeedForwardTrainer.train(data)
    new BMultilayerPerceptronClassificationModel(uid, myLayers, mlpModel.weights())
  }
}

/**
 * :: Experimental ::
 * Classification model based on the Multilayer Perceptron.
 * Each layer has sigmoid activation function, output layer has softmax.
 * @param uid uid
 * @param layers array of layer sizes including input and output layers
 * @param weights vector of initial weights for the model that consists of the weights of layers
 * @return prediction model
 */
@Experimental
class BMultilayerPerceptronClassificationModel private[ml] (
    override val uid: String,
    val layers: Array[Int],
    val weights: Vector)
  extends PredictionModel[Vector, BMultilayerPerceptronClassificationModel]
  with Serializable with Saveable {

  private val mlpModel = FeedForwardTopology.multiLayerPerceptron(layers, true).getInstance(weights)

  /**
   * Predict label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
   */
  override protected def predict(features: Vector): Double = {
    LabelConverter.decodeLabel(mlpModel.predict(features))
  }

  override def copy(extra: ParamMap): BMultilayerPerceptronClassificationModel = {
    copyValues(new BMultilayerPerceptronClassificationModel(uid, layers, weights), extra)
  }
  
  private case class Data(layers: Array[Int], weights: Vector)
  
  override protected def formatVersion: String = "1.0"
  
  override def save(sc: SparkContext, path: String): Unit = {
    
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    // Save metadata and Params
    DefaultParamsWriter.saveMetadata(this, path, sc)
    // Save model data: layers, weights
    val data = Data(layers, weights)
    val dataPath = new Path(path, "data").toString
    sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
  }
  
}

object BMultilayerPerceptronClassificationModel
  extends Loader[BMultilayerPerceptronClassificationModel]{

  /** Checked against metadata when loading model */
  private val className = classOf[BMultilayerPerceptronClassificationModel].getName

  override def load(sc: SparkContext, path: String): BMultilayerPerceptronClassificationModel = {
    
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

    val dataPath = new Path(path, "data").toString
    val data = sqlContext.read.parquet(dataPath).select("layers", "weights").head()
    val layers = data.getAs[Seq[Int]](0).toArray
    val weights = data.getAs[Vector](1)
    val model = new BMultilayerPerceptronClassificationModel(metadata.uid, layers, weights)

    DefaultParamsReader.getAndSetParams(model, metadata)
    model
  }
  
}
