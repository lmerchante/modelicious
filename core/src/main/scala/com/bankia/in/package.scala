

package com.modelicious.in

/**
 * Object with Application wide defined Constants
 */

object Constants {

  // Constants
  /**
  * Percentage of Training/Test. Default 80% for training set.
  */
  final val DEFAULT_TRAINING_RATE = 0.8
  /**
  * Number of KFolds to perform [Cross Validation](https://en.wikipedia.org/wiki/Cross-validation_(statistics))
  */
  final val DEFAULT_KFOLDS = 0
  /**
  * Percentages of training set to build the learning curve <a href="https://en.wikipedia.org/wiki/Cross-validation_(statistics)">Cross Validation</a>
  */
  final val DEFAULT_OVERFITTING_SPLITS = "0.1,0.3,0.5,0.7,0.9,1.0"
  final val DEFAULT_EXECUTOR_MEMORY = "6g"
  final val DEFAULT_EXECUTOR_CORES = "3"
  final val DEFAULT_EXECUTOR_INSTANCES = "10"
  final val DEFAULT_FEATURE_SELECTION_ENABLED = false
  final val DEFAULT_SEND_MAIL_ENABLED = false
  final val DEFAULT_GENERATE_DOCUMENTATION_ENABLED = false
  final val DEFAULT_SHOW_CONSOLE_PROGRESS = true
  final val DEFAULT_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
}
