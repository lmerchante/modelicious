package com.modelicious.in.modelos.predictor.classifier


import com.modelicious.in.modelos.predictor.SVMBase

/**
 * MLLIB Implementation of Support Vector Machines
 * 
 * @see  [MLLIB SVM](https://spark.apache.org/docs/1.6.0/mllib-linear-methods.html#linear-support-vector-machines-svm)
*/


class SVM extends Classifier with SVMBase {
  override val name = "SVM"
}