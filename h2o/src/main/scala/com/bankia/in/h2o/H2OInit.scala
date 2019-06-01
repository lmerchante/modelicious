package com.modelicious.in.h2o

import com.modelicious.in.modelos.predictor.classifier.Classifier
import com.modelicious.in.modelos.classifier.h2o._


object H2OInit {
  
  def initialize = {
    Classifier.register("GBM", (() => new GBM()))
  }

}