package com.modelicious.in.modelos

import org.apache.spark.sql.DataFrame

package object predictor {

  type Transformable = { def transform(testData: DataFrame): DataFrame; }
  
}