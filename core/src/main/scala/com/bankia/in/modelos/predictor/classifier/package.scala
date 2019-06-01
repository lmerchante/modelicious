package com.modelicious.in.modelos.predictor

import org.apache.spark.mllib.tree.configuration.Strategy

package object classifier {
  
  implicit class StrategyImprovements(st: Strategy) {
     def strategyToString = "; Algoritmo: "+st.getAlgo().toString() +"; Impurity: "+ st.getImpurity().toString() + "; MaxBins: "+st.getMaxBins().toString() +"; MaxDepth: "+st.getMaxDepth().toString() +"; NumClasses: "+st.getNumClasses().toString() +"; UseNodeIdCache: "+st.getUseNodeIdCache().toString()
  }
  
}