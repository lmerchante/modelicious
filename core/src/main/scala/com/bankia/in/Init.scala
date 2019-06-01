package com.modelicious.in

import com.modelicious.in.modelos.predictor.classifier._
import com.modelicious.in.modelos.predictor.scorers._
import com.modelicious.in.transform._


object Init {

  def initialize = {
    
    // Classifiers
    Classifier.register("NaiveBayes", (() => new NaiveBayes()))
    Classifier.register("SVM", (() => new SVM()))
    Classifier.register("DecisionTree", (() => new DecisionTree()))
    Classifier.register("RandomForest", (() => new RandomForest()))
    Classifier.register("MultilayerPerceptron", (() => new MultilayerPerceptron()))
    Classifier.register("OneVsAllLogistic", (() => new OneVsAllLogistic()))
    Classifier.register("MLRandomForestC", (() => new MLRandomForestC()))
    Classifier.register("MLDecisionTreeC", (() => new MLDecisionTreeC()))
    Classifier.register("Ensemble", (() => new ClassifierEnsemble()))
    
    
    Scorer.register("SVM_SC", (() => new SVM_SC()))
    Scorer.register("NaiveBayesSC", (() => new NaiveBayesSC()))
    Scorer.register("MLRandomForestS", (() => new MLRandomForestS()))
    Scorer.register("MLDecisionTreeS", (() => new MLDecisionTreeS()))
    Scorer.register("Ensemble", (() => new ScorerEnsemble()))
    
    // Transforms
    DataTransformRegistrar.register((() => new FeatureSelectorTransform), new FeatureSelectorTransformModel(_) )
    DataTransformRegistrar.register((() => new FilterTransform), new FilterTransformModel(_) )
    DataTransformRegistrar.register((() => new SamplingTransform), new SamplingTransformModel(_) )
    DataTransformRegistrar.register((() => new DropTransform), new DropTransformModel(_) )
    DataTransformRegistrar.register((() => new DiscretizeTransform), new DiscretizeTransformModel(_) )
    DataTransformRegistrar.register((() => new PCATransform), new PCATransformModel(_) )
    DataTransformRegistrar.register((() => new StandarTransform), new StandarTransformModel(_) )
    DataTransformRegistrar.register((() => new OneHotTransform), new OneHotTransformModel(_) )
    DataTransformRegistrar.register((() => new RemoveOutliers), new RemoveOutliersModel(_) )
    DataTransformRegistrar.register((() => new PurgeInvariantTransform), new PurgeInvariantTransformModel(_) )
    DataTransformRegistrar.register((() => new PurgeCorrelatedTransform), new PurgeCorrelatedTransformModel(_) )
    DataTransformRegistrar.register((() => new SaveTransform), new SaveTransformModel(_) )
    DataTransformRegistrar.register((() => new OnesBalancerTransform), new OnesBalancerTransformModel(_) )
    DataTransformRegistrar.register((() => new MathTransform), new MathTransformModel(_) )
    DataTransformRegistrar.register((() => new RepartitionTransform), new RepartitionTransformModel(_) )
       
  }

}