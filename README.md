![picture](resources/Title.jpg)

# Spark Base App driven by configuration.

##Table of contents

* [The Application](#the-application)
* [The Input](#the-input)
* [The Models](#the-models)
	* [Naive Bayes](#naive-bayes)
	* [Decision Trees](#decision-trees)
	* [Random Forest](#random-forest)
	* [Multilayer Perceptron](#multilayer-perceptron)
	* [One Vs All](#one-vs-all)
	* [H2O GBM](#h2o-gbm)
* [Stages](#stages)
	* [Configuration files: 0.Setup.xml](#configuration-files-0setupxml)
	* [Configuration files: 10.LoadData.xml](#configuration-files-10loaddataxml)
	* [Configuration files: 20.PrepareData.xml](#configuration-files-20preparedataxml)
	* [Configuration files: SelectPredictor](#configuration-files-selectpredictor)
		* [30.SelectClassifier.xml](#30selectclassifierxml)
		* [40.SelectScorer.xml](#40selectscorerxml)
	* [Configuration files: 50.RunSQL.xml](#configuration-files-50runsqlxml)
	* [Configuration files: RunPredictor](#configuration-files-runpredictor)
		* [60.RunScorer.xml](#60runscorerxml)
		* [70.RunClassifier.xml](#70runclassifierxml)
	* [Configuration files: 80.CompareData.xml](#configuration-files-80comparedataxml)
* [Sequence example](#sequence-example)
* [Data Element](#data-element)
	* [Aditional local handles](#aditional-local-handles)
	* [Input Elements](#input-elements)
	* [Output Elements](#output-elements)
	* [Examples](#examples)
* [Table Transformations](#table-transformations)
	* [Sampling](#sampling)
	* [Remove Outliers](#remove-outliers)
	* [Purge Invariant Columns](#purge-invariant-columns)
	* [Purge Correlated Columns](#purge-correlated-columns)
	* [Feature Selection](#feature-selection)
	* [Discretize](#discretize)
	* [PCA](#pca)
	* [Standard Scaler](#standardscaler)
	* [Ones Balancer](#ones-balancer)
* [Column Transformations](#column-transformations)
	* [Filter](#filter)
	* [Drop](#drop)
	* [One Hot Encoder](#one-hot-encoder)
* [Save Model](#save-model)
* [Check Overfitting](#check-overfitting)
* [Outputs](#outputs)
	* [Logs](#logs)
	* [HDFS Files](#hdfs-files)
	* [Stats](#stats)
	* [Model Saved](#model-saved)
* [Scala and Spark Tips](#scala-and-spark-tips)
	* [Requesting Seed](#requesting-seed)
	* [Coloring Console Output](#coloring-console-output)
	* [Spark Web Server Memory Consumption](#spark-web-server-memory-consumption)
	* [Dependency Tree](#dependency-tree)
* [Developing HTML reports](#developing-html-reports)
* [Demos](#demos)
    * [Overfitting](#overfitting)
	* [Scoring](#scoring)
	* [H2O](#h2o)
	* [Testing](#testing)
* [API](#api)
* [Unit Tests](#unit-tests)
	* [Array to Tuple](#array-to-tuple)
	* [Memory DataWrapper](#memory-datawrapper)
	* [Modelos](#modelos)
	* [Stats](#stats)
	* [TestAppConfig](#testappconfig)
	* [Transformers](#transformers)
	* [Serializers](#serializers)
* [Tareas pendientes](#todo-list)
* [Known issues](#known-issues)
* [Otras Transformaciones](src/main/scala/com/modelicious/in/transform/Ideas.md)


## The Application 

The aim is to be able to Train/apply a model by just passing a set of configuration xmls to this application.

The project is managed by SBT accepting the following commands:

* sbt assembly. To generate a fat jar with all dependences
* sbt core/package. To generate a light jar with source code classes (other dependences are supposed to be already in the cluster)
* sbt core/test:compile. To compile tests
* sbt "core/testOnly test.scala.serializers.TestSerializers". To execute single test.
* sbt "h2o/testOnly test.scala.modelos.h20.TestModelos". To execute single H2O test.
* sbt core/test. To execute only core Tests
* sbt unidoc. To generate api documentation

	
Application is called with the following command:

```shell
 spark-submit  --class com.modelicious.in.BaseApp  core\target\scala-2.10\commander-core_2.10-1.0.jar ${config_folder} ${logfile} [${environment.ini}] [${varaibles.conf}] [${modelID}] [${sessionID}]
```

Where config_folder is a folder containing the configuration xmls, like this:
	
	0.Setup.xml
	10.LoadData.xml
	20.ProcessData.xml
	30.TrainClassifier.xml

See run folder as example.

Files will be loaded by alpha order, and the only constrains are that they should not start by '_'. (So appending an underscore is a fast way to comment a file) and that they have the xml extension

Each file defines a stage that will be run by the application. The application will know which stage it corresponds to by the xml root node name. 

Environment.ini is a file containing a list of parameters that can be modified there and are substituted by they key/values in the xml Stage run files. 
This is an example of how can be this file:

```
[environment]
user:A181345
tmpDB:datalabin
tmpFiles:/user/${user}/fi_cross-selling-renta-variable_gen
outputDB:datalabin
outputFiles:/user/${user}/fi_cross-selling-mixto-renta-variable_gen
urlHIVE:jdbc:hive2://c-1525.cm.es:64703/;principal=hive/c-1525.cm.es@GRUPO.CM.ES
urlIMPALA:jdbc:impala://c-1525.cm.es:64703/;principal=impala/c-1525.cm.es@GRUPO.CM.ES
workingDir:/Users/${user}/dev/gb_bi_fondos_inversion/run_env/cross-selling-renta-variable/
outputDir:/Users/${user}/dev/gb_bi_fondos_inversion/run_env/cross-selling-renta-variable/modelos/
```

These points must be considered:

* It follows a python config file structure
* The key reference in run files must be all lowercase (This is a limitation of the library used to apply the substitution
* The hierarchy in the config file is flattened to a map, using dots to concatenate the levels.
* There can be inplace substitutions.

For example, with the latter config file, to use the value tmpDB, we must use the following item in the stage file:

		${environment.tmpdb}   # note the all lowcase

Also, the value 

		outputFiles:/user/${user}/fi_cross-selling-mixto-renta-variable_gen

Will be converted to 

		outputFiles:/user/A181345/fi_cross-selling-mixto-renta-variable_gen

Before mapping to the in-application structure. Because ${user} is defined in the same file.

Environment, model and session parameters are optional. In the case of model and session, the generic modelID and SessionID will be used.

( See [Stages](#Stages) )

There are several stages defined (and more to come). Right now the following are available:

* Setup - This one is mandatory and must be always the first to be loaded 
* LoadData
* PrepareData
* SelectScorer
* SelectClassifier
* RunSQL
* RunScorer
* RunClassifier

With more to be developed:

* RunCluster
* ...

There can be more than one file running the same stage. Giving the flexibility to, for example, loaded a given data, apply different transformations, generating different data sets, and train some models with each subset. All in a single run.

## The Input

The input table, provided in the <table> section of LoadData XML file, has to meet certain restrictions. Some of them are recommended, some of them are mandatory.

### Mandatory

* If we are performing classification, there must be a column with the labeling. Be aware that there are some models that in their implementation in MLLIB only allow
binary classification, thus, binary labels {0,1} must be provided.
* The current models included in this framework are those from MLLIB and ML Spark libraries. In their versions used in this framework, those coming from MLLIB, require
the data as an RDD[LabeledPoint]. The type of data LabeledPoint is a tuple consisting in a Double, implementing the label, and a Vector, implementing the predictors.
The Vector type of data, only accepts Double values. As a result, we must avoid that the data includes categorical string variables. This limitation affects not only to 
the modeling algorithms but also to other MLLIB methods as projections, transformers, filtering... 

### Recommended

* Training algorithms can produce exceptions if missing values exist in the input table. It's recommended treating the input table to avoid missing or null values. Filter transformer can also be used with this purpopse.
* Avoid constant or correlated columns because this can impact to the accuracy of the training algorithms. Purge invariant and purge correlated transformers can be used with this purpose.
* If we want to use an algorithm that remove outliers, be aware that the mechanism implemented in this framework is a simple approximation. Always check the number of rows removed to confirm if the
algorithm is being too aggressive.
* Some algorithms work better (or only work) or become more robust with categorical variables. It's recommended to discretize the variables manually taking into account the histogram of every variable.
If this tasks is not done before using the framework, the Discretizer transformer can be used. But bear in mind that this algorithm has limitation on the number of generated bins.
* The way of overriding the limitations that some algorithms do not work with categorical variables, is expanding a categorical variable in multiple quantitative columns using some mechanism as
One Hot Encoding. The main issue of those transformations are that the number of dimensions can explode increasing the difficulty of processing the data. Be aware of the number of categories
of every variable, and if this number of categories is too large, consider re-discretize this variable.

## The Models

### Naive Bayes
As per: https://spark.apache.org/docs/1.5.0/mllib-naive-bayes.html
This model only accepts positive values. Due to, literally: ** These models are typically used for document classification. Within that context, 
each observation is a document and each feature represents a term whose value is the frequency of the term (in multinomial naive Bayes) 
or a zero or one indicating whether the term was found in the document (in Bernoulli naive Bayes). Feature values must be nonnegative.**
Thus if we want to use this model for generic datasets we need to re-quantify the dataset so that no negative values were present. The straight forward way
is using the **Discretizer** class, so that we can discretize the whole range (negative and positive values) into strictively positive discrete values.
Thiss discretization is already embedded into the train method.
If no model type is selected, then "multinomial" is the default value.

This model is available as Classifier and Scorer. Both use MLLIB implementation.

Scorer:

        <model name= "Bayes_0.1" id="NaiveBayesSC" test_overfitting="false">
            <conf>
                <lambda>0.1</lambda>
                <modelType>multinomial</modelType>
            </conf>
        </model>

Or classifier:

		<model name= "Bayes_10" id="NaiveBayes" test_overfitting="false">
			<conf>
				<lambda>10</lambda>
				<modelType></modelType>
			</conf>
		</model> 	

### Decision Trees
As per: https://spark.apache.org/docs/1.5.2/mllib-decision-tree.html
**MLlib supports decision trees for binary and multiclass classification and for regression, using both continuous and categorical features. **

For the Tree algorithm to determine which variables must be trated as categorical, some metadata has to be added to the dataset.

This metadata is automatically added by the SelectPredictor Stages, for this, an option setting the maximum number of values that a column can have to be considered as categorical must be set. Please check the SelectPredictor section for more info on this.

BE AWARE THAT CATEGORICAL VALUES HAVE TO BE CONSTRAINED BETWEEN 0 AND num_of_categories-1. That means that if the variable is discretized with N values in the source (table), it has to have values from 0 to N-1.
For example, binary columns like "(ind_recibos_importantes;2)" whose values are {0,1}, are properly configured with the syntax "(ind_recibos_importantes;2)". However, if we are discretizing a column 
using the transformer from this framework, if we configure the transfomer as:

```xml
<discretize>
	<limit>50</limit>
	<nCategories>5</nCategories>
</discretize>
```

we have to be aware that the column discretized could have values in the range {0,1,2,3,4,5}, that means that the real number of categories is 6. And this is the value that we should configure 
in the <nCategories> section of the configuration file of the SelectPredictor Stages.

In the case of DecisionTrees, if no nCategories is set, the algorithm will run anyway, but treating all variables as continuous.

Classifier:

		<model name= "DT_6_4" id="DecisionTree" test_overfitting="false">
			<conf>
				<algo>Classification</algo>
				<impurity>gini</impurity>
				<maxDepth>4</maxDepth>
				<maxBins>32</maxBins>
				<numClasses>3</numClasses>
				<useNodeIdCache>false</useNodeIdCache>
			</conf>
		</model>

Or scorer:

        <model name= "DT_10" id="MLDecisionTreeS" test_overfitting="false">
            <conf>
                <algo>Classification</algo>
                <impurity>gini</impurity>
                <maxDepth>10</maxDepth>
                <maxBins>32</maxBins>
                <numClasses>2</numClasses>
                <useNodeIdCache>false</useNodeIdCache>
            </conf>
        </model>
		

### Random Forest

As per: https://spark.apache.org/docs/1.5.1/mllib-ensembles.html#random-forests
Like decision trees, random forests handle categorical features, extend to the multiclass classification setting, do not require feature scaling,
and are able to capture non-linearities and feature interactions. MLlib supports random forests for binary and multiclass classification and for regression,
Using both continuous and categorical features. MLlib implements random forests using the existing decision tree implementation. So the same consideration about variable **categoric variables** apply to this models.
 
Scorer:

        <model name= "RF_18_16" id="MLRandomForestS" test_overfitting="false">
            <conf>
                <numTrees>18</numTrees>
                <algo>Classification</algo>
                <featureSubsetStrategy>auto</featureSubsetStrategy>
                <impurity>gini</impurity>
                <maxDepth>16</maxDepth>
                <maxBins>32</maxBins>
                <numClasses>2</numClasses>
                <useNodeIdCache>true</useNodeIdCache>
            </conf>
        </model>

Or classifier:
		
		<model name= "RF_6_4" id="RandomForest" test_overfitting="false">
			<conf>
				<numTrees>6</numTrees>
				<algo>Classification</algo>
				<featureSubsetStrategy>auto</featureSubsetStrategy>
				<impurity>gini</impurity>
				<maxDepth>4</maxDepth>
				<maxBins>32</maxBins>
				<numClasses>3</numClasses>
				<useNodeIdCache>true</useNodeIdCache>
			</conf>
		</model> 		

For the RandomForest algorithm to determine which variables must be treated as categorical, some metadata has to be added to the dataset.

This metadata is automatically added by the SelectPredictor Stages, for this, an option setting the maximum number of values that a column can have to be considered as categorical must be set. Please check the SelectPredictor section for more info on this.

BE AWARE THAT CATEGORICAL VALUES HAVE TO BE CONSTRAINED BETWEEN 0 AND num_of_categories-1. That means that if the variable is discretized with N values in the source (table), it has to have values from 0 to N-1.
For example, binary columns like "(ind_recibos_importantes;2)" whose values are {0,1}, are properly configured with the syntax "(ind_recibos_importantes;2)". However, if we are discretizing a column 
using the transformer from this framework, if we configure the transfomer as:

```xml
<discretize>
	<limit>50</limit>
	<nCategories>5</nCategories>
</discretize>
```

we have to be aware that the column discretized could have values in the range {0,1,2,3,4,5}, that means that the real number of categories is 6. And this is the value that we should configure 
in the <nCategories> section of the configuration file of the SelectPredictor Stages.

For RandomForest, the following algorithms are implemented:

As **Classifier**, binary or multiclass:

* MLLIB implementation
* ML implementation ( MLLIB underlying, but not assured to give the same results https://issues.apache.org/jira/browse/SPARK-19449 )

As **Scorer** giving the posterior probabilities for binary classification of the True class. Using the ML implementation. Note that the algorithm will not fail if we try to train it for a multiclass problem, but in this case it will output the probabilities on the 1.0 class only, whis should not be of great use.

### Support Vector Machines
As per http://spark.apache.org/docs/1.5.2/mllib-linear-methods.html#linear-support-vector-machines-svms.
The linear SVM is a standard method for large-scale classification tasks. By default, linear SVMs are trained with an L2 regularization. We also support alternative L1 regularization. In this case, the problem becomes a linear program.
The linear SVMs algorithm outputs an SVM model. Given a new data point, denoted by xx, the model makes predictions based on the value of $\textbf{w}^T\textbf{x}$. By the default, if $\textbf{w}^T\textbf{x}≥0$ then the outcome is positive, and negative otherwise.

This model is available as Classifier and Scorer. Both use MLLIB implementation.

Scorer:

        <model name= "SVM_SC_regL2_0.01" id="SVM_SC" test_overfitting="false">
            <conf>
                <numIterations>100</numIterations>
                <regParam>0.01</regParam>
                <regType>L2</regType>
            </conf>
        </model> 
		
Or classifier:

		<model name= "SVM_regL2_1" id="SVM" test_overfitting="false">
			<conf>
				<numIterations>40</numIterations>
				<regParam>1</regParam>
				<regType>L2</regType>
			</conf>
		</model> 
		
### Multilayer Perceptron
As per https://spark.apache.org/docs/1.5.2/ml-ann.html.
Multilayer perceptron classifier (MLPC) is a classifier based on the feedforward artificial neural network. MLPC consists of multiple layers of nodes. Each layer is fully connected to the next layer in the network. Nodes in the input layer represent the input data. All other nodes maps inputs to the outputs by performing linear combination of the inputs with the node’s weights ww and bias bb and applying an activation function. 
Nodes in intermediate layers use sigmoid (logistic) function:
\begin{equation}
f(z_i)=\frac{1}{1-e^{z_i}}
\end{equation}
Nodes in the output layer use softmax function:
\begin{equation}
f(z_i)=\frac{e^{z_i}}{\sum_{k=1}^{N}e^{z_k}}
\end{equation}

This one is only available as classifier:

		<model name= "NN_2_6_2" id="MultilayerPerceptron" test_overfitting="false">
			<conf>
				<layers>2,6,2</layers>
				<blockSize>128</blockSize>
				<maxIter>100</maxIter>
			</conf>
		</model>
		
The number of nodes in the network detailed with the keyword "layers" represents only the intermediate layers, because the entry and the exit layers are assigned automatically to the number of imput variables and the number of output labels.

### One Vs All
As per https://spark.apache.org/docs/1.5.2/ml-ensembles.html#one-vs-rest-aka-one-vs-all.
OneVsRest is an example of a machine learning reduction for performing multiclass classification given a base classifier that can perform binary classification efficiently. It is also known as “One-vs-Rest.”
OneVsRest is implemented as an Estimator. For the base classifier it takes instances of Classifier and creates a binary classification problem for each of the k classes. The classifier for class i is trained to predict whether the label is i or not, distinguishing class i from all other classes.
Predictions are done by evaluating each binary classifier and the index of the most confident classifier is output as label.
In our case, the basic classifier is a Loggistic Regressor:

```scala
new OneVsRest().setClassifier(new LogisticRegression)
```

This one is also available only as Classifier:

		<model name= "1VsAll" id="OneVsAllLogistic" test_overfitting="false">
			<conf>
				<numClasses>2</numClasses >
			</conf>
		</model>
		
### H2O GBM
As per https://h2o-release.s3.amazonaws.com/h2o/rel-vajda/2/docs-website/h2o-docs/data-science/gbm.html.
Gradient Boosting Machine (for Regression and Classification) is a forward learning ensemble method. 
The guiding heuristic is that good predictive results can be obtained through increasingly refined approximations.
 H2O’s GBM sequentially builds regression trees on all the features of the dataset in a fully distributed way. Each tree is built in parallel.
 
		<model name="H2O_GBM_100_5_32" id="GBM" test_overfitting="false">
			<conf>
				<numTrees>100</numTrees>
				<maxDepth>5</maxDepth>
				<nBins>32</nBins>
			</conf>
		</model>

To execute H2O with commander, H2O application must be submited as:

	h2o-submit.cmd <run configuration folder> <logfile>

H2O temp file can be modified using:

	   <h2o-settings>
			<setting name="spark.ext.h2o.client.iced.dir">C:\\Temp\\here</setting>
	   </h2o-settings>
	   
In the "0.Setup.xml" file.
		
## Stages
One stage is just a particular step in the process of Data transformation and processing. It can just mean the load of a csv file to memory, so I can be treated later in another stage, apply a given transformation to the data, like normalizing it, or train some models.
Every configuration file defines a given stage, the xml root node must be labeled like the stage it calls. 

### Configuration files: 0.Setup.xml
The setup configuration file defines the resources requested for the execution (memory per executor, number of cores per executor, and number of executors) 
and the notifications settings, basically if the output xml is forwarded by mail to a particular address. 

```xml
<spark App="${modelid}-${sessionid}" Master="yarn-client" base="${environment.tmpfiles}" checkpointFolder="${environment.tmpfiles}/cache" logLevel="INFO" ModelID="${modelid}" SessionID="${sessionid}" AllowCheckpoints="false">
	<settings>
		<setting name="spark.executor.memory">8g</setting>
		<setting name="spark.executor.cores">3</setting>
		<setting name="spark.executor.instances">100</setting>
		<!--setting name="spark.logLineage">true</setting-->
		<setting name="spark.ui.retainedStages">10</setting>  <!-- El por defecto son 1000 -->
		<setting name="spark.ui.retainedTasks">5000</setting> <!-- El por defecto son 100000 -->
	</settings>
	<notifications>
		<enabled>true</enabled>
		<email>lsanchezm@modelicious.com</email>
	</notifications>
	<documentation>
		<enabled>true</enabled>
		<format>latex</format>
	</documentation>
</spark>
```

If notification is enabled, and win32com.client, chromedriver and a valid installation of outlook are available in local PC, then an email like this is received:

![picture](resources/notifications.jpg)

The versions tested at the moment of writing this doc were:

* pywin32-220.win-amd64-py2.6.exe
* chromedriver2.26.exe

Those versions can change depending on the Arquitecture and chrome version.

If documentation is enabled, then doc file (latex or html) is generated with formated relevant information
related to the executed stages. This is the document generated for the ~/run/demos/testing example.

![picture](resources/autodocument.jpg)

### Configuration files: 10.LoadData.xml
The load data configuration file details the Hive table where the data to perform the modeling is located. We have to specify the name of the variable that stores the label to perform the supervised training.
Also in this section we can exclude variables that are not needed for the modeling, for example, customer´s copemks. We have to inform also the path where the data
will be stored in the hdfs to be used in the latter stages.

```xml
<LoadData>
	<data>
		<output url="memory:k1|hdfs:/user/A175606/prueba_discretizacion/test.dat"/>
	</data>
	<sql>
		<table>datalabin.cc_clientes_compartidos</table>
		<label>etiqueta_binaria</label>
		<columns>
			<include_only>etiqueta_binaria,posee_deposito_plazo,posee_fondos_inversion,posee_plan_pensiones</include_only>
			<exclude>
				<column>cliente</column>
				<column>sexo</column>
				<column>ind_cargo_comunidad</column>
				<column>etiqueta</column>
			</exclude>
		</columns>
	</sql>
	<limit>10000</limit>
	<drop_table>false</drop_table>
</LoadData>
```

Be aware of the differences between the parameter <include_only> and the parameter <exclude>. They are not conceived to appear together in the same XML file.
The <include_only> parameter enumerates the UNIQUE variables from the table that must remain to the next stage. This parameter is mainly used when we know a subset of relevant features.
The <exclude> parameter allows to remove columns from the table before going to the next stage. This is mainly use when we still don´t know the relevant variables
and we have to exclude some uninformative columns for the processing as the customer ID.
Note that the <include_only> method will be applied in the code before the <exclude> method (the order in the XML file is not relevant). So the "include_only" variables
have priority. However, if both parameters appear, then if any variable that appears in the <include_only> section also appears in the <exclude> section, it will be remove
before going to the next stage. Note also that if the "include_only" parameter is used, then the label must be in the list of variables to include. 
In the example above this label is "etiqueta_binaria".

The <limit> can be used to limit the size of the input data. If no limit is provided or value is set to 0 then no limit is applied.

The <drop_table> parameter can be used to drop table set in the <table> section. This setting is not recommended to be used in training steps, but it can be used for tests where dummy table is created.

### Configuration files: 20.PrepareData.xml
The process data configuration file sets the path in the HDFS where the data has been loaded and we can profit also this file to define some transformations on the variables. Today there are three possibilities: feature_selection, filter and drop. The next step will be create two sections column transformations (with drop, filter, normalization, discretization, codification,...) and a whole table transformations (feature selection, projections, discretization,....)

```xml
<PrepareData>
	<data>
		<input url="memory:k1"/>
		<output url="memory:k2|hdfs:preparenofs.dat"/>
	</data>
	<transformations>
	    <sampling>
			<sampling_rate>0.01</sampling_rate>
		</sampling>
		<drop><columna>seg_valor</columna></drop>
		<filter><condicion>cod_postal is not null</condicion></filter>
		<filter><condicion>ingresos_mensuales is not null</condicion></filter>
		<filter><condicion>porcingexternosestimados is not null</condicion></filter>
		<filter><condicion>estado_civil is not null</condicion></filter>
		<filter><condicion>label is not null</condicion></filter>
		<purge_invariant>
			<min_relative_std>0.1</min_relative_std>
			<auto>true</auto>
		</purge_invariant>
		<purge_correlated>
			<max_correlation>0.85</max_correlation>
		</purge_correlated>
		<remove_outliers>
			<!--columns>ingresos_mensuales,trforgmodelicioussamecustomer,maxmovextplanpens12m</columns-->
			<columns>all</columns>
			<times>3.0</times>
			<max_percentage_of_outliers>0.20</max_percentage_of_outliers>
		</remove_outliers>		
		<discretize>
			<limit>50</limit>
			<nCategories>5</nCategories>
			<exclude>cod_postal, avgingresos12m,</exclude>
		</discretize>
		<onehot force_checkpoint="true">
			<binary>true</binary>	<columns>cod_sexo,cod_estado_civil,segmento_valor,segmento_actividad,segmento_socioeconomico,segmento_por_relacion,segmento_marketing,clasificacion_nivel_renta,codigo_segmento_interno,tenenc_accionmodelicious_mt,tenenc_fondoinvers_mt,tenenc_planpensiones_mt</columns>
		</onehot>
		<standarize>
			<withMean>true</withMean>
			<withSTD>true</withSTD>
		</standarize>
		<feature_selection>
			<method>mifs</method>
			<num_features>10</num_features>
			<num_partitions>100</num_partitions>
		</feature_selection>
		<pca>
			<components>5</components>
		</pca>
	</transformations>
</PrepareData>
```

### Configuration files: SelectPredictor

There are 2 similar configuration files, depending on which kind of predictor we want to train:
	
* __SelectClassifier__: Returns a predicted label, it can be multiclass, depending on the underlying algorithm
* __SelectScorer__: Returns the posterior probability to get a label 1.0. Right now, this limit the algorithms to binary classification

The main difference between both configuration files are:

* The root element name. Either SelectClassifier or SelectScorer
* The allowed algorithms to use. 

There options are common for both selectors:

* __kfolds__: number of folds for cross validation
* __training_rate__: rate between train and test data
* __overfitting_splits__: Splits for overfitting calculation
* __nCategories__: Maximum number of different values for a column of the dataset that will make the Tree algorithms to treat it as categorical. This value will be used to decide which variables are quantitative and which ones are qualitative.
And for those that are qualitative, its number of categories will be computed to optimize the Tree algorithm.

Note that, when using __Tree Algorithms__, the __nCategories__ option _must_ be set, it adds some metadata to the input dataset needed for these algorithms to select which variables will be trated as categoric. If this metadata is missing, the behaviour of the algorithms will be the following:

* For __MLLIB__ based algorithms:
	- __RamdomForest__ ( Classifier )
	- __DecisionTree__ ( Classifier )
	It will trat all variables as continuos
* For ML based algorithms:
	- __MLRandomForestC__ ( Classifier )
	- __MLRandomForestS__ ( Scorer )
	- __MLDecisionTreeC__ ( Classifier )
	- __MLDecisionTreeS__ ( Scorer )
	It will fail with error, as this metadata is a must for ML based algorithms

		<SelectClassifier>
			<options>
				<kfolds>5</kfolds>
				<training_rate>0.75</training_rate>
				<overfitting_splits>0.001,0.01,0.1,1</overfitting_splits>
				<nCategories>5</nCategories>
			</options>
			<data>
				<input url="memory:k2"/>
			</data>
			<models>
			</models>
		</SelectClassifier>	
	
#### 30.SelectClassifier.xml
The train classifier configuration file allows to state different training sessions with different configurations. Every model has different settings.
Here we have an example that uses cross validation with 5 folds, training rate of 75% and train 7 different models: 

- SVM with cuadratique regularization
- SVM with lasso regularization
- Random Forest 
- Decision Trees
- Naive Bayes with a smoothing term
- Multilayer Perceptron Networks
- One versus All logistics classifiers.


```xml
<SelectClassifier>
	<options>
		<kfolds>5</kfolds>
		<training_rate>0.75</training_rate>
		<overfitting_splits>0.001,0.01,0.1,1</overfitting_splits>
		<nCategories>5</nCategories>
	</options>
	<data>
		<input url="memory:k2"/>
	</data>
	<models>
		<model name= "SVM_regL2_1" id="SVM" test_overfitting=false>
			<conf>
				<numIterations>40</numIterations>
				<regParam>1</regParam>
				<regType>L2</regType>
			</conf>
		</model> 
		<model name= "SVM_regL1_1" id="SVM" test_overfitting=false>
			<conf>
				<numIterations>100</numIterations>
				<regParam>1</regParam>
				<regType>L1</regType>
			</conf>
		</model>
		<model name= "RF_6_4" id="RandomForest" test_overfitting="true">
			<conf>
				<numTrees>6</numTrees>
				<algo>Classification</algo>
				<featureSubsetStrategy>auto</featureSubsetStrategy>
				<impurity>gini</impurity>
				<maxDepth>4</maxDepth>
				<maxBins>32</maxBins>
				<numClasses>3</numClasses>
				<useNodeIdCache>true</useNodeIdCache>
			</conf>
		</model> 
		<model name= "DT_6_4" id="DecisionTree" test_overfitting=false>
			<conf>
				<algo>Classification</algo>
				<impurity>gini</impurity>
				<maxDepth>4</maxDepth>
				<maxBins>32</maxBins>
				<numClasses>2</numClasses>
				<useNodeIdCache>false</useNodeIdCache>
			</conf>
		</model>
		<model name= "Bayes_10" id="NaiveBayes" test_overfitting=false>
			<conf>
				<lambda>10</lambda>
				<modelType>multinomial</modelType>
			</conf>
		</model>  
		<model name= "NN_2_6_2" id="MultilayerPerceptron" test_overfitting=false>
			<conf>
				<numClasses>2</numClasses>
				<layers>6</layers>
				<blockSize>128</blockSize>
				<maxIter>100</maxIter>
			</conf>
		</model>
		<model name= "1VsAll" id="OneVsAllLogistic" test_overfitting=false>
			<conf>
			<numClasses>2</numClasses >
			</conf>
		</model>
	</models>
</SelectClassifier>
```

#### 40.SelectScorer.xml
This train scorer configuration file allows to state different training sessions with different configurations. Every model has different settings.
Here we have an example that uses cross validation with 5 folds, training rate of 75% and train 4 different models: 

- SVM with cuadratique regularization
- SVM with lasso regularization
- Decision Trees/Random Forest
- Naive Bayes with a smoothing term

```xml
<SelectScorer>
    <options>
        <kfolds>5</kfolds>
        <training_rate>0.75</training_rate>
        <overfitting_splits>0.001,0.01,0.1,1</overfitting_splits>
        <nCategories>5</nCategories>
    </options>
    <data>
        <input url="csvgz:datos.checkpoint.trainmatrix.csvgz"/>
    </data>
    <models>
        <model name= "RF_12_16" id="MLRandomForestS" test_overfitting="false">
            <conf>
                <numTrees>12</numTrees>
                <featureSubsetStrategy>auto</featureSubsetStrategy>Q
                <impurity>gini</impurity>
                <maxDepth>16</maxDepth>
                <maxBins>32</maxBins>
                <useNodeIdCache>true</useNodeIdCache>
            </conf>
        </model>
        <model name= "DT_10" id="MLDecisionTreeS" test_overfitting="false">
            <conf>
                <algo>Classification</algo>
                <impurity>gini</impurity>
                <maxDepth>10</maxDepth>
                <maxBins>32</maxBins>
                <numClasses>2</numClasses>
                <useNodeIdCache>false</useNodeIdCache>
                <categoricalColumns></categoricalColumns>
            </conf>
        </model>
        <model name= "SVM_regL2_0.01" id="SVM_SC" test_overfitting="false">
            <conf>
                <numIterations>100</numIterations>
                <regParam>0.01</regParam>
                <regType>L2</regType>
            </conf>
        </model> 
        <model name= "Bayes_100" id="NaiveBayesSC" test_overfitting="false">
            <conf>
                <lambda>100</lambda>
                <modelType>multinomial</modelType>
            </conf>
        </model>
        <model name= "Bayes_10" id="NaiveBayesSC" test_overfitting="false">
            <conf>
                <lambda>10</lambda>
                <modelType>multinomial</modelType>
            </conf>
        </model>
    </models>
</SelectScorer>
```

### Configuration files: 50.RunSQL.xml

This configuration file allows to caracterize customers according to some variables that will be use in the
RunPredictor stage to generate a prediction.

```xml
<RunSQL>
	<data>
		<output url="memory:k1"/>
	</data>
    <label>cliente</label>
	<sql>
select cliente,
posee_deposito_plazo,posee_fondos_inversion,saldofinal_pasivo_titprinc
FROM ${variables.tabla_dmtpe}  
	</sql>
</RunSQL>
```

### Configuration files: RunPredictor

What was explained in section [Configuration files: SelectPredictor](#configuration-files-selectpredictor)
can also be applied to this section. Not on a train dimension but on a deploy point of view. 
That means that running a trained model must use one stage or another depending on the family of 
models used (Classifier or Scorers)

#### 60.RunScorer.xml

Note that the model section of this configuration file must point to an existing trained model.

```xml
<RunScorer>
  <data>
    <input url="memory:k1:repartition=40"/>
    <output url="scores:${environment.outputdb}.BGMD_IN_MATRIZ_PROPENSIONES:json=${environment.outputdir}/${modelid}-${sessionid}.json&amp;ic=${environment.outputdir}/${modelid}-${sessionid}.ic"/>
  </data>
  <model url = "local:${environment.workingdir}/models/MLRandomForestS/RF_6_4/" balancing="1.0"/>
</RunScorer>
```

#### 70.RunClassifier.xml

Note that the model section of this configuration file must point to an existing trained model.

```xml
<RunClassifier>
  <data>
    <input url="memory:k1:repartition=40"/>
    <output url="scores:${environment.outputdb}.BGMD_IN_MATRIZ_PROPENSIONES:json=${environment.outputdir}/${modelid}-${sessionid}.json&amp;ic=${environment.outputdir}/${modelid}-${sessionid}.ic"/>
  </data>
  <model url = "local:${environment.workingdir}/models/MLRandomForestC/RF_6_4/" balancing="1.0"/>
</RunClassifier>
```

### Configuration files: 80.CompareData.xml

This Stage is basically used in functional testing. This stage allows to compare some frozen data with the data generated by a predictor.
It looks like this:

```xml
<CompareData>
  <data>
    <input1 url="lcsvgz:${environment.workingdir}/predictions.csvgz"/>
    <input2 url="lcsvgz:${environment.workingdir}/predictions2.csvgz"/>
  </data>
</CompareData>
```

Check [Demos](#demos) section for more examples.

## Sequence example

This structure allows to implement the next flow on the Model Selection phase:

![picture](resources/flow.jpg)

The result of previous flow is a XML file with all the statistics required to choose which combination of Transformers and Models provide the best prediction capabilities.
Once that we have the right settings, we have to train the definitive model. This can be done on two ways to be assessed with Alberto:

* Using the same code, selecting a single repetition (no cross validation) and unique model configuration, the code will be able to detect that this configuration must lead to save a model serialized. We must decide if the definitive model is train with the whole input table or train/test split is also performed.
* Create a new block that takes a single parametrization to generate save a model serialized. We must decide if the definitive model is train with the whole input table or train/test split is also performed.

![picture](resources/flow_train.jpg)

The model saved does not only contain the model settings, but all the transformations applied for the training. That means that in the latter phase of deploying the model on a list of customers characterized, we would only need to select the table with the clients characterized and the path were the trained model was saved.

![picture](resources/flow_deploy.jpg)

There is an important fact that must be taken into consideration. If we are saving a model that uses several transformation like:

	1. Load all variable from table
	2. Remove Outliers
	3. Remove invariant columns
	4. Discretize
	5. Apply one hot encoding
	6. Standarizer
	7. Feature selection

Then we train some models and save their results.
Those saved models will keep all the transformations history so that this can be replicated on prediction time. That means, that on prediction time, we have to implement exactly the same query that we run during training.
This cannot be the best of the ideas. Imagine a situation where original query has 200 dimensions, and the final model, after feature selection, only train 20 variables.
The saved model will expect a dataframe with 200 dimensions to be able to reproduce the same transformations (Feature Selection Transformer wont re-run this algorithm, but drop all variables but the selected ones kept on the metadata of the saved model).
Querying about 200 variables and transforming a 200 variables dataframe can be hard to process. It´s recommended, once that we have the final 20 variables to be selected, re-run the train stage selecting only the 20 relevant variables in the LoadData section.
Then all the transformation will apply exclusively to those 20 variables. Obviously, there is no need to run a new feature selection transformer if we are already running a query with those 20 variables.

## Data element

The Data element configures the input and output of each stage. The number of possible elements depends on each particular stage.
Right now, LoadData only accepts an output element. TrainClassifier only an output element and ProcessData both.
In the future there could be other optios ( Like a JoinTables Stage that joins 2 inputs to an output)

Both Input and output elements have an url attribute that identifies the format and location of the data to read/store.

Each handler is configured by a `(key:location:options)` tuple that is described for each case in the table below:

|Key|Location|Options|Description|Comments|
|-----|----|--------|----|----|
|memory|It´s just a key for the map that will store the data internally| TODO: would be good to add the persistence of the data |The data is stored in an internal Map. But this data will only last for the next stage (Couldnt find other way to ensure the ata is removed from memory and we do not have OOM problems do to this)| To be deprecated probably|
|hdfs|hdfs url|  |Parquet File||
|csv|hdfs url| All the available here https://github.com/databricks/spark-csv#features. The most importants are:<br/> <ul><li>delimiter. By default it uses ,. With this option we can change it to whatever (But pipe or & as it would clash with internal URI handling)</li><li> codec: compression codec to use when saving to file. Should be the fully qualified name of a class implementing org.apache.hadoop.io.compress.CompressionCodec or one of case-insensitive shorten names (bzip2, gzip, lz4, and snappy). <br/> Defaults to no compression when a codec is not specified.</li></ul>|||
|csvgz|hdfs url|Same as csv, just includes by default the compression to gz.|Writes the data to a csv.gz||
|db|`${schema}.${table}`|partitionBy: comma separated list of columns used to partition the table. Order is important|Inserts Data into given table|Only for **Output**|
|scores|`${schema}.${table}`|Same than DB, but it adds automaticaly the partitions modelID and SessionID|Inserts Data into given table|Only for **Output**|


### Aditional local handles

All these handles assume the data/file are to be written/read to/from HIVE.
To be able to work with local files, an additional set of handles has been created, based in the remote ones, but they up/down load the data from local system. 
A temporal partition under the temp folder of the application is used for the copy of these files, and it is transparent for the user.
The following local handles are available:

* lcsv
* lcsvgz
* lhdfs ( Independently of the name, it is just a parket file )

The given url must be, then, a local filesystem URI. Instead of a hive url.

These handles are just decorators to the original ones, and the behavior is the same than their remote counterparts. They work both for read and write.

### Input Elements

There is a common option for all the Input elements.
They allow to force a repartition on the data being read.
It is recommended to select values known to not exceed the limits given by the executors memory or configuration of the node ( Cant be chuncks of an RDD greater than 2GB,for example ) 

Note also that a repartition will probably cause a shuffle and load the cluster network while moving data between executors.

If unsure, it is better to let Spark and the DataFrame API to decide. But selecting a good partition value can greatly speed up the processes.

### Output Elements

The output element can chain more than one handler, separated by pipes ( `|` ) and the data will be stored on all the possible locations.

### Examples

* Example 1:

```xml
<input url="memory:k2:repartition=10"/>
```

* Example 2:

Will load the Data stored in memory identified by the key _k2_ and will force a repartition to 10 before continuing with the process.

```xml
<output url="memory:k1|hdfs:/user/A181345/cc/original.dat|csv:/user/A181345/cc/original.csv.bz:codec=bzip2&amp;delimiter=^|csv:/user/A181345/cc/original.csv|csvgz:/user/A181345/cc/original.csv.gz:delimiter=#"/>
```

Will Create the following data elements:

	* ´memory:k1´: An in memory DataFrame with key k1 that will last for the next stage
	* ´hdfs:/user/A181345/cc/original.dat´:a Parquet file under the given hdfs location ´/user/A181345/cc/original.dat´
	* ´csv:/user/A181345/cc/original.csv.bz:codec=bzip2&amp;delimiter=^´:a bzipped csv under ´/user/A181345/cc/original.csv.bz´ and with ´^´ as delimiter
	* ´csv:/user/A181345/cc/original.csv´:an uncompressed csv under ´/user/A181345/cc/original.csv´
	* ´csvgz:/user/A181345/cc/original.csv.gz:delimiter=#´:a gzipped csv with ´#´ as delimiter and under ´/user/A181345/cc/original.csv.gz´ hdfs folder

* Example Section SelectScorer:

		<SelectScorer>
			<options>
				<kfolds>5</kfolds>
				<training_rate>0.75</training_rate>
				<overfitting_splits>0.001,0.01,0.1,1</overfitting_splits>
				<nCategories>5</nCategories>
				<save>
				  <local>${environment.outputdir}</local>
				  <remote>${environment.outputfiles}</remote>
				</save>
			</options>
			<data>
				<input url="csvgz:datos.checkpoint.trainmatrixvar20.csvgz:repartition=12"/>
			</data>
			<models>
				......
			</models>
		</SelectScorer>
		
* Example Section RunScorer:

		<RunScorer>
		  <data>
			<input url="memory:k1:repartition=40"/>
			<output url="scores:${environment.outputdb}.BGMD_IN_MATRIZ_PROPENSIONES:json=${environment.outputdir}/${modelid}-${sessionid}.json&amp;ic=${environment.outputdir}/${modelid}-${sessionid}.ic"/>
		  </data>
		  <model url = "local:${environment.workingdir}/../models/MLDecisionTreeS/DT_10" nCategories="5" balancing="1.0"/>
		</RunScorer>
		
* Example Section RunSQL:

		<RunSQL>
			<data>
				<output url="memory:k1"/>
			</data>
		  <label>cliente</label>
			<sql>
			.....
			</sql>
		</RunSQL>
	
* Example Section LoadData:

		<LoadData>
			<data>
				<output url="memory:k1"/>
			</data>
			<sql>
				<table>datalabin.fond_inv_down_genericos_training</table>
				<label>label</label>
				<columns>
					<include_only>label,posee_fondos_inversion,saldofinal_pasivo_totastit,im_suscfondinvermes_mt,im_reembfondinvermes_mt,im_totcomismes_mt</include_only>
				</columns>
			</sql>
		</LoadData>

* Example Section ProcessData:

		<PrepareData>
			<data>
				<input url="memory:k1"/>
				<output url="memory:k2|hdfs:datos.checkpoint.trainmatrixvar20.dat|csvgz:datos.checkpoint.trainmatrixvar20.csvgz"/>
			</data>
			<transformations>
				<remove_outliers force_checkpoint="true">
					<columns>all</columns>
					<times>3.0</times>
					<max_percentage_of_outliers>0.05</max_percentage_of_outliers>
				</remove_outliers>	
				<save url="csvgz:datos.checkpoint.post_outliers.data.csvgz"/>
				<discretize>
					<limit>50</limit>
					<nCategories>5</nCategories>
				</discretize>	
				<save url="csvgz:datos.checkpoint.post_discretization.data.csvgz"/>	
				<standarize>
					<withMean>true</withMean>
					<withSTD>true</withSTD>
				</standarize>
				<pca><components>10</components></pca>	
				<save url="csvgz:datos.checkpoint.post_pca.data.csvgz"/>					
			</transformations>
		</PrepareData>
		
In this case, the output datos.checkpoint.post_pca.data.csvgz will be exactly the same as any of the outputs of the whole section: 

	* memory:k2
	* hdfs:datos.checkpoint.trainmatrixvar20.dat
	* csvgz:datos.checkpoint.trainmatrixvar20.csvgz 
		
* Example Section PrepareData to create DB in Hive:

		<PrepareData>
			<data>
				<input url="lcsvgz:${environment.workingdir}/test_data.csvgz"/>
				<output url="db:${environment.tmpdb}.test_table_${modelid}_${sessionid}:dropFirst=true"/>
			</data>
			<transformations>
			</transformations>
		</PrepareData>

This date loaded on the shape of a table, can be accessed with a configuration file like this one:

		<LoadData>
			<data>
				<output url="memory:test_data"/>
			</data>
			<sql>
				<table>${environment.tmpdb}.test_table_${modelid}_${sessionid}</table>
				<label>label</label>
				<columns>
					<include_only>label,antiguedad_hasta_la_caracterizacion,edad,marca_modelicious_sin_comisiones,posee_deposito_plazo,posee_tarj_credito,saldofinal_pasivo_titprinc,numero_lineas_productos,margen_financiero_titularidad_principal_interanual,margen_ordinario_tp,saldo_medio_mensual_total_pasivo_coefvarporcentual,saldo_medio_mensual_total_pasivo_tendrelativa01m,saldo_medio_mensual_total_pasivo_tendrelativa03m,importe_lcp,tiene_lcp,nu_opernoautomoficmes_mt,nu_tjcredactv_mt,nu_ctrahorrplz_mt,im_saldmedmensuahorrvis_mt,segmento_valor,segmento_marketing</include_only>
				</columns>
			</sql>
		</LoadData>

	
* Example Section CompareData applied to Functional Testing:

		<CompareData>
		  <data>
			<input1 url="lcsvgz:${environment.workingdir}/predictions.csvgz"/>
			<input2 url="lcsvgz:${environment.workingdir}/predictions2.csvgz"/>
		  </data>
		</CompareData>

In this case, predictions.csvgz is frozen test data and predictions2.csvgz is the generated prediction by a RunScorer section like this:

		<RunScorer>
		  <data>
			<input url="memory:test_data:repartition=40"/>
			<output url="lcsvgz:${environment.workingdir}/predictions2.csvgz"/>
		  </data>
		  <model url = "local:${environment.workingdir}/models/MLDecisionTreeS/DT_10" nCategories="5" balancing="1.0"/>
		</RunScorer>

		
## Table Transformations

Be aware that during this MODELING phase, on behalf of simplicity, we are applying the transformations to the whole table, before splinting in TRAINNIG and TEST.
Once that we have chose the right model with the right parametrization, we have to possibilities:

* Reuse this phase using a single K-fold, so that only a model is trained, and save the generated model. At the same time that we save the model itself, 
we need to save some parameters of the transformations, for example, the mean and std vectors if any StandarScaler transformation is applied, or the labels that allow to build a StringIndexer for the OneHotEncoding transformation.
* Build a new phase where we will train with the whole data available, and save the model itself with the same transformation settings that we described in the point above.

### Sampling
This modules is not a proper table transformation but a sub-sampling mechanism with a single parameter:

		<sampling>
			<sampling_rate>0.05</sampling_rate>
		</sampling>

A sampling rate equal to 1.0 means no sub-sampling.

### Remove Outliers
This module allows to filter rows in the dataframe whose values in certain columns exceed X times the value of the column standard deviation.

```xml
<remove_outliers>
	<!--columns>ingresos_mensuales,trforgmodelicioussamecustomer,maxmovextplanpens12m</columns-->
	<columns>all</columns>
	<times>3.0</times>
	<max_percentage_of_outliers>0.20</max_percentage_of_outliers>
</remove_outliers>	
```

If we want to apply this rule to all the columns or:

```xml
<remove_outliers>
	<columns>estado_civil,porcingexternosestimados</columns>
	<times>3.0</times>
	<max_percentage_of_outliers>0.20</max_percentage_of_outliers>
</remove_outliers>
```

If we just want to apply it to a limited number of columns. Be aware that this transformation is only applicable to numeric columns.
If we want to apply different standard deviation multiplier to different columns, we can do it defining several <remove_outliers> transformations.
The parameter max_percentage_of_outliers states that if the samples selected using the standard deviation criterion are too many (higher than the ratio specified),
 then, nothing is removed. This feature intends to avoid situations like in binary variables where hundreds of thousands of elements can be considered outliers if we don't pay attention.

### Purge Invariant Columns
Relative Standard Deviation. Enter the minimum relative standard deviation to use in determining invariant variables. If a variables relative 
standard deviation is less than or equal to the number entered here, the variable will be removed from the data set. Relative standard deviation 
is calculated as the standard deviation divided by its mean.
If no value is entered, then 0.0 is considered as default value, so no columns will be purged.

```xml
<purge_invariant>
	<min_relative_std>0.1</min_relative_std>
	<auto>false</auto>
	<exclude>segmento_marketing</exclude>
</purge_invariant>
```

If auto is set to "true" then, entered "min_relative_std" value is ignored and computed as ten times portion the relative standard deviation of the label.
More information can be found here [https://en.wikipedia.org/wiki/Coefficient_of_variation].
Be aware that some numeric issues could appear when mean is zero or when mean and std are zero. Those situations are treated as:

```scala
	if (std==0 & mean==0) 0.0 else if (mean==0) std/abs(mean+0.0000001) else std/abs(mean)
```

So that invariant situations (std=0) result in zero relative standard deviation.

Be aware that for qualitative variables in numeric format, the value of relative standard deviation can lead to misunderstandings. For example, variable "segmento_marketing" is
a qualitative variable that only takes values from 361 to 364... this could be considered a very low relative standard deviation, but the meaning of those values must not be taken as quantitative.

### Purge Correlated Columns
We use Pearson correlation to detect redundant variables. Keep in mind that the Pearson correlation evaluates the linear relationship between two continuous variables. A relationship is linear when a change in one variable is associated with a proportional change in the other variable. So, more complex correlations are not discovered.

```xml
<purge_correlated>
	<max_correlation>0.6</max_correlation>
</purge_correlated>
```

Be aware that constant columns equaled to zero would result into NaN columns and rows in the correlation matrix due to their zero mean and std. That's why, for the proper functioning of this transformation, should be executed
after a "Purge Invariant Columns" transformation that remove those zero columns. If this transformation is not run, the "Purge Correlated Columns" will not fail, but zero columns will not be marked as correlated and wont be removed.

Example:

![picture](resources/correlation.jpg)

Our algorithm removes the "label" column, and computes the correlation matrix with the remaining variables. Then it checks only the upper triangle matrix and marks all columns with at least one correlation value that exceeds the max_correlation setting.
Those marked column are removed from the output dataframe.

Be aware that when constant columns are present, correlation with those columns will result in NaN. To avoid this issue, transformer Purge Invariant Columns must be run in the first place.

### Feature Selection
The module included in this project is documented in reference: "Conditional Likelihood Maximization: A Unifying Framework for Information Theoretic Feature Selection" by Gavin Brown, Adam Pocock, Ming-Jie Zhao, Mikel Luján. 
With the related [GITHUB repository](https://github.com/LIDIAgroup/SparkFeatureSelection/blob/master/README.md).
Configuration file is taking three parameters:

```xml
<feature_selection>
	<method>mifs</method>
	<num_features>10</num_features>
	<num_partitions>100</num_partitions>
</feature_selection>
```

Where method can take one of those values:

```scala
val MIM = "mim"
val MIFS = "mifs"
val JMI  = "jmi"
val MRMR = "mrmr"
val ICAP = "icap"
val CMIM = "cmim"
val IF   = "if"
```
 
### Discretize
Configuration file is taking two parameters:

```xml
<discretize>
	<limit>50</limit>
	<nCategories>5</nCategories>
</discretize>
```

* limit: is the threshold in the number of possible values of every column to trigger discretization
* nCategories: is the maximum number of categories that will be used to discretized a column. The final number of categories depends on the range and number of different values of the variable to be discretized.

The output of a discretize column is constrained between 0 and nCategories-1. This can be tested with this example code:

	import org.apache.spark.ml.feature.{ QuantileDiscretizer, Bucketizer } 
	import org.apache.spark.sql.functions._
	val df=sqlContext.sql("select * from datalabin.fond_inv_cross_todos_training limit 1000")
	val toDouble = udf[Double, String]( _.toDouble) 
	val featureDf = df.withColumn("comisiones_todas_titularidades_interanual", toDouble(df("comisiones_todas_titularidades_interanual")))     
	val discretizer = new QuantileDiscretizer().setInputCol("comisiones_todas_titularidades_interanual").setOutputCol("comisiones_todas_titularidades_interanual_cat").setNumBuckets(5)
	val bucket=discretizer.fit(featureDf)
	val df2=bucket.transform(featureDf)
	df2.select(df2("comisiones_todas_titularidades_interanual_cat")).distinct.collect()
	
IMPORTANT: When selecting N Buckets, the number of potential categories is N+1. The range of those categories goes from 0 to N. 
This feature is relevant for the situation where I want to discretize a categorical variable but whose categories have to be constrained between 0 and N (as happens with random forest or decision trees).
For example, variable "segmento_valor" is categorical whose values can be {380,381,382,392,393,394,395}. If we need to transform those values into {0,1,2,3,4,5,6} with this transformers, If we select 
just 5 buckets:

		val discretizer = new QuantileDiscretizer().setInputCol("segmento_valor").setOutputCol("segmento_valor").setNumBuckets(5)
		val bucket=discretizer.fit(featureDf)
		bucket.getSplits

Those are the splits:  Array[Double] = Array(-Infinity, 380.0, 381.0, 382.0, 392.0, Infinity). As we can see, those splits are merging categories {393,394,395} into the same split. 
If we want prevent this for happening, then, number of buckets has to be larger.

		val discretizer = new QuantileDiscretizer().setInputCol("segmento_valor").setOutputCol("segmento_valor").setNumBuckets(7)
		val bucket=discretizer.fit(featureDf)
		bucket.getSplits

This number of buckets provides 7 intervals: Array(-Infinity, 380.0, 381.0, 382.0, 392.0, 393.0, 394.0, Infinity).That means, 7 categories. What happens if we select more than needed?

		val discretizer = new QuantileDiscretizer().setInputCol("segmento_valor").setOutputCol("segmento_valor").setNumBuckets(10)
		val bucket=discretizer.fit(featureDf)
		bucket.getSplits
	
We get the categories that are needed Array(-Infinity, 380.0, 381.0, 382.0, 392.0, 393.0, 394.0, 395.0, Infinity), the method is not creating empty categories. 

A common configuration of this transformer could be:

		<discretize force_checkpoint="true">
			<limit>50</limit>
			<nCategories>10</nCategories>
			<exclude></exclude>
		</discretize>	
		<discretize force_checkpoint="true">
			<columns>cod_sexo,cod_estado_civil,segmento_valor,segmento_actividad,segmento_socioeconomico,segmento_por_relacion,segmento_marketing,clasificacion_nivel_renta,rating_classification_mp,codigo_segmento_interno,tenenc_accionmodelicious_mt,tenenc_planpensiones_mt</columns>
			<limit>2</limit>
			<nCategories>35</nCategories>
			<exclude></exclude>
		</discretize>	
		<discretize force_checkpoint="true">
			<columns>all</columns>
			<limit>51</limit>
			<nCategories>10</nCategories>
			<exclude></exclude>
		</discretize>	
		
So that the first Transform would quantized all the variables with more than 50 values. The second will re-cuantize columns that we know are cualitative but we just want them to be beetween {0,NumCategories-1}.
The third transform should not apply to any column because the limit is larger than the one that we applied at the first place. But is just a double checking to get all different values for every column logged.

### PCA
Configuration file is just taking a single parameter:

```xml
<pca>
	<components>3</components>
</pca>
```

* components: number of final dimensions of the resulting projected matrix

### StandardScaler
Standardizes features by scaling to unit variance and/or removing the mean using column summary statistics on the samples in the training set. This is a very common pre-processing step
Standardization can improve the convergence rate during the optimization process, and also prevents against features with very large variances exerting an overly large influence during model training.
Configuration file is taking two parameters:

```xml
<standarize>
	<mean>true</mean>
	<std>true</std>
</standarize>
```

### Ones Balancer

This Transform allows to balance the nomber of rows in the dataframe based on the ratio of 1.0 to 0.0 labels.
It uses the DataFrame's sample function, that is known to not be exact for performance reasons. This is not a problem for the normal counts of Data used.

The format of the configuration element is the following:

```xml
<ones_balancer>
	<ratio>1.0</ratio>
	<tolerance>0.05</tolerance>
</ones_balancer>
```

Where ratio is the expected value of dividing the total number of 1.0 by the total number of 0.0 after the transform is applied. i.e.:

	* A 1.0 ratio means a ratio 1:1
	* A 0.5 ratio means 2 zeroes per each one ( 1/2 )
	* A 2.0 ratio means 2 ones per each zero

And so on. The default value of this parameter is 1.0

The tolerance allows to establish a minimal margin for which no transform is applied. The default value is 0.05. This means that if we ask for a ratio of 10.0 and the original DataSet is already around that value ± the tolerance, The transform will not be applied.
Since, as stated before, the underlying sample method is not exact. It is **recommended** to no set the tolerance below the default, as the resulting ratio of the transform in theses cases could be the same or worse thant the original.

#### Per model use
This Ones Balancer transform can be used not only in the PerpareData phase, but, also, in a per model basis. For this, it is enough to drop the configuration element inside the model's configuration, as in the example below:

```xml
<model name="RF_D5_N5" id="RandomForest" test_overfitting="false">
	<conf>
		<numTrees>5</numTrees>
		<algo>Classification</algo>
		<featureSubsetStrategy>auto</featureSubsetStrategy>
		<impurity>gini</impurity>
		<maxDepth>5</maxDepth>
		<maxBins>32</maxBins>
		<numClasses>2</numClasses>
		<useNodeIdCache>true</useNodeIdCache>
	    <ones_balancer><ratio>0.5</ratio></ones_balancer>
	</conf>
</model> 
```

When it is used like this, this balancing is only applied to the training set, and not to the test set.

## Column Transformations	

### Filter
Configuration file is just taking the filtering expression:

```xml
<filter><condicion>cod_postal is not null</condicion></filter>
```

Be aware that the condition could be any SQL conditional. However there is an issue with XML parsing of "lower than" symbol.
In those cases, condition must be written as:

```xml
<filter><condicion><![CDATA[dim3 <= 10]]></condicion></filter>
```

Or more complex conditions as:

```xml
<filter><condicion><![CDATA[dim2 >= 6 and dim3 <= 10]]></condicion></filter>
```
	
### Drop
Configuration file is just taking the name of the variable to be dropped:

```xml
<drop><columna>seg_valor</columna></drop>
```

### Math
Applies the given math transformation(s) to the given column, creating a new columns with the new value. The original column can be, then dropped or not.

The conf elements is as follows:

```xml
<mathT><column>dim1</column><transforms>exp, log2,log10, log, log1p, atan, acos, asin, pow2,pow3,sinh,cosh, tanh, sin, cos, tan,sqrt,cbrt</transforms><drop_original>false</drop_original></mathT>
```

Where:

* Column is the column to apply the formula
* transforms: a List of the math formulas to apply. One or more of the following:
	* exp: exponent
	* log2: base 2 log
	* log10: base10 log
	* log: natural log
	* log1p: natural log plus 1
	* atan: arc tan
	* acos: arc cos
	* asin: arc sin
	* pow2: square
	* pow3: cube
	* sinh: hyp sin
	* cosh: hyp cos
	* tanh: hyp tan
	* sin: sin
	* cos: cos
	* tan: tan
	* sqrt: square root
	* cbrt: cubic root

* drop_original: Selectcs whether if the original columns should be dropped from the result dataframe or not


### One Hot Encoder
This is a typical encoding for categorical variables, as described [in Wikipedia](https://en.wikipedia.org/wiki/One-hot). 
There are papers that show that binary encoding is also possible, but values are not equidistant. However, OneHotEncoding provides the optimal encoding
replacing every single category for an equidistant element. The main drawback is that the number of dimensions increases in the same number as the number of categories.

We are using the OneHotEncoder form [MLLIB] (https://spark.apache.org/docs/1.4.0/api/java/org/apache/spark/ml/feature/OneHotEncoder.html), that returns a column with an sparse
vector. AS we can see in this [example] (https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/OneHotEncoderExample.scala) to 
complete the coding we use the [StringIndexer] (http://spark.apache.org/docs/latest/ml-features.html#stringindexer) class that returns a column with a numerical index order from
0 to the max number of categories depending on its frequency of appearance.  
Then, the StringIndexed column (categoryIndex), is used to get the coding (categoryVec).

| id|category|categoryIndex|  categoryVec|
|---|---|---|---|
|  0|       a|          1.0|(3,[1],[1.0])|
|  1|       b|          3.0|    (3,[],[])|
|  2|       c|          2.0|(3,[2],[1.0])|
|  3|       a|          1.0|(3,[1],[1.0])|
|  4|       a|          1.0|(3,[1],[1.0])|
|  5|       c|          2.0|(3,[2],[1.0])|
|  6|       c|          2.0|(3,[2],[1.0])|
|  7|       e|          0.0|(3,[0],[1.0])|
|  8|       e|          0.0|(3,[0],[1.0])|
|  9|       e|          0.0|(3,[0],[1.0])|
| 10|       e|          0.0|(3,[0],[1.0])|

The OneHotEncoder is built using this sequence where df is the dataframe with the input table:

```scala
import org.apache.spark.ml.feature.StringIndexer
val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)
val indexed = indexer.transform(df)
val encoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")
val encoded = encoder.transform(indexed)
```

That implies two steps:
1. We have to build a StringIndexer, where the categories of column to be encoded "category" will be indexed in column "categoryIndex"
2. We have to build a OneHot encoder, that will use the content of the "categoryIndex" to encode in oneHot into the column "categoryVec".

If this transformation leads to an interesting model, we have to save this encoder setting to be able to re-apply the whole transformation sequence in a Test set. 
The settings that allow to recreate this transformations is the Indexer labels: 

```scala
indexer.labels => Array(e, a, c, b)
```

Thus, to recreate the indexer we have to execute:

```scala
val indexer_to_test = new org.apache.spark.ml.feature.StringIndexerModel(Array("e","a","c","b")).setInputCol("category").setOutputCol("categoryIndex_to_test")
val indexed_to_test = indexer_to_test.transform(df_to_test)
val encoder_to_test = new OneHotEncoder().setInputCol("categoryIndex_to_test").setOutputCol("categoryVec")
```
The configuration of this transformer can expecify some or all columns:

		<onehot>
			<columns>all</columns>
		</onehot>

Or

		<onehot>
			<columns>age,sex</columns>
		</onehot>


#### Binary Hot Encoder

This is a variation of the OneHotEncoder that encodes each value in its binary mode.

		<onehot force_checkpoint="true">
			<binary>true</binary>
			<columns>cod_sexo,cod_estado_civil,segmento_valor,segmento_actividad,segmento_socioeconomico,segmento_por_relacion,segmento_marketing,clasificacion_nivel_renta,codigo_segmento_interno,tenenc_accionmodelicious_mt,tenenc_fondoinvers_mt,tenenc_planpensiones_mt</columns>
		</onehot>


### Partial Data Save

The normal way to store partially transformed dataFrames would be to chain several ProcessData xml files. This element allows to store them in the middle of the transform chain. This makes simpler the taks without forcing to mantains alarger and more complex set of configuration files.

The format is:

		<save url=""/>

Where url accepts the same format than the url attribute for the output data element.

### Common attributes for all transforms

#### force_checkpoint
When using iterative algorithms, it is possible for the logical plan of a DataFrame to grow exponentially, leading to an OOM error. This is because, as most of Spark operations are lazy, 

To be able to avoid this, an option, available for all transforms, has been added. This will force a checkpoint on the dataset after the transform is applied.

Obviously, this can be not enough for transfomers that do a lot of operations. In that case, the inner implementation of the Transformer has to be smart enough to call checkpoint when needed.

This option can be enabled for any transform adding the following attribute to the transform xml element:

		force_checkpoint="true"

For example:

		<onehot force_checkpoint="true">
			<columns>age,sex</columns>
		</onehot>

__Note:__ As of Spark 1.6.0 this has not been implemented for DataFrams and we use a method that calls inner RDD checkpoint. When Spark 2.0 is installed in the cluster, we should update our code to use the good one.


## Check Overfitting

As per reference [An introduction to statistical learning](http://www-bcf.usc.edu/~gareth/ISL/).
It is possible to show that the expected test MSE, for a given value $x_0$, can always be decomposed into the sum of three fundamental quantities: the variance of $\hat{f}(x_0)$, the squared bias of $\hat{f}(x_0)$ and the variance of the error terms $\epsilon$. That is:

\begin{equation}
\mathbb{E} \(y_0-\hat{f(x_0)}\)^2 = Var \left ( \hat{f(x_0)} \right) + \left[ Bias \left ( \hat{f(x_0)} \right) \right ]^2 + Var(\epsilon)
\end{equation}

Here the notation $\mathbb{E} \(y_0-\hat{f(x_0)}\)^2$ defines the expected test MSE,
and refers to the average test MSE that we would obtain if we repeatedly estimated $f$ 
using a large number of training sets, and tested each at $x_0$. the overall 
expected test MSE can be computed by averaging $\mathbb{E} \(y_0-\hat{f(x_0)}\)^2$
over all possible values of x0 in the test set.

Thise equation tells us that in order to minimize the expected test error,
we need to select a statistical learning method that simultaneously achieves
low variance and low bias. Note that variance is inherently a nonnegative
quantity, and squared bias is also nonnegative. Hence, we see that the
expected test MSE can never lie below $Var(\epsilon)$, the irreducible error.

Variance refers to the amount by which $\hat{f}$ would change if we
estimated it using a different training data set. 
Since the training data are used to fit the statistical learning method, 
different training data sets will result in a different $\hat{f}$. 
But ideally the estimate for $f$ should not vary
too much between training sets. However, if a method has high variance
then small changes in the training data can result in large changes in $\hat{f}$.
In general, more flexible statistical methods have higher variance. 

On the other hand, bias refers to the error that is introduced by approximating
a real-life problem, which may be extremely complicated, by a much
simpler model. For example, linear regression assumes that there is a linear
relationship between $Y$ and $X1,X2, . . . , Xp$. It is unlikely that any real-life
problem truly has such a simple linear relationship, and so performing linear
regression will undoubtedly result in some bias in the estimate of $f$.
If the true $f$ is substantially non-linear, so no matter how many
training observations we are given, it will not be possible to produce an
accurate estimate using linear regression. Generally, more flexible
methods result in less bias.

As a general rule, as we use more flexible methods, the variance will
increase and the bias will decrease. The relative rate of change of these
two quantities determines whether the test MSE increases or decreases. As
we increase the flexibility of a class of methods, the bias tends to initially
decrease faster than the variance increases. Consequently, the expected
test MSE declines. However, at some point increasing flexibility has little
impact on the bias but starts to significantly increase the variance. When
this happens the test MSE increases

Good test set performance of a statistical learning method requires 
low variance as well as low squared bias. This is referred to as a trade-off
because it is easy to obtain a method with extremely low bias but
high variance (for instance, by drawing a curve that passes through every
single training observation) or a method with very low variance but high
bias (by fitting a horizontal line to the data).

A learning curve is a diagnostic that can tell which of these situations we're in, 
by plotting training error and validation error as a function of training set size. 

* High training and cross-validation error indicating high bias. A high bias model has 
few parameters and may result in underfitting. Essentially we're trying to fit an 
overly simplistic hypothesis, for example linear where we should be looking for a higher 
order polynomial. In a high bias situation, training and cross-validation error are both 
high and more training data is unlikely to help much. Some advices:
	* Find more features or add polynomial features
	* Increase parameters (more hidden layer neurons, for example)
	* Decrease regularization

![picture](resources/high_bias.jpg)

* Low training error and high cross-validation error indicates a high variance model.
Having lots of parameters, which carries a risk of overfitting. If we are overfitting, 
the algorithm fits the training set well, but has high cross-validation and testing error. 
If we see low training set error, with cross-validation error trending downward, then
the gap between them might be narrowed by training on more data. Some advices:
	* More training data
	* Reduce number of features, manually or using a model selection algorithm
	* Increase regularization

![picture](resources/high_variance.jpg)

The implemented module, to detect over-fitting, plots the training and the 
cross validation error as a function of the training set size, so that the user can check the behavior
of the model with the selected settings.

The result of this module is some entries in the log file with this look:

![picture](resources/overfitting_html.jpg)

And some HTML code that must be used to print some graphs in a future dashboard:

![picture](resources/overfitting_graph.jpg)

## Outputs

### Logs
The log file is the main debugging and verification output after an execution of the commander framework. Let's review some of the outputs of a typical log output.

The very beginning of the log shows the resources requested for the execution of the commander framework in the setup stage. The image below shows the number of instances, memory per executor and number of executors.

![picture](resources/log_settings.jpg)

After the setup, the LoadData stage starts. The image below shows some of the tasks implemented in this phase, like identifying the name of the label column, excluding some variables and saving to the hdfs the data at its entry point before any transformation.

![picture](resources/log_loaddata.jpg)

The next stage is the Process Data. This is the stage where all the transformations are applied to get the data ready before training the model. The image below shows how all transformations are read and listed. This image also shows an example of the Drop column transformation. 

![picture](resources/log_processdata.jpg)

The filter transformation allows to provide a SQL inclusive conditional expression. That means that only rows that verify this condition will be propagated to the next steps. Some examples are shown below.

![picture](resources/log_filter.jpg)

Purge invariant transformation is removing all columns whose relative standard deviation does not exceed the min_relative_std parameter. When the auto setting is set to true, the min_relative_std parameter is overridden with the tenth fraction of the relative standard deviation of the label. In this case, if any column is removed means that the variability of all the variables is larger than this value.

![picture](resources/log_removeinvariant.jpg)

The purge correlated transformation finds all correlated variables that exceed a certain provided value, and removes one of them. In the image belows some messages are shown as the max_correlation parameter and the correlation matrix that can be helpful to be able of modifying the max_correlation if we want to apply this transformation more or less aggressively.

![picture](resources/log_removecorrelated.jpg)

The remove outliers transformation is intended to remove all the rows whose values exceed N times the column standard deviation. Typically N is 3, but we can be more conservative depending on the number of outliers removed, that is also shown in the log entries.

![picture](resources/log_removeoutliers.jpg)

The discretization is doing a quantification of continuous variables. The settings allows to set the maximum number of variations used to separate automatically what we consider a categorical variable from a continuous variable. The nCategories settings fix the number of bins for the quantification. The log entries show some relevant features of this transformer as the possibility of excluding some columns from the discretization, or how the number of distinct values are computed for every column, and how the variables that does not exceed the limit setting are discretized. The list of splits used for the discretization of every variable is also saved and displayed in the log file.

![picture](resources/log_discretize.jpg)

The one hot codification transformation also computes the number of different values and perform the dimensionality expansion of the selected columns into binary new dimensions that code the original variable values.

![picture](resources/log_onehot.jpg)

The standarization of the data, can be accomplished by mean, by std or by both. Those settings are presented as boolean values in the log file, and some  lines after the standarization are shown so we could have a glance if the standarization is doing what expected.

![picture](resources/log_standarization.jpg)

The last transformation used in this example is a PCA projection. The image below shows the number of principal components requested and some sample lines to check that the number of expected dimensions match with the projection.

![picture](resources/log_pca.jpg)

At the end of the Process Data stage, a new data dump is saved into the HDFS so that it can be reused as many times as needed in the next stage.

![picture](resources/log_endprocessdata.jpg)

The last stage is the training of the model. In this case, only a SVM will be tested. The information logged shows the parametrization of the desired model, the number of Kfolds used for the cross validation, some stats computed for every fold and the final average stats that we must consider to decide which model performs better.

![picture](resources/log_train.jpg)

The last lines deal with the notification feature that sends an email with the resulting XML with all transformations and training values.

![picture](resources/log_mail.jpg)

### Stats
Some statistics are precomputed. New stats can easily be added. One of those is the Gain used by Modelicious Business Intelligence Unit, to assess the behaviour of their models.
In the images below it's explained how CRM compute this gain, how this gain should be computed for a classification problem and how this parameter can be approached using the statistics provided by this project.

![picture](resources/Ganancia_continua.JPG)

![picture](resources/gananacia_binario.JPG)

![picture](resources/ganancia_commander.JPG)



### HDFS Files

Check [Data Element](#data-element) for this.
All the output options are accepted here and, as commented in the Data documentation, several output options can be chained by a pipe (|) character.

For deployed models, the configuration of choice should always be the scores handle.

### Model Saved

The tool allows to save the generated models in local, remote or both paths. The SelectClassifier/SelectScorer documents must have this element under the options section:

        <save>
          <local>${environment.outputdir}</local>
          <remote>${environment.outputfiles}</remote>
        </save>

both, local and remote, are optional.

Since the select stages can have several models to generate. The full save path is built by convention as the following:

`${local/remote path}/${model-id}/${model-name}`

By default, all trained models are saved. 

### Inventory control

An additional file has been requested by IT Systems. Since it is only needed in the event of a deployed model, its generation is optional

## Scala and Spark Tips
### Requesting Seed 
A seed for a model can be requested as:

```scala
Application.getNewSeed()
```

The seed can be fixed with:

```scala	
Application.setSeed( Some(tuSeed) )
```
		
And reseted with:

```scala
Application.setSeed( None ) // si está a None, por defecto, llama a random. 
```

### Coloring console output

```scala
import org.fusesource.jansi.Ansi._
import org.fusesource.jansi.Ansi.Color._

log.file( "Column " + columna +"  will " + 
           ansi.fg(RED).a("NOT").reset +" be selected for outliers removal" )

// Also using render:
		   ansi().eraseScreen().render("@|red Hello|@ @|green World|@")

``` 


#### Available colors:

* BLACK
* RED
* GREEN
* YELLOW
* BLUE
* MAGENTA
* CYAN
* WHITE
* DEFAULT

Check https://github.com/fusesource/jansi

### Spark Web Server Memory Consumption

We have reported issues related with the memory consumption of the data stored needed by the Web server that spark uses to track all the steps and stats of the jobs.
When there are many stages on a spark pipeline, a large porcentage of the driver's RAM memory can be dedicated to store this data.
Those are two possibles workarounds:

1. Increase in the spark-default.conf the memory assigned to the driver:

		spark.driver.extraJavaOptions  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=C:/dumps -XX:MaxPermSize=6g -XX:PermSize=3g -XX:+CMSClassUnloadingEnabled

2. Add in the "0.Setup.xml" the properties retainedStages and retainedTasks that customize the size of the information retained for the tracking web service

		<setting name="spark.ui.retainedStages">10</setting>  <!-- El por defecto son 1000 -->
		<setting name="spark.ui.retainedTasks">5000</setting> <!-- El por defecto son 100000 -->

#### OOM Errors documentation
https://www.dataiku.com/learn/guide/spark/tips-and-troubleshooting.html#error-oom

### Dependency Tree

Running sbt with [this pluging](https://github.com/jrudolph/sbt-dependency-graph):

		sbt dependencyTree

#### spark2

		[info] com.modelicious.in:commander-spark2_2.10:1.0 [S]
		[info]   +-org.fusesource.jansi:jansi:1.4

#### lib

		[info] Resolving com.modelicious.in#commander-lib_2.10;1.0 ...
		[info] com.modelicious.in:commander-lib_2.10:1.0 [S]
		[info]   +-log4j:log4j:1.2.17
		[info]   +-org.fusesource.jansi:jansi:1.4
		[info]   +-org.scalatra.scalate:scalate-core_2.10:1.8.0 [S]
		[info]     +-org.scala-lang:scala-compiler:2.10.4 [S]
		[info]     | +-org.scala-lang:scala-reflect:2.10.4 [S]
		[info]     |
		[info]     +-org.scalatra.scalate:scalate-util_2.10:1.8.0 [S]
		[info]       +-org.slf4j:slf4j-api:1.7.21

#### core

		[info] com.modelicious.in:commander-core_2.10:1.0 [S]
		[info]   +-ca.szc.configparser:java-configparser:0.2
		[info]   +-com.modelicious.in:commander-lib_2.10:1.0 [S]
		[info]   | +-log4j:log4j:1.2.17
		[info]   | +-org.fusesource.jansi:jansi:1.4
		[info]   | +-org.scalatra.scalate:scalate-core_2.10:1.8.0 [S]
		[info]   |   +-org.scala-lang:scala-compiler:2.10.4 [S]
		[info]   |   | +-org.scala-lang:scala-reflect:2.10.4 [S]
		[info]   |   |
		[info]   |   +-org.scalatra.scalate:scalate-util_2.10:1.8.0 [S]
		[info]   |     +-org.slf4j:slf4j-api:1.7.21
		[info]   |
		[info]   +-com.modelicious.in:commander-spark2_2.10:1.0 [S]
		[info]   | +-org.fusesource.jansi:jansi:1.4
		[info]   |
		[info]   +-com.databricks:spark-csv_2.10:1.5.0 [S]
		[info]   | +-com.univocity:univocity-parsers:1.5.1
		[info]   | +-org.apache.commons:commons-csv:1.1
		[info]   |
		[info]   +-com.github.scala-incubator.io:scala-io-file_2.10:0.4.3 [S]
		[info]   | +-com.github.scala-incubator.io:scala-io-core_2.10:0.4.3 [S]
		[info]   |   +-com.jsuereth:scala-arm_2.10:1.3 [S]
		[info]   |
		[info]   +-org.fusesource.jansi:jansi:1.4

#### H2O

		[info] com.modelicious.in:commander-h2o_2.10:1.0 [S]
		[info]   +-com.modelicious.in:commander-core_2.10:1.0 [S]
		[info]   | +-ca.szc.configparser:java-configparser:0.2
		[info]   | +-com.modelicious.in:commander-lib_2.10:1.0 [S]
		[info]   | | +-log4j:log4j:1.2.17
		[info]   | | +-org.fusesource.jansi:jansi:1.4
		[info]   | | +-org.scalatra.scalate:scalate-core_2.10:1.8.0 [S]
		[info]   | |   +-org.scala-lang:scala-compiler:2.10.4 [S]
		[info]   | |   | +-org.scala-lang:scala-reflect:2.10.4 [S]
		[info]   | |   |
		[info]   | |   +-org.scalatra.scalate:scalate-util_2.10:1.8.0 [S]
		[info]   | |     +-org.slf4j:slf4j-api:1.7.21
		[info]   | |
		[info]   | +-com.modelicious.in:commander-spark2_2.10:1.0 [S]
		[info]   | | +-org.fusesource.jansi:jansi:1.4
		[info]   | |
		[info]   | +-com.databricks:spark-csv_2.10:1.5.0 [S]
		[info]   | | +-com.univocity:univocity-parsers:1.5.1
		[info]   | | +-org.apache.commons:commons-csv:1.1
		[info]   | |
		[info]   | +-com.github.scala-incubator.io:scala-io-file_2.10:0.4.3 [S]
		[info]   | | +-com.github.scala-incubator.io:scala-io-core_2.10:0.4.3 [S]
		[info]   | |   +-com.jsuereth:scala-arm_2.10:1.3 [S]
		[info]   | |
		[info]   | +-org.fusesource.jansi:jansi:1.4
		[info]   |
		[info]   +-org.fusesource.jansi:jansi:1.4				

#### App
	
		[info] com.modelicious.in:commander-app_2.10:1.0 [S]
		[info]   +-com.modelicious.in:commander-core_2.10:1.0 [S]
		[info]   | +-ca.szc.configparser:java-configparser:0.2
		[info]   | +-com.modelicious.in:commander-lib_2.10:1.0 [S]
		[info]   | | +-log4j:log4j:1.2.17
		[info]   | | +-org.fusesource.jansi:jansi:1.4
		[info]   | | +-org.scalatra.scalate:scalate-core_2.10:1.8.0 [S]
		[info]   | |   +-org.scala-lang:scala-compiler:2.10.4 [S]
		[info]   | |   | +-org.scala-lang:scala-reflect:2.10.4 [S]
		[info]   | |   |
		[info]   | |   +-org.scalatra.scalate:scalate-util_2.10:1.8.0 [S]
		[info]   | |     +-org.slf4j:slf4j-api:1.7.21
		[info]   | |
		[info]   | +-com.modelicious.in:commander-spark2_2.10:1.0 [S]
		[info]   | | +-org.fusesource.jansi:jansi:1.4
		[info]   | |
		[info]   | +-com.databricks:spark-csv_2.10:1.5.0 [S]
		[info]   | | +-com.univocity:univocity-parsers:1.5.1
		[info]   | | +-org.apache.commons:commons-csv:1.1
		[info]   | |
		[info]   | +-com.github.scala-incubator.io:scala-io-file_2.10:0.4.3 [S]
		[info]   | | +-com.github.scala-incubator.io:scala-io-core_2.10:0.4.3 [S]
		[info]   | |   +-com.jsuereth:scala-arm_2.10:1.3 [S]
		[info]   | |
		[info]   | +-org.fusesource.jansi:jansi:1.4
		[info]   |
		[info]   +-org.fusesource.jansi:jansi:1.4

#### Root

		[info] com.modelicious.in:root_2.10:1.0 [S]
		[info]   +-com.modelicious.in:commander-app_2.10:1.0 [S]
		[info]   | +-com.modelicious.in:commander-core_2.10:1.0 [S]
		[info]   | | +-ca.szc.configparser:java-configparser:0.2
		[info]   | | +-com.modelicious.in:commander-lib_2.10:1.0 [S]
		[info]   | | | +-log4j:log4j:1.2.17
		[info]   | | | +-org.fusesource.jansi:jansi:1.4
		[info]   | | | +-org.scalatra.scalate:scalate-core_2.10:1.8.0 [S]
		[info]   | | |   +-org.scala-lang:scala-compiler:2.10.4 [S]
		[info]   | | |   | +-org.scala-lang:scala-reflect:2.10.4 [S]
		[info]   | | |   |
		[info]   | | |   +-org.scalatra.scalate:scalate-util_2.10:1.8.0 [S]
		[info]   | | |     +-org.slf4j:slf4j-api:1.7.21
		[info]   | | |
		[info]   | | +-com.modelicious.in:commander-spark2_2.10:1.0 [S]
		[info]   | | | +-org.fusesource.jansi:jansi:1.4
		[info]   | | |
		[info]   | | +-com.databricks:spark-csv_2.10:1.5.0 [S]
		[info]   | | | +-com.univocity:univocity-parsers:1.5.1
		[info]   | | | +-org.apache.commons:commons-csv:1.1
		[info]   | | |
		[info]   | | +-com.github.scala-incubator.io:scala-io-file_2.10:0.4.3 [S]
		[info]   | | | +-com.github.scala-incubator.io:scala-io-core_2.10:0.4.3 [S]
		[info]   | | |   +-com.jsuereth:scala-arm_2.10:1.3 [S]
		[info]   | | |
		[info]   | | +-org.fusesource.jansi:jansi:1.4
		[info]   | |
		[info]   | +-org.fusesource.jansi:jansi:1.4
		[info]   |
		[info]   +-com.modelicious.in:commander-h2o_2.10:1.0 [S]
		[info]   | +-com.modelicious.in:commander-core_2.10:1.0 [S]
		[info]   | | +-ca.szc.configparser:java-configparser:0.2
		[info]   | | +-com.modelicious.in:commander-lib_2.10:1.0 [S]
		[info]   | | | +-log4j:log4j:1.2.17
		[info]   | | | +-org.fusesource.jansi:jansi:1.4
		[info]   | | | +-org.scalatra.scalate:scalate-core_2.10:1.8.0 [S]
		[info]   | | |   +-org.scala-lang:scala-compiler:2.10.4 [S]
		[info]   | | |   | +-org.scala-lang:scala-reflect:2.10.4 [S]
		[info]   | | |   |
		[info]   | | |   +-org.scalatra.scalate:scalate-util_2.10:1.8.0 [S]
		[info]   | | |     +-org.slf4j:slf4j-api:1.7.21
		[info]   | | |
		[info]   | | +-com.modelicious.in:commander-spark2_2.10:1.0 [S]
		[info]   | | | +-org.fusesource.jansi:jansi:1.4
		[info]   | | |
		[info]   | | +-com.databricks:spark-csv_2.10:1.5.0 [S]
		[info]   | | | +-com.univocity:univocity-parsers:1.5.1
		[info]   | | | +-org.apache.commons:commons-csv:1.1
		[info]   | | |
		[info]   | | +-com.github.scala-incubator.io:scala-io-file_2.10:0.4.3 [S]
		[info]   | | | +-com.github.scala-incubator.io:scala-io-core_2.10:0.4.3 [S]
		[info]   | | |   +-com.jsuereth:scala-arm_2.10:1.3 [S]
		[info]   | | |
		[info]   | | +-org.fusesource.jansi:jansi:1.4
		[info]   | |
		[info]   | +-org.fusesource.jansi:jansi:1.4
		[info]   |
		[info]   +-org.fusesource.jansi:jansi:1.4



## Developing HTML reports

Scalate templating system was selected to transform the xml results to html.

The templates are located under lib/src/resources/WEB-INF/templates

For convenience and faster development, a module web has been added to the project. It contains a scalatra web application that loads a result.xml sample file and uses the lib templates to render it as it would on a results email.

A jetty server with automatic reloading can be started with:

		sbt ~web/jetty:start

And then, the result.html page can be accessed through __localhost:8080__

Changes on the templating will be reflected in the page served on this URL

## Demos

In this section we provide some reproducible examples to be run as training or to be used as templates.

### Overfitting

Run: 
		app-submit /run/demos/overfitting
		
In the path ~/run/demos/overfitting some configuration files and demo data can be found.
Its execution with the selected resources takes around 22 minutes.

* 0.Setup.xml
	This file defines resources and notification settings.
* 10.LoadData.xml
	This file defines load data when querying from Hive Table. In this demo, data will be loaded from csv file. So this file can be ignored (add underscore to the filename).
* 20.ProcessData.xml
	This file defines all transformations to pre-process data. Note that sub-sampling is commented when reading from local csv file (its size is already sampled to fit demo purposes) 
	
		<data>
			<input url="lcsv:${environment.workingdir}/data.csv"/>
			<output url="hdfs:${environment.tmpfiles}/temp.dat"/>
		</data>
	
	The processed data will be temporary stored in hdfs ${environment.tmpfiles} path to feed the SelectClassifier and SelectScorer sections.
	
* 30.SelectClassifier.xml
	This file defines two classifiers to be tested using cross-validation with 3 k-folds. Overfitting will be studied by computation of learning curves for 
	sampled data on the percentages [0.001,0.01,0.1,0.5,1]
* 40.SelectScorer.xml
	This file defines two scorers to be tested using cross-validation with 3 k-folds. Overfitting will be studied by computation of learning curves for 
	sampled data on the percentages [0.001,0.01,0.1,0.5,1]
	
![picture](resources/demo_overfitting.JPG)

The final report can be found in this [link](runs/demos/overfitting/report.html)

### Scoring

Run: 
		app-submit /run/demos/scoring

In the path ~/run/demos/scoring some configuration files can be found.
Its execution with the selected resources takes around 10 minutes.

* 0.Setup.xml
	This file defines resources and notification settings.
* 10.RunSQL.xml
	This file defines the query to caracterize all customers.
* 20.RunScorer.xml
	This file defines the model to be used to score all the customers.
	
![picture](resources/demo_scoring.JPG)

The final report can be found in this [link](runs/demos/scoring/report.html)

### H20

Run: 
		h2o-submit /run/demos/h2o
		
In the path ~/run/demos/h2o some configuration files and demo data can be found.
Its execution with the selected resources takes around 12 minutes.

* 0.Setup.xml

	This file defines resources and notification settings. Note h2o needs some settings to be settle in this section:
	
		<spark App="ModelsTemplate" Master="yarn-client" base="/user/A181345/prueba_discretizacion" checkpointFolder="cache" logLevel="INFO">
			<settings>
				<setting name="spark.executor.memory">8g</setting>
				<setting name="spark.executor.cores">4</setting>
				<setting name="spark.executor.instances">120</setting>
				<setting name="spark.logLineage">true</setting>
			</settings>
		  <h2o-settings>
			<setting name="ice_root">c:\tmp\prueba</setting>
		  </h2o-settings>
		  <notifications>
				<enabled>true</enabled>
				<email>${environment.user}@nube.modelicious.com</email>
		  </notifications>
		</spark>

* 10.LoadData.xml

This file defines load data when querying from Hive Table. In this demo, data will be loaded from csv file. So this file can be ignored (add underscore to the filename).

* 20.ProcessData.xml

	This file defines all transformations to pre-process data. Note that sub-sampling is commented when reading from local csv file (its size is already sampled to fit demo purposes) 
	
		<data>
			<input url="lcsv:${environment.workingdir}/data.csv"/>
			<output url="hdfs:${environment.tmpfiles}/temp.dat"/>
		</data>
	
	The processed data will be temporary stored in hdfs ${environment.tmpfiles} path to feed the SelectClassifier section.

* 30.SelecClassifier.xml

	This file defines two classifiers to be tested using cross-validation with 3 k-folds. Overfitting will be studied by computation of learning curves for 
	sampled data on the percentages [0.001,0.01,0.1,0.5,1]
	
![picture](resources/demo_h2o.JPG)

The final report can be found in this [link](runs/demos/h2o/report.html)

### Testing

Run: 
		app-submit /run/demos/testing

In the path ~/run/demos/testing some configuration files and demo data can be found.
Its execution with the selected resources takes around 4 minutes.

* 0.Setup.xml

	This file defines resources and notification settings.
	
* 10.LoadTestDataToDB.xml

	This file is a particularization of a PrepareData stage that reads frozen data into a hive table with no transformations:
	
		<PrepareData>
			<data>
				<input url="lcsvgz:${environment.workingdir}/test_data.csvgz"/>
				<output url="db:${environment.tmpdb}.test_table_${modelid}_${sessionid}:dropFirst=true"/>
			</data>
			<transformations>
			</transformations>
		</PrepareData>

* 11.LoadDataFromDB.xml

	This file is a particularization of a LoadData stage that retrieves a DataFrame from a hive table
	
		<LoadData>
			<data>
				<output url="memory:test_data"/>
			</data>
			<sql>
				<table>${environment.tmpdb}.test_table_${modelid}_${sessionid}</table>
				<label>label</label>
				<columns>
					<include_only>label,antiguedad_hasta_la_caracterizacion,edad,marca_modelicious_sin_comisiones,posee_deposito_plazo,posee_tarj_credito,saldofinal_pasivo_titprinc,numero_lineas_productos,margen_financiero_titularidad_principal_interanual,margen_ordinario_tp,saldo_medio_mensual_total_pasivo_coefvarporcentual,saldo_medio_mensual_total_pasivo_tendrelativa01m,saldo_medio_mensual_total_pasivo_tendrelativa03m,importe_lcp,tiene_lcp,nu_opernoautomoficmes_mt,nu_tjcredactv_mt,nu_ctrahorrplz_mt,im_saldmedmensuahorrvis_mt,segmento_valor,segmento_marketing</include_only>
				</columns>
			</sql>
		</LoadData>


* 20.RunScorer.xml

	This file defines the model to be used to score all the customers.

		<RunScorer>
		  <data>
			<input url="memory:test_data:repartition=40"/>
			<output url="lcsvgz:${environment.workingdir}/predictions2.csvgz"/>
		  </data>
		  <model url = "local:${environment.workingdir}/models/MLDecisionTreeS/DT_10" nCategories="5" balancing="1.0"/>
		</RunScorer>

* 30.CompareData.xml

	This file defines the assessment of the test between the frozen data and the recently predicted data.
	
		<CompareData>
		  <data>
			<input1 url="lcsvgz:${environment.workingdir}/predictions.csvgz"/>
			<input2 url="lcsvgz:${environment.workingdir}/predictions2.csvgz"/>
		  </data>
		</CompareData>

	
![picture](resources/demo_testing.JPG)

The final report can be found in this [link](runs/demos/testing/report.html)

## API

This framework generates API documentation runing command: 

		sbt unidoc

This command required the instalation of [Graphviz](http://www.graphviz.org/).
The output is stored in the path $REPO_ROOT/api and can be accessed through the file [index.html](api/index.html).

![picture](resources/api.jpg)

## Unit TESTS

### Array to Tuple
Test the URI parsing and conversions

### Memory DataWrapper
TODO

### Modelos
Tests:

* MLLIB Random Forest without feature selection
* MLLIB Random Forest with feature selection
* MLLIB Random Forest without feature selection and categorical variables
* MLLIB Random Forest with feature selection save/load
* ML Random Forest without feature selection
* ML Random Forest with feature selection
* ML Random Forest without feature selection and categorical variables
* ML Random Forest Scorer without feature selection
* ML Random Forest Scorer with feature selection
* ML Random Forest Scorer without feature selection and categorical variables
* ML Random Forest Scorer without feature selection save/load
* ML Random Forest Scorer with transformations save/load
* MLLIB Decision Trees without feature selection
* MLLIB Decision Trees with feature selection
* MLLIB Decision Trees without feature selection and categorical variables
* ML Decision Trees without feature selection
* ML Decision Trees with feature selection
* ML Decision Trees without feature selection and categorical variables
* ML Decision Trees Scorer without feature selection
* ML Decision Trees Scorer with feature selection
* ML Decision Trees Scorer without feature selection and categorical variables
* MLLIB SVM L1 without feature selection
* MLLIB SVM L1 with feature selection
* MLLIB SVM L1 without feature selection save/load
* MLLIB SVM L2 without feature selection
* MLLIB SVM L2 with feature selection
* MLLIB SVM SCorer L1 without feature selection
* MLLIB SVM SCorer L1 with feature selection
* MLLIB SVM SCorer L2 without feature selection
* MLLIB SVM SCorer L2 with feature selection
* MLLIB SVM SCorer L2 with feature selection save/load
* MLLIB Naive Bayes without feature selection
* MLLIB Naive Bayes with feature selection
* MLLIB Naive Bayes without feature selection save/load
* MLLIB Naive Bayes Scorer without feature selection
* MLLIB Naive Bayes Scorer with feature selection
* MLLIB Naive Bayes SCorer with feature selection save/load
* MLLIB Multilayer Perceptron without feature selection. 
* MLLIB OneVsAll Logistic modeling without feature selection.
* H2O GBM: GBM without feature selection and 32 trees
* H2O GBM: GBM without feature selection and 16 trees

### Stats
TODO

### TestAppConfig
TODO

### Transformers
Tests:

* Remove Outliers: Remove items that exceed 3*std in one column in single column DF
* Remove Outliers: Remove items that exceed 3*std in one column in multi-column DF
* Remove Outliers: Remove items that exceed 3*std in all column in multi-column DF
* Filter: Filter column values larger than a fix value
* Filter: Return the same DF when filter condition applies on inexistent column
* Filter: Return the same DF when filter condition does not filter anything
* Filter: Filter column values lower then a fix value
* Filter: Filter two columns with a double condition larger than and lower than some values
* Drop: Remove column
* Drop: Drop wrong column
* PCA: Test number of dimensions projected
* PCA: Test vectors projected
* PCA: Test schema of DF generated
* Standarizer: Test standarization with mean and std
* Standarizer: Test standarization with mean
* Standarizer: Test standarization with std
* Standarizer: Test standarization without mean and std
* Discretizer: Test nothing is discretized when limit is too large
* Discretizer: Test one columns is discretized
* Discretizer: Test two columns are discretized
* Discretizer: Test three columns are discretized
* Discretizer: Test four columns are discretized
* Discretizer: Test five columns are discretized
* Discretizer: Test three columns are discretized using the columns parameter
* Discretizer: Test two columns are discretized using the columns and exclude parameter
* PurgeInvariant: Test remove the single column in a single column DF
* PurgeInvariant: Test remove nothing in a single column DF
* PurgeInvariant: Test remove two columns in a multiple column DF
* PurgeInvariant: Test remove nothing in a multiple column DF
* PurgeInvariant: Test remove three columns, in AUTO mode, in a multiple column DF
* PurgeCorrelated: Test nothing is removed in a single column DF
* PurgeCorrelated: Test remove one dimension in a multicolumn DF
* PurgeCorrelated: Test remove two dimensions in a multicolumn DF
* PurgeCorrelated: Test nothing is removed when max correlation is too high in a multicolumn DF
* Sampling: Test that sampling with no rate should return the original DF
* Sampling: Test that sampling with 0.0 rate should return empty DF
* Sampling: Test that sampling with 0.25 rate should return 1/4 of the DF
* Sampling: Test that sampling with 0.50 rate should return half the original DF
* Sampling: Test that sampling with 0.75 rate should return 3/4 the original DF
* Sampling: Test that sampling with 1.00 rate should return the original DF
* FeatureSelector: Test selection with  MIM selecting 2 variables.
* FeatureSelector: Test selection with MIFS selecting 2 variables.
* FeatureSelector: Test selection with  JMI selecting 2 variables.
* FeatureSelector: Test selection with MRMR selecting 2 variables.
* FeatureSelector: Test selection with ICAP selecting 2 variables.
* FeatureSelector: Test selection with CMIM selecting 2 variables.
* FeatureSelector: Test selection with   IF selecting 2 variables.
* FeatureSelector: Test selection with NONE selecting 2 variables.
* OneHotEncoder: Test coding column category1.
* OneHotEncoder: Test coding column category2.
* OneHotEncoder: Test coding column category1 and category2.
* OnesBalancerTest: Test creation of 1:1 ratio dropping zeros
* OnesBalancerTest: Test creation of 1:1 ratio dropping ones
* OnesBalancerTest: Test creation of 2:1 ratio dropping zeros
* OnesBalancerTest: Test creation of 1:2 ratio dropping ones
* OnesBalancerTest: Test to do nothing when a tolerance is already met by the test data
* OnesBalancerTest: Another test to do nothing when a tolerance is already met by the test data

### Serializers
Tests:

* Serializers: Test Discretizer serialization on all dimensions
* Serializers: Test Discretizer serialization on subset of dimensions (4)
* Serializers: Test Discretizer serialization on subset of dimensions (3)
* Serializers: Test Discretizer serialization on subset of dimensions (2)
* Serializers: Test Discretizer serialization on subset of dimensions (1)
* Serializers: Test Discretizer serialization on any dimensions
* Serializers: Test OneHote on one column serialization
* Serializers: Test OneHote on two column serialization
* Serializers: Test Standard Scaler serialization with MEAN and STD
* Serializers: Test Standard Scaler serialization with MEAN
* Serializers: Test Standard Scaler serialization with STD
* Serializers: Test Standard Scaler serialization with no standarization
* Serializers: Test Sampling serialization with no rate
* Serializers: Test Sampling serialization with rate 0.25
* Serializers: Test Sampling serialization with rate 0.50
* Serializers: Test Sampling serialization with rate 0.75
* Serializers: Test Sampling serialization with rate 1.0
* Serializers: Test Drop column serialization
* Serializers: Test Drop label serialization
* Serializers: Test Drop empty field serialization
* Serializers: Test Drop wrong column serialization
* Serializers: Test Feature Selector serialization MIFS selecting 2 variables.
* Serializers: Test Feature Selector serialization MIFS selecting 4 variables.
* Serializers: Test Feature Selector serialization MIFS selecting 6 variables.
* Serializers: Test PCA serialization with 2 principal components.
* Serializers: Test PCA serialization with 4 principal components.
* Serializers: Test PCA serialization with 5 principal components.
* Serializers: Test PCA serialization without any principal components.
* Serializers: Test PurgeCorrelated when removing one column
* Serializers: Test PurgeCorrelated when removing two column
* Serializers: Test PurgeCorrelated when not removing any column
* Serializers: Test PurgeCorrelated when not max_correlation is provided
* Serializers: Test PurgeInvariant when removing three columns
* Serializers: Test PurgeInvariant when removing one columns
* Serializers: Test PurgeInvariant in auto mode
* Serializers: Test PurgeInvariant with no parameters
* Serializers: Test RemoveOutliers with 3*std on one column
* Serializers: Test RemoveOutliers with 3*std on all columns
* Serializers: Test RemoveOutliers with 3*std and no max_percentage_of_outliers
* Serializers: Test RemoveOutliers with no parameters at all

# TODO List
|Descripcion|Tipo requisito|Prioridad
|----|----|:-----:|
|<s>Refactorization to MLLIB style train and apply methods for models </s>|Tecnico |1 |
|<s>Unit Test for all models <s>|Tecnico |1|
|<s>Unit Test for all transformers </s> |Tecnico |1|
|<s>Generación de metadatos sobre los RDDs. Qué variables son categóricas y cuáles no y cuantos valores tienen las categóricas</s>| Funcional | 1
|<s>Intentar conseguir un mecanismo de discretización de variables contínuas o de re-discretización de variables categóricas. A ser posible sin las limitaciones del QuantileDiscretizer que tiene un número de bins limitado.</s>| Funcional | 1
|<s>Implementar un mecanismo que permita codificar variables categóricas fácilmente en oneHotEncoding. Y en caso de que el número de categorías sea muy elevado, que nos permita codificar variables en binario</s>| Funcional | 1
|<s>Implementar Clase Wrapper sobre DataFrame que nos permita añadir metadatos propios </s>| Técnico| 2|
|<s>En Perceptron multicapa dividir layers en 2 parámetros: Capas itnermedias y numero de clases ( numero nodos capa final). Tomar capa inicial dinámicamente del número de variables de entrada.</s>|Técnico|1|
|<s>Metodo override def result() de la Transformacion PCA pendiente de la forma de añadir metadatos que esta implementando Alberto</s>|Técnico|1
| Añadir intervalo de checkpoints como utilidad para modificar dataframes en bucles <br/>Ver codigo en http://stackoverflow.com/questions/34461804/stackoverflow-due-to-long-rdd-lineage?noredirect=1&lq=1<br/><pre><code>def mergeWithLocalCheckpoint[T: ClassTag](interval: Int)<br/>(acc: RDD[T], xi: (RDD[T], Int)) = {<br/>    if(xi._2 % interval == 0 & xi._2 > 0) xi._1.union(acc).localCheckpoint<br/>    else xi._1.union(acc)<br/>  }<br/></code></pre><br/>Tambien se podría hacer con el checkpoint no local | Técnico | 2|
|<s>Restore the auto funcionality in Purge invariant</s>| Funcional | 2|
|Update the statistics computation module using MLLIB methods | Technical| 1|
|Implement save model section for selected model settings| Technical |1|
|<s>Implement over-fitting detector</s>| Technical |1|
|<s>Make Unit Test for serialize and save models and unserialize and load models</s>| Technical |2|
|Make Unit Test for saving models and load models| Technical |2|
|<s>Refactor Models so the classes follow the same flow than Spark ones (or our own Transformers) modelCreator.fit.train -> new Model -> model-> predict</s> | Technical |2|
|Refactor models to use reflectios for configuration, just like with transformers| Technical |2| 
|<s>Define categorical variables for decision trees and random forest</s>| Technical |1|  
|Introduce new onehot encoder with binary code for shorten dimensions| Technical |1|   
|Add CRM used stat| Technical |1|  

# Known Issues
* Spark 1.6.0 ( The one in cluster ) QuantileDiscretizer (has a bug when there are more than 10.000 distinct values)[http://apache-spark-developers-list.1001551.n3.nabble.com/Opening-a-JIRA-for-QuantileDiscretizer-bug-td16396.html]. To avoid this, the only way we had is to backport the Class from 1.6.3 and include it, renamed, in the project files. When cluster gets upgraded to 1.6.3+, this should be removed. 
* ML models read/write is not implemented until Spark 2.0. For Perceptron it´s been backported in this project, but for 1VSAll not yet 
