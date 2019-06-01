
import com.modelicious.in.app.Application
import com.modelicious.in.Constants
import com.modelicious.in.data.DataWrapper
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.SchemaTools
import org.apache.log4j.Logger
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.{ PCA, PCAModel, PCAModelProxy }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, Column }
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType, IntegerType }
import org.apache.spark.SparkException
import java.{ util => ju }
import scala.xml._
import scala.language.postfixOps
import scala.language.reflectiveCalls
import com.modelicious.in.Constants

package com.modelicious.in.transform {

  case class pca() extends TransformerState {
    var components: Int = 3
    var principal_componentes: DenseMatrix = _
  }

  class PCATransformModel(theState: TransformerState) extends DataTransformerModel[pca](theState) {

    def doTransform(data: DataWrapper): DataWrapper = {

      withRDDAndVaryingSchema(data) { (rdd, schema) =>
        val pca: PCAModel = PCAModelProxy.createPCAModel(state.components, state.principal_componentes)
        val rdd_projected = rdd.map(p => p.copy(features = pca.transform(p.features)))
        log.file(s"PCA output variables without label:" + rdd_projected.first().features.size)
        val newSchema = StructType(Seq(StructField("label", DoubleType, true)) ++ (1 to state.components).map(component => StructField("pca_" + component, DoubleType, false)))

        rslt = <principal_components>{ state.toXML }</principal_components>

        (rdd_projected, newSchema)

      }.addToMetaData(state.toXML)
    }

  }

  class PCATransform extends DataTransformer[pca] {

    override def fit(dw: DataWrapper): PCATransformModel = {
      val data = SchemaTools.DFtoLabeledRDD(dw.data)
      val pca = new PCA(state.components).fit(data.map(_.features))
      state.principal_componentes = pca.pc

      new PCATransformModel(state)
    }
  }

}

// PCAModel is private at package level so we do this "hack" to be able to instantiate 
package org.apache.spark.mllib.feature {
  object PCAModelProxy {
    def createPCAModel(k: Int, pc: DenseMatrix): PCAModel = new PCAModel(k, pc)
  }
}

