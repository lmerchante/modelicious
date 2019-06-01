package com.modelicious.in.html


import scala.xml._
import scala.language.postfixOps

object ScalateHelpers  {
  
  val scorer_columns = List("Ganancia","NumUnos", "decil1", "decil2", "decil3", "decil4", "decil5", "decil6", "decil7", "decil8", "decil9", "Time")
  val classifier_columns = List("Ganancia", "Accuracy", "Precision", "PrecisionMinima", "PrecisionExclu", "PrecisionMinimaExclu", "VolCampanha", "VolCampaniaExclu", "N", "P", "FPR", "FNR", "TPR", "TNR", "Tiempo")
  
  def data( node: scala.xml.NodeSeq ): scala.xml.Elem  = {
    def split_url( url: String ): scala.xml.Elem = {
      <ul>
      {
        for ( u <- url.split('|') ) yield {
          <li><a style="overflow-wrap:break-word;">{u}</a></li>
        }
      }
      </ul>
    }
     
    <li>Data: <ul>
   	{    	  
    	  for( c <- node.head.child ) yield {
          <li>{c.label.capitalize} { split_url( (c \ "@url").text ) } 
          { val cols = (c \ "columns")
            if (cols.size > 0) {
              <li><a>{cols.text}</a></li>
            }
           }  
				</li>
        }
    }
    </ul></li>
  }
  
  def table( stage: scala.xml.NodeSeq, node_name: String, fields: List[String] ): scala.xml.Elem  = {
    	<table class="table table-striped table-bordered table-sm stats hover">
			<thead class="thead-inverse">
			<tr scope="row">
				<th>Model ID</th>
				{ for (c <- fields ) yield { <th>{c}</th> } }
			</tr>
			</thead>
			<tbody>
			
			{for( m <- (stage \ "models" \ "model") ) yield { 
			<tr scope="row">
				<td>{ m \ "@id" }</td>
				{if (!(m \ "result" \ "stats").isEmpty) {
					val s = m \ "result" \ "stats" \ node_name 
					for (c <- fields ) yield { <td>{s \ c text }</td> }
				}
				else
					for( i <- (1 to fields.size + 1) ) yield {
					<td></td>
					}
				}
			</tr>
			}
			}
			</tbody>
		</table>
  
  } 
  
  
  def scorer_statistics_table( stage: scala.xml.NodeSeq ): scala.xml.Elem  = table( stage, "ScorerStatistics", scorer_columns )
  def classifier_statistics_table( stage: scala.xml.NodeSeq ): scala.xml.Elem  = table( stage, "Statistics", classifier_columns )
  
}