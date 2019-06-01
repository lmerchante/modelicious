package com.modelicious.in.tools


// Included in spark
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.util.parsing.json.{JSONObject,JSONFormat,JSONArray}


// Code from https://coderwall.com/p/o--apg/easy-json-un-marshalling-in-scala-with-jackson
object JsonUtil {
  val mapper = new ObjectMapper() //  with ScalaObjectMapper  // Bug: https://github.com/FasterXML/jackson-module-scala/issues/288
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }
  
  def format(json: Any, i: Int = 0):String = {
    // Add Indents and carriage returns
    // It has to be called as: 
    //   val json_in_string="{\"KPI\":[{\"kpiName\":\"Num_clientes \",\"kpiType\":\"Long\",\"kpiValue\":\"432674\"},{\"kpiName\":\"Decil1 \",\"kpiType\":\"Double\",\"kpiValue\":\"0.8009929885239466\"},],\"sessionID\":\"s20171129141026\",\"modelID\":\"m1343aed5\"}"
    //   val json_in_formated_string = format(  scala.util.parsing.json.JSON.parseRaw(json_in_string).get  )
    json match {
    case o: JSONObject =>
      o.obj.map{ case (k, v) =>
        "  "*(i+1) + JSONFormat.defaultFormatter(k) + ": " + format(v, i+1)
      }.mkString("{\n", ",\n", "\n" + "  "*i + "}")
  
    case a: JSONArray =>
      a.list.map{
        e => "  "*(i+1) + format(e, i+1)
      }.mkString("[\n", ",\n", "\n" + "  "*i + "]")
    case _ => JSONFormat defaultFormatter json
    }
  }
  
  
  /*def toStringMap(json: String): Map[String,String] = {
    org.json4s.jackson.JsonMethods.parse(json).values.asInstanceOf[Map[String, String]]
  }*/
  
  
// Does not compile without ScalaObjectMapper
// Solution would be The only difference in code is to use classOf[...] to specify type for readValue as the 2nd parameter.
// From https://stackoverflow.com/a/34818559/1829897
//  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)
//  def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
//    mapper.readValue[T](json)
//  }
}