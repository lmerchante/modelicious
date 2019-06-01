package com.modelicious.in.tools


import com.modelicious.in.logging.FileLog4jLevel;

import scala.util.Try
import scala.xml.{Node, NodeSeq, Elem, UnprefixedAttribute, Null}
import scala.xml.transform.RewriteRule

import org.apache.log4j.Logger

import scala.collection.immutable.ListMap

  
object Implicits {

    // http://stackoverflow.com/questions/23128433/simple-iteration-over-case-class-fields
    // Nota mia: /: y \: son operadores para foldLeft y Right
    implicit class CaseClassPrettyOuputFormatter(c: AnyRef) {
      private def getFields: ListMap[String, Any] = {
        val fields = (Map[String, Any]() /: c.getClass.getDeclaredFields) { (a, f) =>
          f.setAccessible(true)
          a + (f.getName -> f.get(c))
        }
        
        ListMap(fields.toSeq.sortBy(_._1):_*)
      }
      
      def toStringWithFields: String = {
        val fields = getFields
        
        s"${c.getClass.getName.split('.').last}(${fields.mkString(", ")})"
      }
    
      def toXMLWithFields: scala.xml.Node = {
         val fields = getFields
        
        <a>{for(f <- fields) yield <b>{f._2}</b>.copy(label = f._1)}</a>.copy(label=c.getClass.getName.split('.').last)
      }
    } 
    
    
    implicit class LogExtender( logger: org.apache.log4j.Logger ) {
      def file( msg: String ) = logger.log(FileLog4jLevel.FILE, msg)
    }
    
  // http://stackoverflow.com/a/7739960/1829897
  import scala.language.implicitConversions
  implicit def enrichNodeSeq(nodeSeq: scala.xml.NodeSeq) = new AnyRef {
    def textOption : Option[String] = {
      val text = nodeSeq.text
      if (text == null || text.length == 0) None else Some(text)
    }

    def textOrElse(elze : String) : String = textOption.getOrElse(elze)
    
    def textAsDoubleOption : Option[Double] = {
      val text = nodeSeq.text
      Try { text.toDouble }.toOption
    }

    def textAsDoubleOrElse(elze : Double) : Double = textAsDoubleOption.getOrElse(elze)
    
    def textAsIntOption : Option[Int] = {
      val text = nodeSeq.text
      Try { text.toInt }.toOption
    }

    def textAsIntOrElse(elze : Int) : Int = textAsIntOption.getOrElse(elze)
    
    def textAsBoolOption : Option[Boolean] = {
      val text = nodeSeq.text
      Try { text.toBoolean }.toOption
    }

    def textAsBoolOrElse(elze : Boolean) : Boolean = textAsBoolOption.getOrElse(elze)
  }
  
}
  
  

  class AddChildrenTo(m: Node, newChild: NodeSeq) extends RewriteRule {
    def this(m: Node, newChild: Node) = this( m, NodeSeq.fromSeq( Seq( newChild ) ) )
    
    def addChild(n: Node, newChild: NodeSeq) = n match {
      case Elem(prefix, label, attribs, scope, child @ _*) =>
        Elem(prefix, label, attribs, scope, child.isEmpty, child ++ newChild : _*)
      case _ => sys.error("Can only add children to elements!")
    }
      
    override def transform(n: Node) = n match {
      case n @ `m` => addChild(n, newChild)
      case other => other
    }
  }
    