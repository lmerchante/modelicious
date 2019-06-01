import com.modelicious.in.data.parseURI
import org.scalatest._

class ArrayToTuple extends FlatSpec with Matchers {

  "parseURI" should " return an URI like memory:key as a tuple (\"memory\", \"key\", \"Map()\")" in {
    val uri = "memory:key"

    parseURI(uri) should be ("memory", "key", Map())
  }
  
  it should " return an URI like memory:key:opts as a tuple (\"memory\", \"key\", \"Map(myopt: \"Hola\")\")" in {
    val uri = "memory:key:myopt=Hola"

    parseURI(uri) should be ("memory", "key", Map(("myopt","Hola")))
  }
  
  it should " default to memory" in {
    val uri = "key"

    parseURI(uri) should be ("memory", "key", Map())
  }
  
  
  
  it should "throw when cannot create an URI" in {
    a [RuntimeException] should be thrownBy {
      val uri = "hdfs:key:wrong_opts"
      parseURI(uri) 
    } 
  }
  
  it should "throw when pased an empty URI" in {
    a [RuntimeException] should be thrownBy {
      val uri = ""
      parseURI(uri) 
    } 
  }

  
}