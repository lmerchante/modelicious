package com.modelicious.in

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType


/**
 * This package handles all the Application data related tasks.
 * 
 * This includes:
 *   - [[com.modelicious.in.data.DataWrapper]]: Wrapper for DataFrame that keeps the historic of the transformations applied to the original data 
 *   - [[com.modelicious.in.data.DataManager]]: Handles the interaction of the Application with the HDFA/memory or other places where the data can be stored.
 *   - [[com.modelicious.in.data.DataHandle]] and all their child classes: Implement the all the specific tasks related to read/write a specific data format. 
 * 
 * ===Usage in configuration files=== 
 * 
 * The configuration files can have several data elements in the form:
 * 
 *  {{{
 *  <output|input url="url"/>
 *  }}}
 * 
 * That will be translated to the right DataHandle and read/wrote to the phisical systems by DataManager as explained by the following rules: 
 * 
 * Both Input and output elements have an url attribute that identifies the format and location of the data to read/store. With a particularity for output.
 * It can chain more than one handler, separated by pipes ( `|` ) and the data will be stored on all the possible locations.
 * 
 * For example, the element:
 * 
 *  {{{
 *  <output url="memory:k1|hdfs:/user/A181345/cc/original.dat|csv:/user/A181345/cc/original.csv.bz:codec=bzip2&amp;delimiter=^|csv:/user/A181345/cc/original.csv|csvgz:/user/A181345/cc/original.csv.gz:delimiter=#"/>
 *  }}}
 * 
 * Will Create the following data elements:
 * 
 *   - ''memory:k1'': An in memory DataFrame with key k1 that will last for the next stage
 *   - ''hdfs:/user/A181345/cc/original.dat'':a Parquet file under the given hdfs location <code>/user/A181345/cc/original.dat</code>
 *   - ''csv:/user/A181345/cc/original.csv.bz:codec=bzip2&amp;delimiter=^'':a bzipped csv under <code>/user/A181345/cc/original.csv.bz</code> and with <code>^</code> as delimiter
 *   - ''csv:/user/A181345/cc/original.csv'':an uncompressed csv under <code>/user/A181345/cc/original.csv</code>
 *   - ''csvgz:/user/A181345/cc/original.csv.gz:delimiter=#'':a gzipped csv with <code>#</code> as delimiter and under <code>/user/A181345/cc/original.csv.gz</code> hdfs folder
 * 
 * Each handler is configured by a `(key:location:options)` tuple that is described for each case in the table below:
 *  * 
 *  
 *  
 * <table width="100%" cellspacing="1" border="0.6em" cellpadding="0" align="left">
 *  <thead>
 *    <tr>
 *      <th style="border-style: solid; border-width: 2px">Key</th>
 *      <th style="border-style: solid; border-width: 2px">Location</th>
 *      <th style="border-style: solid; border-width: 2px">Options</th>
 *      <th style="border-style: solid; border-width: 2px">Description</th>
 *      <th style="border-style: solid; border-width: 2px">Comments</th>
 *    </tr>
 *  </thead>
 *  <tbody>
 *    <tr>
 *      <td style="border-style: solid; border-width: 1px">memory</td>
 *      <td style="border-style: solid; border-width: 1px">key for the map that will store the data internally</td>
 *      <td style="border-style: solid; border-width: 1px">TODO: add the persistence of the data </td>
 *      <td style="border-style: solid; border-width: 1px">The data is stored in an internal Map. But this data will only last for the next stage (Could'nt find other way to ensure the data is removed from memory and we do not have OOM problems do to this)</td>
 *      <td style="border-style: solid; border-width: 1px">To be deprecated probably</td>
 *    </tr>
 *    <tr>
 *      <td style="border-style: solid; border-width: 1px">hdfs</td>
 *      <td style="border-style: solid; border-width: 1px">hadoop file system</td>
 *      <td style="border-style: solid; border-width: 1px">hdfs url</td>
 *      <td style="border-style: solid; border-width: 1px">Parquet File</td>
 *      <td style="border-style: solid; border-width: 1px"></td>
 *    </tr>
 *    <tr>
 *      <td style="border-style: solid; border-width: 1px">csv</td>
 *      <td style="border-style: solid; border-width: 1px">hadoop file system</td>
 *      <td style="border-style: solid; border-width: 1px">hdfs url</td>
 *      <td style="border-style: solid; border-width: 1px">All the available here <a href="https://github.com/databricks/spark-csv#features">https://github.com/databricks/spark-csv#features</a>. The most importants are:<br> <ul><li>delimiter. By default it uses ,. With this option we can change it to whatever (But pipe or &amp; as it would clash with internal URI handling)</li><li> codec: compression codec to use when saving to file. Should be the fully qualified name of a class implementing org.apache.hadoop.io.compress.CompressionCodec or one of case-insensitive shorten names (bzip2, gzip, lz4, and snappy). <br> Defaults to no compression when a codec is not specified.</li></ul></td>
 *      <td style="border-style: solid; border-width: 1px"></td>
 *    </tr>
 *    <tr>
 *      <td style="border-style: solid; border-width: 1px">csvgz</td>
 *      <td style="border-style: solid; border-width: 1px">hadoop file system</td>
 *      <td style="border-style: solid; border-width: 1px">Same as csv, just includes by default the compression to gz.</td>
 *      <td style="border-style: solid; border-width: 1px">Writes the data to a csv.gz</td>
 *      <td style="border-style: solid; border-width: 1px"></td>
 *    </tr>
 *    <tr>
 *      <td style="border-style: solid; border-width: 1px">lcsv</td>
 *      <td style="border-style: solid; border-width: 1px">local drive</td>
 *      <td style="border-style: solid; border-width: 1px">Same as csv</td>
 *      <td style="border-style: solid; border-width: 1px">Writes the data to a csv</td>
 *      <td style="border-style: solid; border-width: 1px"></td>
 *    </tr>
 *    <tr>
 *      <td style="border-style: solid; border-width: 1px">lcsvgz</td>
 *      <td style="border-style: solid; border-width: 1px">local drive</td>
 *      <td style="border-style: solid; border-width: 1px">Same as csv, just includes by default the compression to gz.</td>
 *      <td style="border-style: solid; border-width: 1px">Writes the data to a csv.gz</td>
 *      <td style="border-style: solid; border-width: 1px"></td>
 *    </tr>
 *    <tr>
 *      <td style="border-style: solid; border-width: 1px">lhdfs</td>
 *      <td style="border-style: solid; border-width: 1px">local drive</td>
 *      <td style="border-style: solid; border-width: 1px">local path</td>
 *      <td style="border-style: solid; border-width: 1px">Local Parquet File</td>
 *      <td style="border-style: solid; border-width: 1px"></td>
 *    </tr>
 *    <tr>
 *      <td style="border-style: solid; border-width: 1px">db</td>
 *      <td style="border-style: solid; border-width: 1px">"${schema}.${table}"</td>
 *      <td style="border-style: solid; border-width: 1px">partitionBy: comma separated list of columns used to partition the table. Order is important</td>
 *      <td style="border-style: solid; border-width: 1px">Inserts Data into given table</td>
 *      <td style="border-style: solid; border-width: 1px">Only for Output</td>
 *    </tr>
 *    <tr>
 *      <td style="border-style: solid; border-width: 1px">scores</td>
 *      <td style="border-style: solid; border-width: 1px">"${schema}.${table}"</td>
 *      <td style="border-style: solid; border-width: 1px">Same than DB, but it adds automaticaly the partitions modelID and SessionID</td>
 *      <td style="border-style: solid; border-width: 1px">Inserts Data into given table</td>
 *      <td style="border-style: solid; border-width: 1px">Only for Output</td>
 *    </tr>
 *  </tbody>
 * </table>
 */

package object data {
  
  type MetaData = scala.xml.Elem
  
  
  /**
   * This is just an approach to use C++'s RAII in Scala adding a finally clause that '''always''' closes the resource but eliminating  
   * all the scaffolding that this needs in scala.
   * 
   * This trait allows to use the method using, defined also in this package to handle the life of resource objects 
   * like it would be done e.g. with python '''with''' clause.
   * 
   * Usage: 
   * {{{
   *   using(new FileHandle("good")) { handle =>
	 *   handle.read
   *	 handle.write(42)
   *	}
	 *  //
   *  // handle is not visible down here and
	 *  // can't be abused, Yay
   * }}}
   * 
   * 
   * @see [[com.modelicious.in.data.using]]
   * 
   * @see http://naturalsoftware.blogspot.com.es/2009/06/more-scala-using-raisin.html
   * @see http://naturalsoftware.blogspot.com.es/2009/06/hey-scala-finalize-this.html
   * 
   * @todo Move to a more generic place, maybe tools
   */
  trait Disposable {
    def dispose(): Unit = {}
  }
  
  def parseURI( uri: String ) : (String, String, Map[String, String])  = {
    return uri.split(":") match {
      case Array(a) if a.isEmpty() => throw new RuntimeException("Invalid empty URI!") ;
      case Array(path) => ("memory", path, Map());
      case Array(handle, path) => (handle, path, Map());
      case Array(handle, path, opts) => (handle, path, parseOpts(opts));
      case _ => throw new RuntimeException("Invalid URI: " + uri ) 
      }
  }
  
  def parseOpts( opts: String ): Map[String, String] ={
    opts.split( "&" ).map{ x => x.split("=") }.map { x => (x(0), x(1)) }.toMap
  }
  
  /**
   * Method that always calls the Disposable object method dispose after working with it.
   * 
   * Usage: 
   * {{{
   *   using(new FileHandle("good")) { handle =>
	 *   handle.read
   *	 handle.write(42)
   *	}
	 *  //
   *  // handle is not visible down here and
	 *  // can't be abused, Yay
   * }}}
   * 
   * @tparam Class of type [[com.modelicious.in.data.Disposable]]
   * 
   * @see [[com.modelicious.in.data.Disposable]]
   * 
   */
  def using[T <% Disposable]
  (resource: T)(block: T => Unit) {
    try {
      block(resource)
    }
    finally {
      resource.dispose
    }
  }
  
  implicit class DataFrameExtender( df: DataFrame ) {
    def updateWithMetaData(schema : StructType): DataFrame = {
      schema.foldLeft(df) { (d, s) => 
          val column_with_new_metadata = d.col( s.name ).as( s.name, s.metadata ) 
          d.withColumn(s.name, column_with_new_metadata) // Sobreescribe la del mismo nombre
      } 
    }
  }
}
