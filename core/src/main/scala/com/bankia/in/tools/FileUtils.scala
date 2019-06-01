package com.modelicious.in.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, LocatedFileStatus, RemoteIterator}
import org.apache.spark.SparkContext


import java.io.{BufferedWriter, OutputStreamWriter, BufferedReader, InputStreamReader }

import org.apache.log4j.Logger
import Implicits._

object FileUtils {
  
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  def writeStringToFile( path: String, data: String ) = {
  
    val configuration = new Configuration()
    val hdfs = FileSystem.get( configuration )
    val file = new Path(path);
    if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
    val os = hdfs.create( file //,
//        new Progressable() {
//        public void progress() {
//            out.println("...bytes written: [ "+bytesWritten+" ]");
//        } }
          );
    val bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) )
    bw.write(data)
    bw.close()
    //hdfs.close()
  }
  
   def writeStringToLocalFile( path: String, data: String ) = {
    val file = new java.io.File(path)
    val bw = new java.io.BufferedWriter(new java.io.FileWriter(file))
    bw.write(data)
    bw.close()
  }
  
  def createFolder( path: String ) = {
    val configuration = new Configuration();
    val hdfs = FileSystem.get( configuration );
    val my_path = new org.apache.hadoop.fs.Path(path) 
    
    try { 
      hdfs.delete(my_path, true) 
      hdfs.mkdirs(my_path)  
    } 
    catch { 
      case e: Throwable => { log.file( "ERROR: FileUtils.createOrCleanFolder: " +  e.getLocalizedMessage ) } 
    }
    
    //hdfs.close()
  }
  
  def readFileToString( path: String ) : String = {
    val configuration = new Configuration();
    val hdfs = FileSystem.get( configuration );
    val file = new Path(path);
    val is = hdfs.open( file )
    val isr = new InputStreamReader(is)
    val br = new BufferedReader( isr )
  
    def readLines = Stream.cons(br.readLine, Stream.continually( br.readLine)).takeWhile(_ != null)
    
    //hdfs.close()
    
    readLines.mkString("\n")
  }
  
  def createOrCleanFolder( path: org.apache.hadoop.fs.Path ) = {
    val conf_hdfs = new Configuration()
    val hdfs = FileSystem.get(conf_hdfs)
    // Limpiamos existente o creamos directorio 
    
    try { 
      hdfs.delete(path, true)
      hdfs.mkdirs(path.getParent)
    } 
    catch 
    { case e: Throwable => { log.file( "ERROR: FileUtils.createOrCleanFolder: " +  e.getLocalizedMessage ) } }
  }
  
  def getAbsolutePath( path: String, base: String ) : String = {
    val conf_hdfs = new Configuration()
    val hdfs = FileSystem.get(conf_hdfs)
    val file = new Path(path);
    if( file.isAbsolute() ) {
      return path
    } 
    else {
      return base + "/" + path
    }
  }

  def list_files(dir: String): List[String] = {
    val conf_hdfs = new Configuration()
    val hdfs = FileSystem.get(conf_hdfs)

    val iter = hdfs.listFiles(new Path(dir), false)

    def listFiles(iter: RemoteIterator[LocatedFileStatus]) = {
      def go(iter: RemoteIterator[LocatedFileStatus], acc: List[String]): List[String] = {
        if (iter.hasNext) {
          val uri = iter.next.getPath.getName.toString()
          go(iter, uri :: acc)
        } else {
          acc
        }
      }
      go(iter, List())
    }
    listFiles(iter)
  }  
  
  // TODO: Error handling
  def copyToLocal( orig: String, dest: String, del_from_remote: Boolean = false ) = {
    val conf_hdfs = new Configuration()
    val hdfs = FileSystem.get(conf_hdfs)
    val org_file = new Path(orig)
    val dest_file = new Path(dest)
    
    val destFile = new java.io.File( dest ) 
    
    // cleanup But only the model, it can have siblings on the same folder.
    // Can fail if not exists, not a problem
    try {
      org.apache.commons.io.FileUtils.deleteDirectory(destFile)
    } catch { case _: Throwable => () }
    // Create hierarchy of folders if not exists
    val parentFile = destFile.getParentFile()
    parentFile.mkdirs()
    // copy
    
    log.file("Copying file from remote " + org_file + " to local destination: " + dest_file )
    
    hdfs.copyToLocalFile(del_from_remote, org_file, dest_file)
  }
  
    // TODO: Error handling
  def copyFromLocal( orig: String, dest: String) = {
    val conf_hdfs = new Configuration()
    val hdfs = FileSystem.get(conf_hdfs)
    val org_file = new Path(orig)
    val dest_file = new Path(dest)
    
    hdfs.delete(dest_file, true)
    hdfs.mkdirs(dest_file.getParent)
    
    log.file("Copying file from local " + org_file + " to remote destination: " + dest_file )
    
    hdfs.copyFromLocalFile(org_file, dest_file)
 
  }
  
  
  def deleteFolder( path: String ) = {
    val configuration = new Configuration();
    val hdfs = FileSystem.get( configuration );
    val my_path = new org.apache.hadoop.fs.Path(path) 
    
    try {
      
      log.file("Deleting folder " + my_path )
      
      val result = hdfs.delete(my_path, true)
      
      log.file( "deleteFolder result: " + result )
    } 
    catch { 
      case e: Throwable => { log.file( "ERROR: FileUtils.deleteFolder: " +  e.getLocalizedMessage ) } 
    }
    
    //hdfs.close()
  }
  
}

