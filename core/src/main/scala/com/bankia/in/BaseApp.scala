package com.modelicious.in

import com.modelicious.in.stage.StageManager
import scala.xml._
import com.modelicious.in.app.{ Application, AppConfig }
import com.modelicious.in.tools.Implicits._
import com.modelicious.in.tools.{Logs, AddChildrenTo,DocGenerator}
import org.apache.log4j.Logger
import com.modelicious.in.tools.Mail.send_mail
import scala.xml.transform.RuleTransformer
import org.fusesource.jansi.AnsiConsole
import com.modelicious.in.html.ScalateSupport
import com.modelicious.in.tools.EnvReader


import org.apache.log4j.MDC

/**
  * The Application Base class 
  *
  * @author Alberto Lago
  * @author Luis Francisco Sanchez
  * @version 1.0
  * @todo This just allows minimal extension needed to have a second app with H2O. No need for more right now
  * 
  */
class BaseApp extends Serializable {

  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  def Init: Unit = {
    com.modelicious.in.Init.initialize
  }
  
  def AppInit ( xml_conf: scala.xml.Elem ) = {
    val appconfig = new AppConfig(xml_conf)
    
    Application.init(appconfig)
    MDC.put("Pipeline", appconfig.Pipeline)
  }

  /**
   * The entry point to run the application
   * 
   * Must be submited with this expression:
   * 
   * {{{
   * spark-submit  --class BaseApp "\target\scala-2.10\Application.jar" [config_folder] [logfile_name] 
   * }}}
   * 
   * where:
   * 		- config_folder is a folder with the configuration xmls like it is explained in the documentation.
   * 		- logfile_name is the file where logging will be printed out.
   * 
   *  
   * 
   */
  def main(args: Array[String]): Unit = {

    AnsiConsole.systemInstall()
    
    val input_files_dir = new java.io.File( args(0) ).getAbsolutePath
    val logPath = args(1)
    
    // Config Log
    Logs.config_logger(getClass.getName, logPath)
    MDC.put("appName", "BDMODELOS")
    MDC.put("Phase", "INIT" )
    val config_files = list_config_files(input_files_dir)
    val xml_conf = scala.xml.Utility.trim(XML.load(config_files.head)).asInstanceOf[scala.xml.Elem]
        
    val ModelID = args.lift(4).getOrElse("m" + java.util.UUID.randomUUID().toString.takeRight(8))
    
    import java.text.SimpleDateFormat
    import java.util.Calendar

    val SessionID = args.lift(5).getOrElse("s"+ new SimpleDateFormat("YYYYMMddHHmmss").format( Calendar.getInstance.getTime))
    
    val environment = args.lift(2)
    val variables = args.lift(3)
    
    val env = new EnvReader( Seq(environment, variables).flatten , ModelID, SessionID )
    
    MDC.put("ModelID", ModelID)
    log.file("Files under " + input_files_dir + " : " + config_files)
   
    AppInit( env( xml_conf ) )   
    Init
    
    val author=env.getCurrentEnvironment()("environment.user")
    val path=env.getCurrentEnvironment()("environment.outputdir")+"/"+env.getCurrentEnvironment()("modelid")+"_"+env.getCurrentEnvironment()("sessionid")+".tex"
    Application.document.init(path,author)   
    
    var result: scala.xml.Node = <result/>

    var exit_code = 0
    
    var index = 0
    
    try {  
      for (file <- config_files.tail) {
        val file_conf = env(load_xml_file(file))
        index = StageManager.getStageIndex( file_conf.head.label )
        val stage = StageManager(file_conf)
        log.file("[STAGE] Launching Stage from File: " + file + ".")
        Application.document.addSection(file,"This is a stage type: "+file_conf.head.label)
        Application.document.addSubSection("Stage Configuration","")
        Application.document.addXml(Right(file_conf))
        
        log.file( "[DEBUG]: Data in Memory: " )
        Application.dataManager.debugMemory.foreach( { case (k, v)  => log.file( "Key " + k + " : " + v )} )
        
        Application.document.addSubSection("Stage Run: ","")
        stage.preExecute()
      
        val stage_result = stage.execute(file_conf) %  new UnprefixedAttribute("file",file,Null)
      
        result = new RuleTransformer(new AddChildrenTo(result,stage_result)).transform(result).head
      
        stage.postExecute()
        log.file("[STAGE] Stage from File: " + file + " finished.")
        }
      } catch {
        case e: Exception => { 
          log.file( "Error in main loop:" + e.getStackTraceString + "\n" +e.getMessage() )
          val error = <error><trace>{e.getStackTraceString}</trace><msg>{e.getMessage()}</msg></error> 
          result = new RuleTransformer(new AddChildrenTo(result,error)).transform(result).head
          
          exit_code = -index         
         }
      }
    
    val format = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss")
    val fecha = format.format( new java.util.Date() )
       
    val output_xml = input_files_dir + s"/_result_${fecha}.xml" // TODO: Nombre deberia venir de la configuracion
    XML.save(output_xml, result )
    
    val html_string = ScalateSupport.render("result.ssp", Map("res" -> result) )
    val output_html = input_files_dir + s"/_result_${fecha}.html"
    
    import java.io._
    val file = new File(output_html)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(html_string)
    bw.close()
    
    val mail_enabled = Application.mailOptions.mailEnabled
    val mail_recipient = Application.mailOptions.mailRecipient
//    
    log.file(mail_enabled + " " + mail_recipient)
    
    val printer = new PrettyPrinter(180, 2)
    if ((mail_enabled) && (mail_recipient!="none")) {
      send_mail(log, mail_recipient, "resultados", output_xml, output_html )//printer.format(result))
    }
    
    Application.document.close()
    Application.stop()
    AnsiConsole.systemUninstall()
    sys.exit( exit_code )
  }

  /**
   * Helper method that returns a List with all the config xml files under a given path
   * 
   * There are some conventions by default:
   * 		* Only xml files are listed
   *    * Files which name start by an underscored are filtered out
   * 
   * @param dir Configuration folder path.
   * @return List with the all the xml absolute FilePaths as Strings
   *  
   */
  protected def list_config_files(dir: String): List[String] = {
    import java.io.File

    val d = new File(dir)
    
    log.file( dir + " exists: " + d.exists)
    log.file( dir + " isDir: " + d.isDirectory)
    
    
    if (d.exists && d.isDirectory) {
      d.listFiles
        // Is file
        .filter(_.isFile)
        // Filter only xml
        .filter(_.getName().endsWith("xml"))
        // filter prefixed by _ (So we can comment out files fast)
        .filter(!_.getName().startsWith("_"))
        // Sort by name
        .sortBy { x => x.getName }
        // map to full path string
        .map { x => x.getAbsolutePath }
        // as List
        .toList
    } else {
      List[String]()
    }
  }

  /**
   * Returns an xml.Elem from file that exists at given path
   * 
   * @param path Absolute path to file.
   * @return XML Element with configuration
   */
  protected def load_xml_file(path: String): scala.xml.Elem = {
    scala.xml.Utility.trim(XML.load(path)).asInstanceOf[scala.xml.Elem]
  }
  
}