package com.modelicious.in.tools

import java.io.{BufferedWriter,File,FileWriter}

class DocGenerator(docEnabled: Boolean, docFormat: String) {
  

  //var bw = new BufferedWriter(new FileWriter(""))
  var bw: BufferedWriter = _
  
  def init(path: String, author: String)=  {
    if (docEnabled) {
      if (docFormat=="latex") {init_latex(path,author)}
    }
  }
  def init_latex(path: String, author: String)=  {
    val file = new File(path)
    bw = new BufferedWriter(new FileWriter(file))
    bw.write("\\documentclass[12pt]{article}\n")
    
    bw.write("\\setlength{\\oddsidemargin}{-0.25in}\n")
    bw.write("\\setlength{\\textwidth}{7in}\n")
    bw.write("\\setlength{\\topmargin}{-.75in}\n")
    bw.write("\\setlength{\\textheight}{9.2in}\n")
    bw.write("\\usepackage{xcolor}\n")
    bw.write("\\colorlet{punct}{red!60!black}\n")
    bw.write("\\definecolor{delim}{RGB}{20,105,176}\n")
    bw.write("\\colorlet{numb}{magenta!60!black}\n")
    bw.write("\\usepackage{float}\n")
    bw.write("\\usepackage{tikz,pgfplots}\n")
    bw.write("\\usepackage{listings}\n")
    bw.write("\\lstset{\n")
    bw.write("  basicstyle=\\tiny,\n")
    bw.write("  columns=fullflexible,\n")
    bw.write("  showstringspaces=false,\n")
    bw.write("  commentstyle=\\color{gray}\\upshape,\n")
    bw.write("  breaklines=true,\n")
    bw.write("}\n")
    bw.write("\\lstdefinelanguage{XML}{\n")
    bw.write("  basicstyle=\\small,\n")
    bw.write("  morestring=[b][\\color{brown}],\n")
    bw.write("  morestring=[s][\\color{red}\\bfseries]{>}{<},\n")
    bw.write("  morecomment=[s]{<?}{?>},\n")
    bw.write("  stringstyle=\\color{black},\n")
    bw.write("  identifierstyle=\\color{blue},\n")
    bw.write("  keywordstyle=\\color{cyan},\n")
    bw.write("  morekeywords={name,use,xmlns,version,type}\n")
    bw.write("}\n")
    
    bw.write("\\lstdefinelanguage{JSON}{\n")
    bw.write("basicstyle=\\normalfont\\ttfamily,\n")
    bw.write("breaklines=true,\n")
    bw.write("frame=lines,\n")
    bw.write("literate=\n")
    bw.write(" *{0}{{{\\color{numb}0}}}{1}\n")
    bw.write("  {1}{{{\\color{numb}1}}}{1}\n")
    bw.write("  {2}{{{\\color{numb}2}}}{1}\n")
    bw.write("  {3}{{{\\color{numb}3}}}{1}\n")
    bw.write("  {4}{{{\\color{numb}4}}}{1}\n")
    bw.write("  {5}{{{\\color{numb}5}}}{1}\n")
    bw.write("  {6}{{{\\color{numb}6}}}{1}\n")
    bw.write("  {7}{{{\\color{numb}7}}}{1}\n")
    bw.write("  {8}{{{\\color{numb}8}}}{1}\n")
    bw.write("  {9}{{{\\color{numb}9}}}{1}\n")
    bw.write("  {:}{{{\\color{punct}{:}}}}{1}\n")
    bw.write("  {,}{{{\\color{punct}{,}}}}{1}\n")
    bw.write("  {\\{}{{{\\color{delim}{\\{}}}}{1}\n")
    bw.write("  {\\}}{{{\\color{delim}{\\}}}}}{1}\n")
    bw.write("  {[}{{{\\color{delim}{[}}}}{1}\n")
    bw.write("  {]}{{{\\color{delim}{]}}}}{1},\n")
    bw.write("}\n")
    
    bw.write("\\lstset{language=XML,basicstyle=\\ttfamily,breaklines=true}\n")
    bw.write("\\usepackage{seqsplit}\n")
    bw.write("\\usepackage{adjustbox} \n")
    bw.write("\\begin{document}\n")
    bw.write("\\title{Automated report}\n")
    bw.write("\\author{"+author+"}\n")
    bw.write("\\date{\\today}\n")
    bw.write("\\maketitle\n")
    bw.write("\\tableofcontents\n")
    bw.write("\\listoffigures\n")
    bw.write("\\listoftables\n")
    bw.write("\\newpage\n")
  }
  
  def enabled():Boolean={docEnabled}
  def format():String={docFormat}
  
  def addSection(title: String,text: String)=  {
    if (docEnabled && docFormat=="latex") {
      bw.write("\\section{"+title.replace("\\","\\textbackslash ").replace("_","\\_")+"}\n")
      add(text)
    }
  }
  
  def addSubSection(title: String,text: String)=  {
    if (docEnabled && docFormat=="latex") {
      bw.write("\\subsection{"+title.replace("\\","\\textbackslash ").replace("_","\\_")+"}\n")
      add(text)
    }
  }
  
  def addSubSubSection(title: String,text: String)=  {
    if (docEnabled && docFormat=="latex") {
      bw.write("\\subsubsection{"+title.replace("\\","\\textbackslash ").replace("_","\\_")+"}\n")
      add(text)
    }
  }
  
  def addItem(text: String)=  {
    if (docEnabled && docFormat=="latex") {
      bw.write("\\begin{itemize}\n")
      bw.write("\\item "+text.replace("\\","\\textbackslash ").replace("_","\\_")+"\n")
      bw.write("\\end{itemize}\n")
      bw.write("\n")
    }
  }
  def add(text: String, tab: Int=0)=  {
    if (docEnabled && docFormat=="latex") {
      bw.write("\\setlength{\\leftskip}{"+tab+"cm} "+text.replace("\\","\\textbackslash ").replace("_","\\_")+"\n")
      bw.write("\n")
    }
  }
  
  def addXml(input: Either[scala.xml.Node,scala.xml.Elem], tab: Int=0)=  {
    if (docEnabled && docFormat=="latex") {
      val text= input match {
        case Left(input)  => new scala.xml.PrettyPrinter(80, 4).format(input)
        case Right(input) => new scala.xml.PrettyPrinter(80, 4).format(input)
      }
      bw.write("\\begin{lstlisting}[language=XML,xleftmargin="+tab+"cm]\n")
      // had to split long queries to avoid dimension errors
      bw.write(text.replace("&quot;","\"").
        replace("&gt;",">").
        replace("&lt;","<").
        replace("select","\n select ").replace("Select","\n Select ").replace("SELECT","\n SELECT ") +"\n")
      bw.write("\\end{lstlisting}\n")
    }
  }
  
  def addTable(json: Map[String,(String,String)], caption:String)=  {
    if (docEnabled && docFormat=="latex") {
      bw.write("\\begin{table}[H]\n")
      bw.write("\\begin{center}\n")
      bw.write("\\begin{adjustbox}{max width=\\textwidth}\n")
      bw.write("\\begin{tabular}{| l | l | l |}\n")
      bw.write("\\hline\n")
      json.toSeq.sortBy(_._1).map{ x=>
        bw.write(x._1.replace("_","\\_") +" & " +x._2._1 +" & " +x._2._2 + "\\\\ \\hline\n"  )
      }
      bw.write("\\end{tabular} \n")
      bw.write("\\end{adjustbox}\n")
      bw.write("\\caption{"+ caption.replace("\\","\\textbackslash ").replace("_","\\_") +"} \n")
      bw.write("\\end{center}\n")
      bw.write("\\end{table}\n")
    }
  }
  
  def addJSON(json: String, tab: Int=0)=  {
    if (docEnabled && docFormat=="latex") {
      bw.write("\\begin{lstlisting}[language=JSON,xleftmargin="+tab+"cm]\n")
      bw.write(json+" \n"  )
      bw.write("\\end{lstlisting}\n")
    }
  }
  
  def addPlotDeciles(json: Map[String,(String,String)], caption:String)=  {
    if (docEnabled && docFormat=="latex") {
        bw.write("\\begin{figure}[H]\n")
        bw.write("\\centering\n")
        bw.write("\\begin{tikzpicture}\n")
        bw.write("\\begin{axis}[\n")
        bw.write("	xlabel=Deciles,\n")
        bw.write("	ylabel=Probabilidad]\n")
        bw.write("\\addplot[color=red,mark=x] coordinates {\n")
        json.toSeq.sortBy(_._1).map(x=> if (x._1.toLowerCase().contains("decil")) bw.write("("+x._1.toLowerCase().replace("decil","")+","+x._2._2+")"))
        bw.write("	};\n")
        bw.write("\\end{axis}\n")
        bw.write("\\end{tikzpicture}\n")
        bw.write("\\caption{"+caption.replace("\\","\\textbackslash ").replace("_","\\_")+"}\n")
        bw.write("\\end{figure}\n")
    }
  }
  
  def addPlotLearningCurve(porcentages: Array[Double], ECM_train: Array[Double], ECM_test: Array[Double], ECM_train_std: Array[Double], ECM_test_std:Array[Double], caption:String)=  {
    if (docEnabled && docFormat=="latex") {
        bw.write("\\begin{figure}[H]\n")
        bw.write("\\centering\n")
        bw.write("\\begin{tikzpicture}\n")
        bw.write("\\begin{axis}[\n")
        bw.write("	xlabel=Porcentajes,\n")
        bw.write("	ylabel=Mean Square Error]\n")
        bw.write("\\addplot+[mark=x,color=red,error bars/.cd,y dir=both,y explicit ]  coordinates {\n")
        porcentages.zip(ECM_train).zip(ECM_train_std).map(x=> bw.write("("+x._1._1+","+x._1._2+") +- (0,"+x._2+")"))
        //(1,0.0319) += (0,-0.0035) -=(0,-0.0135) for asymetric bars
        bw.write("	};\n")
        bw.write("\\addlegendentry{ECMTrain}")
        bw.write("\\addplot+[mark=x,color=blue,error bars/.cd,y dir=both,y explicit ] coordinates {\n")
        porcentages.zip(ECM_test).zip(ECM_test_std).map(x=> bw.write("("+x._1._1+","+x._1._2+") +- (0,"+x._2+")"))
        bw.write("	};\n")
        bw.write("\\addlegendentry{ECMTest}")         
        bw.write("\\end{axis}\n")
        bw.write("\\end{tikzpicture}\n")
        bw.write("\\caption{"+caption.replace("\\","\\textbackslash ").replace("_","\\_")+"}\n")
        bw.write("\\end{figure}\n")
    }
  }
  
   def addQuery(sql: String, tab: Int=0)=  {
    if (docEnabled && docFormat=="latex") {
      // had to split long queries to avoid dimension errors
      bw.write("\\begin{lstlisting}[language=SQL,xleftmargin="+tab+"cm]\n")
      bw.write(sql.replace("select","\n select ").replace("Select","\n Select ").replace("SELECT","\n SELECT ")+"\n")
      bw.write("\\end{lstlisting}\n")
    }
  }
    
    
  def close() ={
    if (docEnabled && docFormat=="latex") {
      bw.write("\\end{document}")
      bw.close()
    }
  }
  
}

