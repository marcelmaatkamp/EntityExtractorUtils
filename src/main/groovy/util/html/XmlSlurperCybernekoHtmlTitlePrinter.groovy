package util.html

import com.gargoylesoftware.htmlunit.WebClient

class XmlSlurperCybernekoHtmlTitlePrinter {

  static void main(args) {
    def search = new XmlSlurperCybernekoHtmlTitlePrinter(args)
  }

  def XmlSlurperCybernekoHtmlTitlePrinter(args) {
    new File(args[0]).eachLine { url -> 
      def parser = new org.cyberneko.html.parsers.SAXParser()
      parser.setFeature('http://xml.org/sax/features/namespaces', false)
      def filename = ""
      try { 
        filename = url.substring(url.lastIndexOf('/')+1, url.length())
        def page = new XmlSlurper(parser).parse(url)
        def file = new File(filename) 
        file.createNewFile()
        file  << page
        println "$url: " + page.size()
        
      } catch(Exception e) { 
        println "Exception for filename '$filename': $e"
      }
    }
  }
}
