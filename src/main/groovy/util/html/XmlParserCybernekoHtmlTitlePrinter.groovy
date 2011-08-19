package util.html

import com.gargoylesoftware.htmlunit.WebClient

class XmlParserCybernekoHtmlTitlePrinter {

  static void main(args) {
    def search = new XmlParserCybernekoHtmlTitlePrinter(args)
  }

  def XmlParserCybernekoHtmlTitlePrinter(args) {
    new File(args[0]).eachLine { url -> 
        def parser = new org.cyberneko.html.parsers.SAXParser()
        parser.setFeature('http://xml.org/sax/features/namespaces', false)
      try { 
        def page = new XmlParser(parser).parse('http://groovy.codehaus.org/')
        println "$url: " + page.titleText
      } catch(Exception e) { 
        println "Exception: $e"
      }
    }
  }
}
