package util.html

import com.gargoylesoftware.htmlunit.WebClient

class WebClientHtmlTitlePrinter {

  static void main(args) {
    def search = new WebClientHtmlTitlePrinter(args)
  }

  def WebClientHtmlTitlePrinter(args) {
    new File(args[0]).eachLine { url -> 
      try { 
        def webClient = new WebClient()
        def page = webClient.getPage(url)
        println "$url: " + page.titleText
      } catch(Exception e) { 
        println "Exception: $e"
      }
    }
  }
}
