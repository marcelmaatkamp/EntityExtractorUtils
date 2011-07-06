@Grapes([
  @Grab('net.sourceforge.htmlunit:htmlunit:2.4'),
])

package utils.google

import com.gargoylesoftware.htmlunit.WebClient

class GoogleSearch { 

  static void main(args) { 
    def search = new GoogleSearch(args[0])
  }

  def GoogleSearch(query) { 
    def webClient = new WebClient()
    def page = webClient.getPage('http://www.google.com')
    // check page title
    assert 'Google' == page.titleText
    // fill in form and submit it
    def form = page.getFormByName('f')
    def field = form.getInputByName('q')
    field.setValueAttribute('Groovy')
    def button = form.getInputByName('btnG')
    def result = button.click()
    // check groovy home page appears in list (assumes it's on page 1)
    assert result.anchors.any{ a -> a.hrefAttribute == 'http://groovy.codehaus.org/' }
println result
  }
}
