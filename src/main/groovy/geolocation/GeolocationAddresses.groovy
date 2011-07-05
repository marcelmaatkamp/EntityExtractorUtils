import groovyx.net.http.*
@Grab(group='org.codehaus.groovy.modules.http-builder', 
    module='http-builder', version='0.5.1' )
 
def http = new HTTPBuilder( 'http://twitter.com/statuses/' )
 
http.get( path: 'user_timeline.json', 
        query: [id:'httpbuilder', count:5] ) { resp, json ->
         
    println resp.status
     
    json.each {  // iterate over JSON 'status' object in the response:
        println it.created_at
        println '  ' + it.text
    }
}
