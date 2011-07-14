package nutch.util

//JDK imports
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.InflaterInputStream;
 
//Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
 
//Nutch imports
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;

// Tika 
import org.apache.tika.Tika
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParsingReader;
import org.apache.tika.parser.asm.ClassParser;
import org.apache.tika.parser.audio.AudioParser;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.parser.image.ImageParser;
import org.apache.tika.parser.microsoft.OfficeParser;
import org.apache.tika.parser.opendocument.OpenOfficeParser;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.parser.rtf.RTFParser;
import org.apache.tika.parser.txt.TXTParser;
import org.apache.tika.parser.xml.XMLParser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.apache.tika.parser.jpeg.JpegParser
import org.apache.tika.parser.ParseContext;
import org.xml.sax.helpers.DefaultHandler
 
class NutchDBExport {

  def MAX_FILE_LENGTH = 150

  static void main(String[] args) {
    def showContent = new NutchDBExport(args)
  }

  def NutchDBExport(argv) { 

    Map<String, Set> ids = new HashMap<String, Set>()
    def tika = new Tika()
    def tikaMimeTypes = TikaConfig.getDefaultConfig().getMimeRepository();
    def conf = NutchConfiguration.create();
    def fs = FileSystem.parseArgs(argv, 0, conf);

def parser = new org.cyberneko.html.parsers.SAXParser()
parser.setFeature('http://xml.org/sax/features/namespaces', false)
    try {
      String nutchLocation = "/media/current/projects/nutch-1.3/runtime/local/"
      new File(nutchLocation + "crawl/segments").eachFile { file ->
        String segment = file.canonicalPath
        if(new File(segment + "/content/part-00000/data").exists()) { 
          Path path = new Path(segment, Content.DIR_NAME + "/part-00000/data");
          SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
     
          Text key = new Text();
          Content nutchContent = new Content();
   
          while (reader.next(key, nutchContent)) {
            if(nutchContent.content.length > 0) { 

              def url = new java.net.URL(nutchContent.url)
              def filename = url.path 
              filename = filename + ((url.path.endsWith("/"))?"index.html":"") 
              filename = filename + (!filename.contains(".")?".html":"") 
              filename = filename + (url.query?"?"+url.query:"")
              def directory = filename.substring(0, filename.lastIndexOf("/")) 
              def dir = new File( "extracted/" + url.host + "/" + directory ).mkdirs()
              if(filename.length() > MAX_FILE_LENGTH) {
                filename = filename.substring(0, MAX_FILE_LENGTH)
              }

              def contentType = nutchContent.metadata.get("Content-Type")
              if(!contentType) { 
                contentType = nutchContent.contentType
              }

              def version = nutchContent.version
              def base = nutchContent.base
              def mt = []

              nutchContent.metadata.names().each { name ->
                def value = nutchContent.metadata.get(name)
                mt += ["$name": value]
              }

              def links = []
              if(contentType == "text/html") { 
                def page = new XmlParser(parser).parseText(new String(nutchContent.content))
                // def data = page.depthFirst().A.'@href'.grep{ it != null && it.endsWith('.html') }
                def data = page.depthFirst().A.'@href'.grep{ it != null }
                data.each { link ->
                  def u = ""
                  if(link.startsWith("..")) { 
		    u = url.path + link
                  } else if(link.startsWith("/")) { 
                    u = url.protocol + "://"+url.host + link
                  } else if(link.startsWith("http:") || link.startsWith("email:") || link.startsWith("ftp:")) { 
                    u = link
                  } else { 
                    u = url.protocol + "://"+url.host + "/" + link
                  }
                  if(u.endsWith("/")) { 
                    u = u.substring(0,u.length()-1)
                  }
                  println "[$key] $link [->] $u"
                  links += [u]
                }
              }

// println "[$key] $links "
// println "[$key] $links "

            } else { 
              println "[$key] -> empty"
            }
          }
          reader.close();
        }
      }
    } finally {
      fs.close();
    }
  }
}
