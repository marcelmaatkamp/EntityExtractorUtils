package nutch

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
 
class NutchExtractor {

  def MAX_FILE_LENGTH = 150

  static void main(String[] args) {
    def showContent = new NutchExtractor(args)
  }

  def NutchExtractor(argv) { 

    Map<String, Set> ids = new HashMap<String, Set>()
    def tika = new Tika()
    def tikaMimeTypes = TikaConfig.getDefaultConfig().getMimeRepository();
    def conf = NutchConfiguration.create();
    def fs = FileSystem.parseArgs(argv, 0, conf);

    try {
      String nutchLocation = "/Volumes/Descartes-1/"
      new File(nutchLocation + "crawler_data/segments").eachFile { file ->
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

              def f = new File("extracted/" + url.host + "/" + filename)
              try { 
                def newLength = nutchContent.metadata.get("Content-Length") as int 
                if(!f.exists()) { 
                  println "[$key] [$contentType] [$newLength] $filename"
                  f.createNewFile()
                  f << nutchContent.content
                } else {
                  if(f.length() < newLength) { 
                    println "[$key] already exists, but old.size($f.length()) and new.size($newLength)  .. "
                  } else { 
                    println "[$key] already exists "
                  }
                }  
              } catch(Exception e) { 
                println "Exception: $e"
              }
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
