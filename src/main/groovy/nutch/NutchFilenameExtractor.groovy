package nutch

// @Grapes([
//   // @Grab('org.apache.mahout.hadoop:hadoop-core:0.20.1'),
//   @Grab('org.apache.mahout.hadoop:hadoop-core:0.19.1'),
//   @Grab('org.apache.nutch:nutch:1.3'),
//   @Grab('org.apache.tika:tika-core:0.9'),
//   @Grab('org.apache.tika:tika-parsers:0.9'),
//   @Grab('commons-lang:commons-lang:2.2'),
//   // @GrabConfig(systemClassLoader=true, initContextClassLoader=true)
// ])

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
 
class NutchFilenameExtractor {

  static void main(String[] args) {
    def showContent = new NutchFilenameExtractor(args)
  }

  def NutchFilenameExtractor(argv) { 

    Map<String, Set> ids = new HashMap<String, Set>()
    def tika = new Tika()
    def tikaMimeTypes = TikaConfig.getDefaultConfig().getMimeRepository();
    def conf = NutchConfiguration.create();
    def fs = FileSystem.parseArgs(argv, 0, conf);

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
            def contentUrl = nutchContent.url
            def contentBase = nutchContent.base
            def nutchContentType = nutchContent.contentType
            def content = nutchContent.content
            def contentLength = nutchContent.metadata.get("Content-Length") as int
            def contentLocation = nutchContent.metadata.get("Location")
            def contentType = nutchContent.metadata.get("Content-Type")

            // def type = tika.detect(content, contentUrl as String)

            if( contentType == null) {    
              contentType = nutchContentType
            }
            if(contentType.contains(";")) { 
              contentType = contentType.substring(0, contentType.indexOf(";")-1)
            }

            // println "$key,$contentLength bytes"
            // println "<tr><td>$contentType</td><td>$contentLength</td><td><a href=\"$key\">$key</a></td></tr>"
            // println "$key,$contentType,$contentLength,"+nutchContent

            println "key[$key]: $contentBase -> $contentUrl type[$type] [$nutchContentType/$contentType] $contentLength bytes"
          }
          reader.close();
        }
      }
    } finally {
      fs.close();
    }

    ids.eachWithIndex { entry, i ->
      println "csmera_id [$entry.key] made the following photo's: "
      entry.value.each { value ->
        println " -> $value"
      }
    }
  }
  def printMetaData(metadata) {
    metadata.names().each {  name ->
      def value = metadata.get(name)?.trim()
      println " $name:$value"
    }  
  }  

  def getComments(metadata) {
    def software = metadata.get("Software")
    def author = metadata.get("Author")
    def comment = metadata.get("Comment")
    def imageDescription = metadata.get("Image Description")
    def result = [software: software, author: author, comment: comment]

    metadata.names().each {  name ->
      if(name.startsWith("Unknown tag")) {
        result += [name, metadata.get(name)]
      }
    }
    return result
  }

  def extractTime(metadata) {
    def timeOriginal = metadata.get("Date/Time Original")	// 2006:08:09 09:22:56
    def timeExifOriginal = metadata.get("exif:DateTimeOriginal")// 2010-01-05T15:22:03[false]
    def time= metadata.get("Date/Time")
    def timeModified  = metadata.get("Last-Modified")  		// 2006-09-14T00:03:43[false]
    def timeDigitized = metadata.get("Date/Time Digitized") 	// 2006:08:09 09:22:56

    return [ "timeOriginal": timeOriginal, "time": time, timeModified: timeModified ]
  }
}
