package util

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
 
class TikaExplorer {

  static void main(String[] args) {
    def showContent = new TikaExplorer(args)
  }

  def tika = new org.apache.tika.Tika()

  def TikaExplorer(argv) { 
    new File("/Users/marcel/Desktop/Noah Holland/Noah Holland/").eachFile { file ->
      def contentType = tika.detect(file)

      if( contentType.startsWith("image/jpg") || contentType.startsWith("image/jpeg")) {
        def parser = new JpegParser();
        def handler = new DefaultHandler();
        def context = new ParseContext();
        def metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, contentType);
        try { 
          parser.parse(new ByteArrayInputStream(file.bytes),handler,metadata,context);
          def cameraId = metadata.get("Camera Id")?.trim()
          def cameraModel = metadata.get("Model")?.trim()
          def cameraMake = metadata.get("Make")?.trim()
          def width = metadata.get("Image Width")?.trim()
          if(width && width.endsWith(" pixels")) { 
            width=width.substring(0, width.indexOf(" pixels"))
          }
          def height = metadata.get("Image Height")?.trim()
          if(height && height.endsWith(" pixels")) {               
            height=height.substring(0,height.indexOf(" pixels"))
          }

          printMetaData(metadata)
          println ""
          println "$file.name: ($width x $height) "+(cameraMake?"[$cameraMake]":"") + (cameraModel?"[$cameraModel]":"") + (cameraId?", id[$cameraId]":"")

        } catch (Exception e) { 
          println "$e"
        }
      } else if( contentType.startsWith("image")) {
        def ImageParser parser = new ImageParser();
        def handler = new DefaultHandler();
        def context = new ParseContext();
        def metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, contentType);
        try { 
          parser.parse(new ByteArrayInputStream(file.bytes),handler,metadata,context);
          def width = metadata.get("Image Width")?.trim()
          if(width && width.endsWith(" pixels")) { 
            width=width.substring(0, width.indexOf(" pixels"))
          }
          def height = metadata.get("Image Height")?.trim()
          if(height && height.endsWith(" pixels")) { 
            height=height.substring(0,height.indexOf(" pixels"))
          }

          def time = metadata.get("tIME")?.trim()
          def tyear = metadata.get("tIME.year")?.trim()
          def year = metadata.get("year")?.trim()
          def text = metadata.get("tEXT")?.trim()

          printMetaData(metadata)
          println ""

          println "time{$time}, tyear($tyear) year($year): $text"
          println "$file.name .. ($width x $height) "+ (metadata?"meta[$metadata]":"")
        } catch ( Exception e ) { 
          println "$e"
        }
      }
    }
  }

  def printMetaData(metadata) {
    metadata.names().each {  name ->
      def value = metadata.get(name)?.trim()
      println " $name:$value"
    }
  }
}
