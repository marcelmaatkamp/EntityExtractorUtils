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
 
class NutchTikaExtractor {

  static void main(String[] args) {
    def showContent = new NutchTikaExtractor(args)
  }

  def NutchTikaExtractor(argv) { 

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

            def type = tika.detect(content, contentUrl as String)

            if( contentType == null) {    
              contentType = nutchContentType
            }

            if( contentType.equalsIgnoreCase("image/jpg") || contentType.equalsIgnoreCase("image/jpeg")) { 
              def parser = new JpegParser();
              def handler = new DefaultHandler();
              def context = new ParseContext();
              def metadata = new Metadata();
              metadata.set(Metadata.CONTENT_TYPE, contentType);
              try { 
                parser.parse(new ByteArrayInputStream(content),handler,metadata,context);
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
                if(cameraId) { 
                  if(!ids.containsKey(cameraId)) { 
                    ids.put(cameraId, new HashSet<String>())
                  }
                  ids.get(cameraId).add(contentUrl)
                }
                println "$contentUrl: ($width x $height) "+(cameraMake?"[$cameraMake]":"") + (cameraModel?"[$cameraModel]":"") + (cameraId?", id[$cameraId]":"")

              } catch (Exception e) { 
                println "$contentUrl: $e"
              }
            // } else if( contentType.equalsIgnoreCase("image/png") || contentType.equalsIgnoreCase("image/gif")) {
            } else if( contentType.startsWith("image")) {
              def ImageParser parser = new ImageParser();
              def handler = new DefaultHandler();
              def context = new ParseContext();
              def metadata = new Metadata();
              metadata.set(Metadata.CONTENT_TYPE, contentType);
              try { 
                parser.parse(new ByteArrayInputStream(content),handler,metadata,context);
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
// meta[Compression Lossless=true tIME=year=2010, month=12, day=17, hour=17, minute=41, second=32 Dimension PixelAspectRatio=1.0 tiff:ImageLength=640 height=640 pHYs=pixelsPerUnitXAxis=7874, pixelsPerUnitYAxis=7874, unitSpecifier=meter tiff:ImageWidth=660 Chroma BlackIsZero=true Document ImageModificationTime=year=2010, month=12, day=17, hour=17, minute=41, second=32 Data BitsPerSample=8 8 8 Dimension VerticalPixelSize=0.12700026 tiff:BitsPerSample=8 8 8 width=660 Dimension ImageOrientation=Normal Chroma Gamma=0.45455 Compression CompressionTypeName=deflate Data SampleFormat=UnsignedIntegral Dimension HorizontalPixelSize=0.12700026 Transparency Alpha=none Chroma NumChannels=3 Compression NumProgressiveScans=1 Chroma ColorSpaceType=RGB IHDR=width=660, height=640, bitDepth=8, colorType=RGB, compressionMethod=deflate, filterMethod=adaptive, interlaceMethod=none Data PlanarConfiguration=PixelInterleaved gAMA=45455 Content-Type=image/png ]
http://c7jh7jz
                println "$contentUrl .. ($width x $height) "+ (metadata?"meta[$metadata]":"")
              } catch ( Exception e ) { 
                println "$contentUrl: $e"
              }
            }
            // println "key[$key]: $contentBase -> $contentUrl type[$type] [$nutchContentType/$contentType] $contentLength bytes"
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
