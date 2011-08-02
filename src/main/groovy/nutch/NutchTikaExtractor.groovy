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
import java.io.ByteArrayInputStream
import java.io.DataInput
import java.io.DataInputStream
import java.io.DataOutput
import java.io.DataOutputStream
import java.io.IOException
import java.util.Arrays
import java.util.zip.InflaterInputStream
 
//Hadoop imports
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.ArrayFile
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.ValueBytes
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.UTF8
import org.apache.hadoop.io.VersionMismatchException
import org.apache.hadoop.io.Writable
 
//Nutch imports
import org.apache.nutch.metadata.Metadata
import org.apache.nutch.protocol.Content
import org.apache.nutch.util.MimeUtil
import org.apache.nutch.util.NutchConfiguration

// Tika 
import org.apache.tika.Tika
import org.apache.tika.config.TikaConfig
import org.apache.tika.exception.TikaException
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.parser.ParsingReader
import org.apache.tika.parser.asm.ClassParser
import org.apache.tika.parser.audio.AudioParser
import org.apache.tika.parser.html.HtmlParser
import org.apache.tika.parser.image.ImageParser
import org.apache.tika.parser.microsoft.OfficeParser
import org.apache.tika.parser.opendocument.OpenOfficeParser
import org.apache.tika.parser.pdf.PDFParser
import org.apache.tika.parser.rtf.RTFParser
import org.apache.tika.parser.txt.TXTParser
import org.apache.tika.parser.xml.XMLParser
import org.apache.tika.sax.WriteOutContentHandler
import org.apache.tika.parser.jpeg.JpegParser
import org.apache.tika.parser.ParseContext
import org.xml.sax.helpers.DefaultHandler

// Natural language
import java.util.ArrayList
import com.cybozu.labs.langdetect.Detector
import com.cybozu.labs.langdetect.DetectorFactory
import com.cybozu.labs.langdetect.Language

// NLP
import com.aliasi.classify.BaseClassifier
import com.aliasi.util.AbstractExternalizable
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation
import edu.stanford.nlp.ling.CoreLabel

// NER
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation
import com.aliasi.classify.BaseClassifier
import com.aliasi.util.AbstractExternalizable
import org.apache.tika.parser.txt.CharsetDetector

 
class NutchTikaExtractor {

  static void main(String[] args) {
    def showContent = new NutchTikaExtractor(args)
  }

  def getClassifiers() {
    String[] klas = [
      "src/main/resources/classifiers/ner-eng-ie.crf-4-conll-distsim.ser.gz",
      "src/main/resources/classifiers/ner-eng-ie.crf-3-all2008-distsim.ser.gz",
      "src/main/resources/classifiers/ner-eng-ie.crf-4-conll.ser.gz",
      "src/main/resources/classifiers/ner-eng-ie.crf-3-all2008.ser.gz"
    ]

    AbstractSequenceClassifier[] classifier = new AbstractSequenceClassifier[4]
    for (int i = 0; i < klas.length; i++) {
      classifier[i] = CRFClassifier.getClassifierNoExceptions(klas[i])
    }
    return classifier
  }

  def determineLocation(classifier, txt) throws IOException {
    def entities = [] 
    for (int j = 0; j < 4; j++) {
      List<List<CoreLabel>> out = classifier[j].classify(txt)
      for (List<CoreLabel> sentence: out) {
        for (CoreLabel word: sentence) {
          String annotation = word.get(AnswerAnnotation.class)
          String valueWord = word.word()
          if(!"O".equals(word)) { 
println "$annotation: $valueWord" 
            entities = entities + ["$annotation":valueWord]
          }
        }
      }
    }
    return entities
  }


  def NutchTikaExtractor(argv) { 

    Map<String, Set> ids = new HashMap<String, Set>()

    // DetectorFactory.loadProfile("src/main/resources/profiles")
    // def classifier = (BaseClassifier<String>) AbstractExternalizable.readObject(new File("src/main/resources/langid-leipzig.classifier"))
    // def AbstractSequenceClassifier[] NERclassifier = getClassifiers()

    def tika = new Tika()
    def autodetectParser = new AutoDetectParser()
    def tikaMimeTypes = TikaConfig.getDefaultConfig().getMimeRepository()
    def conf = NutchConfiguration.create()
    def fs = FileSystem.parseArgs(argv, 0, conf)

    try {
      String nutchLocation = "/media/current/projects/nutch-1.3/runtime/local/"
      new File(nutchLocation + "crawl/segments").eachFile { file ->
        String segment = file.canonicalPath
        if(new File(segment + "/content/part-00000/data").exists()) { 
          Path path = new Path(segment, Content.DIR_NAME + "/part-00000/data")
          SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)
     
          Text key = new Text()
          Content nutchContent = new Content()
   
          while (reader.next(key, nutchContent)) {
            def contentUrl = nutchContent.url
            def contentBase = nutchContent.base
            def nutchContentType = nutchContent.contentType
            def content = nutchContent.content
            def contentLength = nutchContent.metadata.get("Content-Length") as int
            def contentLocation = nutchContent.metadata.get("Location")?.trim()
            def contentType = nutchContent.metadata.get("Content-Type")?.trim()

            def type = tika.detect(content, contentUrl as String)

            if( contentType == null) {    
              contentType = nutchContentType?.trim()
            }

         // METADATA
            Metadata metadata = new Metadata()
            metadata.set(Metadata.CONTENT_TYPE, contentType)
            def hndlr = new DefaultHandler()
            try {
              autodetectParser.parse(new ByteArrayInputStream(nutchContent.content), hndlr, metadata)
            } catch (Exception e) {
              // println "Exception: $e"
            }
  

         // LANGUAGE
         if(false) {  
            try { 
              def txt = tika.parseToString(new ByteArrayInputStream(nutchContent.content), metadata)
              // Detector detector = DetectorFactory.create()
              // detector.append(txt)
              if(txt && txt.length()>10) { 
                try { 
                  def charset = new CharsetDetector().setText(txt.bytes).detect()
                  println "$contentUrl: mime($contentType), size($nutchContent.content.length), charset.name($charset.name), char.lang($charset.language)"
                  // println "$contentUrl: lang("+detector.getProbabilities()+")"
                  // println "$contentUrl: NER.fromTxt: "+determineLocation(NERclassifier, txt)
                } catch( com.cybozu.labs.langdetect.LangDetectException e) { 

                }
              }
           // NLP
           // def classification = classifier.classify(txt)
           // println ("$contentUrl: NLP: "+classification.bestCategory())
            } catch( org.apache.tika.exception.TikaException et ) {

            }
          }

            if(contentType.startsWith("image")) { 
              def cameraId = metadata.get("Camera Id")?.trim()
              def cameraModel = metadata.get("Model")?.trim()
              def cameraMake = metadata.get("Make")?.trim()
              def width  = (metadata.get("width")?metadata.get("width"):metadata.get("Image Width"))?.trim()
              def height = (metadata.get("height")?metadata.get("height"):metadata.get("Image Height"))?.trim()
              def lat = metadata.get("geo:lat")?.trim()
              def lng = metadata.get("geo:long")?.trim()
              if(width && width.endsWith(" pixels")) { 
                width=width.substring(0, width.indexOf(" pixels"))
              }
              if(height && height.endsWith(" pixels")) {               
                height=height.substring(0,height.indexOf(" pixels"))
              }
              println "url: $contentUrl: size("+width+"x"+height+") "+(cameraMake?"[$cameraMake]":"") + (cameraModel?"[$cameraModel]":"") + (cameraId?", id[$cameraId]":"") + (lat?" - position($lat,$lng)":"")
              printMetaData(metadata)
              println ""

              if(cameraId) { 
                if(!ids.containsKey(cameraId)) { 
                  ids.put(cameraId, new HashSet<String>())
                }
                ids.get(cameraId).add(contentUrl)
              }
            } else if(contentType.startsWith("text/")) { 
            } else {

              println "url: $contentUrl"
              printMetaData(metadata)
              println ""
            } 
          }
          // println "key[$key]: $contentBase -> $contentUrl type[$type] [$nutchContentType/$contentType] $contentLength bytes"
          reader.close()
        }
      }
    } finally {
      fs.close()
    }

    ids.eachWithIndex { entry, i ->
      println "camera id [$entry.key] made the following photo's: "
      entry.value.each { value ->
        println " -> $value"
      }
      println ""
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
    def timeOriginal = metadata.get("Date/Time Original")  // 2006:08:09 09:22:56
    def timeExifOriginal = metadata.get("exif:DateTimeOriginal")// 2010-01-05T15:22:03[false]
    def time= metadata.get("Date/Time")
    def timeModified  = metadata.get("Last-Modified")      // 2006-09-14T00:03:43[false]
    def timeDigitized = metadata.get("Date/Time Digitized")   // 2006:08:09 09:22:56

    return [ "timeOriginal": timeOriginal, "time": time, timeModified: timeModified ]
  }
}
