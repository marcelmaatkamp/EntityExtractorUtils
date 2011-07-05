package nl.klpd.nhtcu.tools.ner.stanford;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation;

public class Ner {
  Logger log = LoggerFactory.getLogger(Ner.class);

  private String readFile(String path) throws IOException {
      FileInputStream stream = new FileInputStream(new File(path));
      try {
        FileChannel fc = stream.getChannel();
        MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
        /* Instead of using default, pass in a decoder. */
        return Charset.defaultCharset().decode(bb).toString();
      }
      finally {
        stream.close();
      }
  }

  
  @SuppressWarnings("null")
  public static void main(String[] args) throws IOException {
    Ner ner = new Ner();
  }

  public Ner() throws IOException { 

    //als engels -> EN classifier
    //goede nl classifier zoeken
    //EN classifier testen en verbeteren indien mogelijk
    //meer locaties toevoegen?
    //alleen locaties opslaan in csv bestand?
    //xmls los opslaan?
    
    File folder = new File("src/main/resources/unieke_chats/");
    File[] listOfFiles = folder.listFiles();
      
    FileWriter writer = new FileWriter("NER_lokaties.csv", false);
    String[] klas = { 
      "src/main/resources//classifiers/ner-eng-ie.crf-4-conll-distsim.ser.gz",
      "src/main/resources//classifiers/ner-eng-ie.crf-3-all2008-distsim.ser.gz",
      "src/main/resources//classifiers/ner-eng-ie.crf-4-conll.ser.gz",
      "src/main/resources//classifiers/ner-eng-ie.crf-3-all2008.ser.gz"
    };
      
    //AbstractSequenceClassifier classifier = CRFClassifier.getDefaultClassifier();
    AbstractSequenceClassifier[] classifier = new AbstractSequenceClassifier[4];
    for (int i = 0; i < 4; i++ ){
      //System.out.println(i);
      //System.out.println(klas[i]);
      classifier[i] = CRFClassifier.getClassifierNoExceptions(klas[i]);
    }
      
      //AbstractSequenceClassifier classifier = CRFClassifier.getClassifierNoExceptions(klas);
      
    for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isFile()) {
        
        String chatfile = listOfFiles[i].getName();
        String chatpath = folder + "/" + chatfile;
        //System.out.println("File " + chatpath);
        writer.append(chatfile);
        String chatinhoud = new String();
  
        log.info("reading "+chatpath+" ..");
        chatinhoud = readFile(chatpath);
        chatinhoud = chatinhoud.replaceAll("\\[.*>", "");
        
        List lokatielist = new LinkedList();
        lokatielist = new ArrayList(); 
        
        for (int j = 0; j < 4; j++){
          List<List<CoreLabel>> out = classifier[j].classify(chatinhoud);
          for (List<CoreLabel> sentence : out) {
            for (CoreLabel word : sentence) {
              String ne = word.get(AnswerAnnotation.class);
              String type = null;
              if ("LOCATION".equals(ne)) {
                String lokatie=word.word();
                if (!(lokatielist.contains(lokatie))){
                  lokatielist.add(lokatie);
                  writer.append('|' + lokatie);
                }
              } 
            }
          }
        }
        
        writer.append('\n');
        writer.flush();
      }
    } 
    writer.close();
    System.out.println("klaar!");
  }
}
