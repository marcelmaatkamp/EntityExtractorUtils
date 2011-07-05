package entity_extractor

import com.aliasi.classify.BaseClassifier
import com.aliasi.util.AbstractExternalizable
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation
import edu.stanford.nlp.ling.CoreLabel
import groovy.sql.Sql
import java.text.SimpleDateFormat
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.fop.util.text.AdvancedMessageFormat;
import org.apache.fop.util.text.AdvancedMessageFormat.Part;
import org.apache.fop.util.text.AdvancedMessageFormat.PartFactory;

class PV_Lex {
    Logger log = LoggerFactory.getLogger(NickLanguage.class)

    def chatsDB = Sql.newInstance("jdbc:mysql://localhost:3306/chats", "grails", "grails", "com.mysql.jdbc.Driver")
    def entityDB = Sql.newInstance("jdbc:mysql://localhost:3306/entity_extractor_test", "grails", "grails", "com.mysql.jdbc.Driver")

    def q_entity = "insert into entity(version) values(0)"
    def q_entity_souce = "insert into mm_source_entities(mm_source_id, mm_entity_id) values(?,?)"
    def q_entity_country = "insert into entity_country(id,code,continent,full_name,iso3,name,number) values(?,?,?,?,?,?,?)"
    def q_entity_host = "insert into entity_host(id,geolocation_id,hostname,ipaddress,ipnumber,tor_proxy) values(?,?,?,?,?,?)"
    def q_entity_geolocation = "insert into entity_geolocation(id, country_code, hostname, ipaddress, latitude, locality, longitude, postal, region_code, x, y) values(?,?,?,?,?,?,?,?,?,?,?)"

    def entities = entityDB.dataSet("entity")
    def entity_host = entityDB.dataSet("entity_host")
    def entity_geolocation = entityDB.dataSet("entity_geolocation")
    def entity_country = entityDB.dataSet("entity_country")
    def entity_nick = entityDB.dataSet("entity_nick")
    def entity_status = entityDB.dataSet("entity_status")
    def entity_message = entityDB.dataSet("entity_message")
    def mm_source_entities = entityDB.dataSet("mm_source_entities")

    def entity_chat_nick = entityDB.dataSet("entity_chat_nick")

    def entity_chat_status = entityDB.dataSet("entity_chat_status")
    def entity_chat_irc_status = entityDB.dataSet("entity_chat_irc_status") // entity_chat_irc_status || entity_chat_irc_user_status
    def entity_chat_user_status = entityDB.dataSet("entity_chat_irc_user_status")
    def entity_chat_channel_status = entityDB.dataSet("entity_chat_irc_channel_status")

    def entity_chat_message = entityDB.dataSet("entity_chat_message")
    def entity_chat_user_switch = entityDB.dataSet("entity_chat_irc_user_status_switch")

    def entity_chat_user_message = entityDB.dataSet("entity_chat_irc_user_message")

    def sources = entityDB.dataSet("sources")
    def source_irc_files = entityDB.dataSet("source_irc_file")
    def source_irc_lines = entityDB.dataSet("source_irc_line")

    def sdf_session = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy")
    def sdf_line = new SimpleDateFormat("dd-MM-yyyy HH:mm")
    def sdf_simple = new SimpleDateFormat("EE dd-MM-y HH:mm", new Locale("NL", "nl"))
    def sdf_time = new SimpleDateFormat("HH:mm")

    def getChatInhoudUniek(nick_id) {
        StringBuffer chats = new StringBuffer()
        Set<String>c=new HashSet<String>()

        entity_chat_user_message.eachRow("select line from entity_chat_irc_user_message chatmessage, entity_message message where chatmessage.id = message.id and nick_id = $nick_id") { row ->
            if(!c.contains(row.line)) {
                chats.append(row.line + "\n")
                c.add(row.line)
            }
        }
        return chats.toString()
    }


    def determineLanguage(nick_id) throws IOException, ClassNotFoundException {
        def nickname = entity_nick.firstRow("select name from entity_nick where id = $nick_id").name
        String chatinhoud = getChatInhoudUniek(nick_id)

        def classifier =
            (BaseClassifier<String>) AbstractExternalizable.readObject(new File("src/main/resources/langid-leipzig.classifier"))
        def classification = classifier.classify(chatinhoud)
        println ("$nickname\t "+classification.bestCategory());

    }



    // database

    def PV_Lex() {
      int index = 1
      def files = source_irc_files.rows("select * from source_irc_file where not channel and filename != '_away.log' and filename != '_status.log'")
      files.each { file -> 

      def nn = file.filename.substring(0, file.filename.length()-4)

      def pattern = new File("/Users/marcel/projects/pvgen/pv_chatlogs_eng.lyx").text

      AdvancedMessageFormat format = new AdvancedMessageFormat(pattern.replace("\\","\\\\"));
      Map params = new java.util.HashMap();
      params.put("pvnr", "29749048/${index}");
      params.put("title", file.filename);
      params.put("tijdstip_verbaal", new SimpleDateFormat("HH:mm:ss dd-MM-yyyy", new Locale("NL", "nl")).format(new Date()));
      params.put("bestandsnaam", file.filename);
      params.put("onderwerp", "Chatlogs");
 
      StringBuffer b = new StringBuffer()     
      def date = null
      def nick = null

      Set nicks = new HashSet();

      source_irc_lines.eachRow("select nick.name name,line.date date,message.line line from source_irc_line line, mm_source_entities es, entity_chat_irc_user_message chatmessage inner join entity_nick nick on nick.id=chatmessage.nick_id,entity_message message  where line.source_irc_file_id=$file.id and es.mm_source_id = line.id and es.mm_entity_id = message.id and message.id=chatmessage.id") { line ->
        // println "$file.filename $line.name $line.line"
        if(!date && line.date) { 
          date = line.date
        }

        nicks.add(line.name)

		b.append( 
"""\\begin_layout Gesprek
\\begin_inset Flex GesprekRegelWie
status collapsed
\\begin_layout Plain Layout
""" + sdf_line.format(line.date) + """
\\end_layout
\\end_inset

\\begin_inset Flex GesprekRegelWanneer
status collapsed
\\begin_layout Plain Layout
${line.name}
\\end_layout
\\end_inset

\\begin_inset Flex GesprekRegelWat
status collapsed
\\begin_layout Plain Layout
${line.line} 
\\end_layout
\\end_inset
\\end_layout
\\end_layout""")
 
      }

      if(nicks != null && nicks.size() > 1) {

      nicks.each { n ->
        def nname = n.replace("^","_")
        nname = nname.replace("@","_")
        nname = nname.replace("+","_")
        nname = nname.replace("!","_")
     
        if(!nick && !nn.toLowerCase().equals(nname.toLowerCase())) { 
          nick = n
        }
      }

      params.put("nickname", nn);

      def nnm = (nick?nick:"toddlerLuvNL")
      if(nicks.contains("toddlerLuvNL")) { 
        nnm = "toddlerLuvNL"
      }
      if(nicks.contains("totLoverNL")) { 
        nnm = "totLoverNL"
      }
      
      params.put("nickname_mickelsons", nnm);
      params.put("tijdstip_chat", new SimpleDateFormat("HH:mm:ss dd-MM-yyyy 'GMT' Z", new Locale("NL", "nl")).format(date));
      params.put("chattekst", b.toString());

      def pv = format.format(params);
      new File("chats/${file.filename}.${file.checksum}.lyx").write(pv)
      println " -[ $file.filename | "+params.get("nickname_mickelsons")+"]- " + nicks
    }
    index++
    }
    println "done printing "+files.size()+" nicks.."
    }

    public static void main(String[] args) {
        def database = new PV_Lex()
    }
	
}
