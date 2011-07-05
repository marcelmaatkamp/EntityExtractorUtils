package entity_extractor

import groovy.sql.Sql
import java.text.SimpleDateFormat

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation
import com.aliasi.classify.BaseClassifier
import com.aliasi.util.AbstractExternalizable

class NickLocation {
    Logger log = LoggerFactory.getLogger(NickLocation.class)

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

    def getClassifiers() {
        String[] klas = [
                        "src/main/resources/classifiers/ner-eng-ie.crf-4-conll-distsim.ser.gz",
                        "src/main/resources/classifiers/ner-eng-ie.crf-3-all2008-distsim.ser.gz",
                        "src/main/resources/classifiers/ner-eng-ie.crf-4-conll.ser.gz",
                        "src/main/resources/classifiers/ner-eng-ie.crf-3-all2008.ser.gz"
                ]

                AbstractSequenceClassifier[] classifier = new AbstractSequenceClassifier[4];
                for (int i = 0; i < klas.length; i++) {
                    classifier[i] = CRFClassifier.getClassifierNoExceptions(klas[i]);
                }

        return classifier

    }

    def determineLocation(classifier, nick_id) throws IOException {
        def writer = new FileWriter("NER_lokaties.csv", false);
        String[] klas = [
                "src/main/resources/classifiers/ner-eng-ie.crf-4-conll-distsim.ser.gz",
                "src/main/resources/classifiers/ner-eng-ie.crf-3-all2008-distsim.ser.gz",
                "src/main/resources/classifiers/ner-eng-ie.crf-4-conll.ser.gz",
                "src/main/resources/classifiers/ner-eng-ie.crf-3-all2008.ser.gz"
        ]
        String nickname = entity_nick.firstRow("select name from entity_nick where id = $nick_id").name
        String chatinhoud = getChatInhoudUniek(nick_id)
        // println "nick($nick_id/$nickname).chats: " + chatinhoud.toString()


        Set lokatielist = new HashSet();

        for (int j = 0; j < 4; j++) {
            List<List<CoreLabel>> out = classifier[j].classify(chatinhoud);
            for (List<CoreLabel> sentence: out) {
                for (CoreLabel word: sentence) {
                    String ne = word.get(AnswerAnnotation.class);
                    String type = null;

                    if ("LOCATION".equals(ne)) {
                        String lokatie = word.word();
                        // println "nick($nick_id/$nickname): ne($ne), lok($lokatie)"


                        if (!"confucius".equals(lokatie.toLowerCase())) {
                            lokatielist.add(lokatie);
                        }

                    }
                }
            }
        }

        if(lokatielist.size() > 0 ) {
            println "$nickname\t $lokatielist"
        }

        writer.append('\n');
        writer.flush();

    }

    def NickLocation() {
        AbstractSequenceClassifier[] classifier = getClassifiers()
        entity_chat_user_message.eachRow("select distinct chatnick.nick_id from entity_chat_irc_user_message chatnick inner join entity_nick nick on chatnick.nick_id = nick.id order by nick.name") {
            determineLocation(classifier, it.nick_id)
        }
    }

    public static void main(String[] args) {
        def database = new NickLocation()
    }
}
