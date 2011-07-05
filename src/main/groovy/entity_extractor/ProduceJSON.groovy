package entity_extractor

import groovy.sql.Sql
import org.apache.commons.io.FileUtils
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import org.xbill.DNS.ReverseMap
import org.xbill.DNS.Record
import org.xbill.DNS.Message
import org.xbill.DNS.Type
import org.xbill.DNS.DClass
import org.xbill.DNS.Section

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;

class ProduceJSON { 
  Logger log = LoggerFactory.getLogger(Database.class)

  def chatsDB  			= Sql.newInstance("jdbc:mysql://localhost:3306/chats", "grails", "grails", "com.mysql.jdbc.Driver")
  def entityDB 			= Sql.newInstance("jdbc:mysql://localhost:3306/entity_extractor_test", "grails", "grails", "com.mysql.jdbc.Driver")

  def q_entity      		= "insert into entity(version) values(0)"
  def q_entity_souce    	= "insert into mm_source_entities(mm_source_id, mm_entity_id) values(?,?)"
  def q_entity_country 		= "insert into entity_country(id,code,continent,full_name,iso3,name,number) values(?,?,?,?,?,?,?)"
  def q_entity_host     	= "insert into entity_host(id,geolocation_id,hostname,ipaddress,ipnumber,tor_proxy) values(?,?,?,?,?,?)"
  def q_entity_geolocation     	= "insert into entity_geolocation(id, country_code, hostname, ipaddress, latitude, locality, longitude, postal, region_code, x, y) values(?,?,?,?,?,?,?,?,?,?,?)"

  def entities  		= entityDB.dataSet("entity")
  def entity_host     		= entityDB.dataSet("entity_host")
  def entity_geolocation 	= entityDB.dataSet("entity_geolocation")
  def entity_country		= entityDB.dataSet("entity_country")
  def entity_nick		= entityDB.dataSet("entity_nick")
  def entity_status		= entityDB.dataSet("entity_status")
  def entity_message		= entityDB.dataSet("entity_message")
  def mm_source_entities	= entityDB.dataSet("mm_source_entities")
  
  def entity_chat_nick 		= entityDB.dataSet("entity_chat_nick")
  
  def entity_chat_status        = entityDB.dataSet("entity_chat_status")
  def entity_chat_irc_status    = entityDB.dataSet("entity_chat_irc_status") // entity_chat_irc_status || entity_chat_irc_user_status
  def entity_chat_user_status   = entityDB.dataSet("entity_chat_irc_user_status")
  def entity_chat_channel_status= entityDB.dataSet("entity_chat_irc_channel_status")

  def entity_chat_message	= entityDB.dataSet("entity_chat_message")
  def entity_chat_user_message  = entityDB.dataSet("entity_chat_irc_user_message")

  def sources          		= entityDB.dataSet("sources")
  def source_irc_files 		= entityDB.dataSet("source_irc_file")
  def source_irc_lines 		= entityDB.dataSet("source_irc_line")

  def sdf_session = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy")
  def sdf_simple = new SimpleDateFormat("EE dd-MM-y HH:mm", new Locale("NL", "nl"))
  def sdf_time = new SimpleDateFormat("HH:mm")

  // database
  def produceJSON() {
    // def files = source_irc_files.rows().each { file ->
    def file = source_irc_files.firstRow("select * from source_irc_file where id = 1")
    log.info " -[ $file.filename ]-"
    source_irc_lines.eachRow("select * from source_irc_line where source_irc_file_id=$file.id") { line ->
      log.info "$file.filename [$line.id] {$line.linenumber} $line.line"

      XStream xstream = new XStream(new JettisonMappedXmlDriver());
      xstream.setMode(XStream.NO_REFERENCES);

      System.out.println(line.toRowResult());
      System.out.println(xstream.toXML(line.toRowResult()));
    }
  }

  public static void main(String[] args) { 
    def database = new ProduceJSON().produceJSON() 
  }
}
