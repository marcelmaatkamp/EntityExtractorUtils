@Grapes([
  @Grab('mysql:mysql-connector-java:5.1.13'),
  @Grab('commons-codec:commons-codec:1.4'),
  @Grab('commons-io:commons-io')
])

import groovy.sql.Sql
import org.apache.commons.io.FileUtils

public static String getSHAsum(File file) {
        def shasum = java.security.MessageDigest.getInstance("SHA-256").digest(file.bytes);
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < shasum.length; i++) {
            sb.append(Integer.toString((shasum[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString()
    }

def sql = Sql.newInstance("jdbc:mysql://localhost:3306/entity_extractor_test", "grails", "grails", "com.mysql.jdbc.Driver")

/**
  try { 
    sql.execute("drop table irc_chatfile")
    sql.execute("drop table irc_chatline")
    sql.execute("drop table irc_chatfile_chatline")
  } catch(Exception e) { 
    // println "Exception "+e
  }
  sql.execute("create table irc_chatfile( id BIGINT(20) NOT NULL AUTO_INCREMENT, filename varchar(255), canonicalPath varchar(255), checksum varchar(255), PRIMARY KEY (id)) ENGINE=MyISAM") 
  sql.execute("create index irc_chatfile_checksum_idx on irc_chatfile(checksum)")
  sql.execute("create table irc_chatline( id BIGINT(20) NOT NULL AUTO_INCREMENT, line varchar(4096), PRIMARY KEY (id)) ENGINE=MyISAM") 
  sql.execute("create table irc_chatfile_chatline( chatfile_id MEDIUMINT, chatline_id MEDIUMINT) ENGINE=MyISAM")
*/
def directory = args[0] 

println " checking $directory.." 

new File(directory).eachFile { file ->
  if (file.isFile() && file.name.endsWith(".log")) {
   def rows = sql.rows("select * from irc_chatfile where checksum = '"+getSHAsum(file)+"'")
   if(rows.size() == 0) {
      def chatfile_keys = sql.executeInsert("insert into irc_chatfile(filename,canonicalPath,checksum) VALUES (?,?,?)", [file.name as String, file.canonicalPath as String, getSHAsum(file)])
      def chatfile_id = chatfile_keys[0][0]
      println "[$chatfile_id] "+getSHAsum(file)+" $file.canonicalPath [$rows]"
      FileUtils.readLines(file).each { line ->
        def chatline_keys = sql.executeInsert("insert into irc_chatline(line) VALUES (?)", [line])
        def chatline_id = chatline_keys[0][0]
        def chat_rel = sql.executeInsert("insert into irc_chatfile_chatline(chatfile_id,chatline_id) VALUES (?,?)", [chatfile_id,chatline_id])
      }
    } else { 
      println getSHAsum(file)+" $file.canonicalPath already inserted by checksum.."
    }     
  }
}

 
// sql.eachRow("select * from chatline", { println it.id + " -- ${it.line} --"} );
