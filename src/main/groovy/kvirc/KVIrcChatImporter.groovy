package kvirc

import groovy.sql.Sql
import org.apache.commons.io.FileUtils
import java.text.SimpleDateFormat

import org.apache.fop.util.text.AdvancedMessageFormat;
import org.apache.fop.util.text.AdvancedMessageFormat.Part;
import org.apache.fop.util.text.AdvancedMessageFormat.PartFactory;

public class KVIrcChatImporter { 

  static void main(String[] args) { 
    def importer = new KVIrcChatImporter('src/main/resources/kvirc/query/')
  }

  public static String getSHAsum(File file) {
      def shasum = java.security.MessageDigest.getInstance("SHA-256").digest(file.bytes);
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < shasum.length; i++) {
          sb.append(Integer.toString((shasum[i] & 0xff) + 0x100, 16).substring(1));
      }
      return sb.toString()
  }

  def sdf_session = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy")
  def sdf_simple = new SimpleDateFormat("EE dd-MM-y HH:mm", new Locale("NL", "nl"))
  def sdf_time = new SimpleDateFormat("HH:mm:ss")
  def sdf_line = new SimpleDateFormat("dd-MM-yyyy HH:mm")
 
  def entity = Sql.newInstance("jdbc:mysql://localhost:3306/entity_extractor_kvirc", "grails", "grails", "com.mysql.jdbc.Driver")
  def chatsour_query = "insert into source(version, sourcetype) values(0,'KVIRC')"
  def chatfile_query = "insert into source_irc_file(id, channel,checksum,filename,fullname) values(?,?,?,?,?)"
  def chatline_query = "insert into source_irc_line(id, state, date, linenumber, line, source_irc_file_id, chatlines_idx) values(?,?,?,?,?,?,?)"

  Set shasums = new HashSet()

  def KVIrcChatImporter(directory) { 

    println "starting import from: $directory"
    def insert = false

    def pattern = /log$/
    def files = []
    new File(directory).eachFileRecurse { file ->
      def shasum = getSHAsum(file)
      if (file =~ pattern && !shasums.contains(shasum)) {
        shasums.add(shasum) 
        files << file
      }
    }

    int filenr = 1
    files.each { file ->
      if (file.isFile() && file.name.endsWith(".log") && file.size()>0) {
        if(insert) { 
          def chatfile_id = entity.executeInsert(chatsour_query, [])[0][0]
          entity.executeInsert(chatfile_query, [chatfile_id, (file.filename.startsWith("channel_")?1:0), getSHAsum(file), file.filename, file.canonicalPath])
        }

        def patfile = new File("src/main/resources/pv_chatlogs.lyx").text
        AdvancedMessageFormat format = new AdvancedMessageFormat(patfile.replace("\\","\\\\"));
        Map params = new java.util.HashMap()
        params.put("pvnr", "29-783260/${filenr}")
        params.put("title", file.name)
        params.put("tijdstip_verbaal", new SimpleDateFormat("HH:mm:ss dd-MM-yyyy", new Locale("NL", "nl")).format(new Date()))
        params.put("bestandsnaam", file.name)
        params.put("onderwerp", "Tor Chatlogs")
  
        StringBuffer b = new StringBuffer()
        int index = 1

        def calendar = Calendar.getInstance()
        def date = null 

        def other_username 
        file.text.replace("\r!n\r","").replace("\r!nc\r","").replace("\r!h\r","").replace("\r!c\r","").replace("\u0002", "").replace("\r","").eachLine { original_line -> 
          def line = original_line.trim()

          if(line.size() > 0 && !line.equals("")) { 

          int state = line.substring(0, line.indexOf(" ")) as int
          line = line.substring(line.indexOf(" ")+1)

          def line_pv = line

          if(state == 0 && line.indexOf("Log session started at") != -1) { 
            def c = line.substring(27, line.lastIndexOf(" ###"))
            calendar.setTime(sdf_session.parse(c))
            if(date == null) { 
              date = calendar.time
            }
          } 
          if(state == 0 && line.indexOf("Log session terminated at") != -1) {
            def c = line.substring(30, line.lastIndexOf(" ###"))
            calendar.setTime(sdf_session.parse(c))
          }

          if (line.startsWith("[")) {
            def time = new GregorianCalendar(new Locale("NL", "nl"))
            def sTime = line.substring(1, line.indexOf("]"))
            time.setTime(sdf_time.parse(sTime))
            if(calendar != null) {
              calendar.set(Calendar.HOUR, time.get(Calendar.HOUR))
              calendar.set(Calendar.MINUTE, time.get(Calendar.MINUTE))
              calendar.set(Calendar.SECOND, time.get(Calendar.SECOND))
            }
            line = line.substring(line.indexOf("] ")+2)
          }

          def username
          if(state == 24 || state == 25 ||  state == 26) {
            if(line.startsWith("<")) { 
              line = line.substring(1)
              username = line.substring(0, line.indexOf(">"))

              if(username.startsWith("+") || username.startsWith("@")) {
                username = username.substring(1)
              }
              if(username!="biped") {
                other_username = username
              }
              line = line.substring(line.indexOf(">")+2)

b.append(
"""\\begin_layout Gesprek
\\begin_inset Flex GesprekRegelWie
status collapsed
\\begin_layout Plain Layout
${username}
\\end_layout
\\end_inset
\\begin_inset Flex GesprekRegelWanneer
status collapsed
\\begin_layout Plain Layout
""" + sdf_line.format(calendar.time) + """
\\end_layout
\\end_inset
\\begin_inset Flex GesprekRegelWat
status collapsed
\\begin_layout Plain Layout
${line}
\\end_layout
\\end_inset
\\end_layout
\\end_layout

""")
            }
            line_pv = String.format("[%s] [%s] %s (%s) (%s) %s",file.name,index-1,calendar.time,state,username,line)
          } else { 
            line_pv = String.format("[%s] [%s] %s (%s) %s",file.name,index-1,calendar.time,state,line)
          }

          if(insert) { 
            def chatline_id = entity.executeInsert(chatsour_query, [])[0][0]
            entity.executeInsert(chatline_query, [chatline_id, state, calendar.time, index, original_line, chatfile_id, (index as int) - 1])
            def chat_rel = sql.executeInsert("insert into irc_chatfile_chatline(chatfile_id,chatline_id) VALUES (?,?)", [chatfile_id, chatline_id])
          }
        }
        index++
        }
        params.put("nickname",other_username);
        params.put("nickname_mickelsons", "biped");
println "[$file.name] date: $date"
        params.put("tijdstip_chat", new SimpleDateFormat("HH:mm:ss dd-MM-yyyy 'GMT' Z", new Locale("NL", "nl")).format(date));
        params.put("chattekst", b.toString());
        def pv = format.format(params);

        new File("out/$file.name" + ".lex").write pv
      }
      filenr++
    }
  }
} 
