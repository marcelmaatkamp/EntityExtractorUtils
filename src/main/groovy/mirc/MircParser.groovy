package mirc

import java.text.SimpleDateFormat
import groovy.sql.Sql
import org.apache.commons.io.FileUtils

// create table chatfiles( id BIGINT(20) NOT NULL AUTO_INCREMENT, canonicalPath varchar(255), checksum varchar(255), timestamp TIMESTAMP NULL DEFAULT NULL, PRIMARY KEY (id)) ENGINE=MyISAM;
// create index chatfiles_idx_id on chatfiles(id);
// create index chatfiles_idx_chk on chatfiles(checksum);

// create table chatlines( id BIGINT(20) NOT NULL AUTO_INCREMENT, chatfile_id BIGINT(2), timestamp TIMESTAMP NULL DEFAULT NULL, linenumber INT, username varchar(255), line varchar(1024), PRIMARY KEY (id)) ENGINE=MyISAM;
// create index chatlines_id_idx on chatlines(id);
// create index chatlines_timestamp_idx on chatlines(timestamp);

class MircParser { 

  static void main(args) { 
    def filename = "src/main/resources/#Lulzsec.hell.allyoursecretsbelongto.us.log"
    def parser = new MircParser().parse(filename)
  }

  def parse(filename) { 
    def sql = Sql.newInstance("jdbc:mysql://localhost:3306/mirc", "grails", "grails", "com.mysql.jdbc.Driver")

    Calendar calendar =  GregorianCalendar.getInstance()
    calendar.lenient = false

    def sdf_session = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy")
    def sdf_date = new SimpleDateFormat("MMM dd yyyy")
    def sdf_time = new SimpleDateFormat("HH:mm")

    Map<String, Set> aliases = new HashMap<String, Set>()

    def file = new File(filename)
    def checksum = getSHAsum(file)
    def file_id = sql.executeInsert("insert into chatfiles(canonicalPath, checksum, timestamp) VALUES (?,?,?)", [file.canonicalPath, checksum, new Date()])[0][0]

    file.eachLine { line, index ->

      int state = 0
      def username = ""

      if(line.startsWith("\u0003")) { 
        line = line.substring(1)
      }
      line = line.replace("\u000f", "")
  
      def line_orig = line

      if(line.size() == 0) {
        // println "#-#" 
      } else if(line.equals("-")) { 
        // println line
      } else if(line.startsWith("Session")) { 
        if (line.startsWith("Session Start: ")) {
          def l = line.substring("Session Start: ".length())
          def date = sdf_session.parse(l)
          calendar.setTime(date)
        } else if (line.startsWith("Session Time: ")) {
          def l = line.substring("Session Time: ".length())
          def date = sdf_session.parse(l)
          calendar.setTime(date)
        } else if (line.startsWith("Session Close: ")) {
          def l = line.substring("Session Close: ".length())
          def date = sdf_session.parse(l)
          calendar.setTime(date)
        }
      } else {  
        if(!line.startsWith("[")) {
          state = line.substring(0,2) as int 
          line = line.substring(2)
        }
         
        def time = new GregorianCalendar(new Locale("NL", "nl"))
        time.setTime(new SimpleDateFormat("HH:mm").parse(line.substring(1, line.indexOf("]"))))
        if(calendar != null) {
            calendar.set(Calendar.HOUR_OF_DAY, time.get(Calendar.HOUR_OF_DAY))
            calendar.set(Calendar.MINUTE, time.get(Calendar.MINUTE))
            calendar.set(Calendar.SECOND, 00)
        }
        line  = line.substring(line.indexOf("]")+2)
    
        if(line.startsWith("<")) {
          username = line.substring(1,line.indexOf(">"))
          line = line.substring(line.indexOf(">")+2)
        } else if(line.startsWith("* ")) { 
          line = line.substring(2)
          if(line.startsWith("Now talking in ") || line.startsWith("Set by") || line.startsWith("Topic is") || line.startsWith("Disconnected") ) { 
    
          } else { 
            username = line.substring(0, line.indexOf(" ")) 
            if(!aliases.containsKey(username)) { 
              Set names = new HashSet<String>()
              names.add(username)
              aliases.put(username, names)
            }
    
            line = line.substring(line.indexOf(" ")+1)
      
            if(line.indexOf("is now known as ") != -1) { 
              def alias = line.substring(line.indexOf("is now known as ") + "is now known as ".size())
              Set names = aliases.get(username)
                if(!names.contains(alias)) { 
                  names.add(alias)
                  aliases.remove(username)
                  aliases.put(alias, names)         
                }
              }
            }
          }
        }
        println index + "\t [$state] \t" + sdf_date.format(calendar.time) + "\t" + sdf_time.format(calendar.time) +"\t $username \t $line"
        def line_id = sql.executeInsert("insert into chatlines(chatfile_id,timestamp,linenumber,username,line) VALUES (?,?,?,?,?)", [file_id, calendar.time, index, username, line])[0][0]
    }
  }

  public static String getSHAsum(File file) {
    def shasum = java.security.MessageDigest.getInstance("SHA-256").digest(file.bytes);
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < shasum.length; i++) {
      sb.append(Integer.toString((shasum[i] & 0xff) + 0x100, 16).substring(1));
    }
    return sb.toString()
  }
}