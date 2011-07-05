package entity_extractor

import groovy.sql.Sql
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xbill.DNS.*

class DatabaseIsNowKnown {
  Logger log = LoggerFactory.getLogger(DatabaseIsNowKnown.class)

  def chatsDB  = Sql.newInstance("jdbc:mysql://localhost:3306/chats", "grails", "grails", "com.mysql.jdbc.Driver")
  def entityDB = Sql.newInstance("jdbc:mysql://localhost:3306/entity_extractor_test", "grails", "grails", "com.mysql.jdbc.Driver")

  def q_entity      		= "insert into entity(version) values(0)"
  def q_entity_souce    	= "insert into mm_source_entities(mm_source_id, mm_entity_id) values(?,?)"
  def q_entity_country 		= "insert into entity_country(id,code,continent,full_name,iso3,name,number) values(?,?,?,?,?,?,?)"
  def q_entity_host     	= "insert into entity_host(id,geolocation_id,hostname,ipaddress,ipnumber,tor_proxy) values(?,?,?,?,?,?)"
  def q_entity_geolocation     	= "insert into entity_geolocation(id, country_code, hostname, ipaddress, latitude, locality, longitude, postal, region_code, x, y) values(?,?,?,?,?,?,?,?,?,?,?)"

  def entities  		        = entityDB.dataSet("entity")
  def entity_host     		    = entityDB.dataSet("entity_host")
  def entity_geolocation 	    = entityDB.dataSet("entity_geolocation")
  def entity_country		    = entityDB.dataSet("entity_country")
  def entity_nick		        = entityDB.dataSet("entity_nick")
  def entity_status		        = entityDB.dataSet("entity_status")
  def entity_message		    = entityDB.dataSet("entity_message")
  def mm_source_entities	    = entityDB.dataSet("mm_source_entities")

  def entity_chat_nick 		    = entityDB.dataSet("entity_chat_nick")

  def entity_chat_status        = entityDB.dataSet("entity_chat_status")
  def entity_chat_irc_status    = entityDB.dataSet("entity_chat_irc_status") // entity_chat_irc_status || entity_chat_irc_user_status
  def entity_chat_user_status   = entityDB.dataSet("entity_chat_irc_user_status")
  def entity_chat_channel_status= entityDB.dataSet("entity_chat_irc_channel_status")

  def entity_chat_message	    = entityDB.dataSet("entity_chat_message")
  def entity_chat_user_switch   = entityDB.dataSet("entity_chat_irc_user_status_switch")

  def entity_chat_user_message  = entityDB.dataSet("entity_chat_irc_user_message")

  def sources          		    = entityDB.dataSet("sources")
  def source_irc_files 		    = entityDB.dataSet("source_irc_file")
  def source_irc_lines 		    = entityDB.dataSet("source_irc_line")

  def sdf_session = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy")
  def sdf_simple = new SimpleDateFormat("EE dd-MM-y HH:mm", new Locale("NL", "nl"))
  def sdf_time = new SimpleDateFormat("HH:mm")

  // relateChatChannelStatus(chatline, line)
  // addChatUserSwitchLine(chatline, line, from, to)

  // chatstatus
  def getChatChannelStatusById(id) {
    return entity_chat_channel_status.firstRow("select * from entity_chat_irc_channel_status where id=$id")
  }
  def insertChatChannelStatus(line) {
    def message = insertChatStatus(line)
    entity_chat_channel_status.add(id:message.id)
    return getChatChannelStatusById(message.id)
  }
  def relateChatChannelStatus(source_id, line) {
    assert line != null
    relateSourceEntity(source_id, insertChatChannelStatus(line).id)
  }

  // chatstatus
  def getChatUserSwitchById(id) {
    return entity_chat_user_status.firstRow("select * from entity_chat_irc_user_status where id=$id")
  }
  def insertChatUserSwitch(from_user, to_user, line) {
    assert line != null
    def message = insertChatStatus(line)
    entity_chat_user_switch.add(id:message.id, from_id: from_user.id, to_id=to_user.id)
    return getChatUserSwitchById(message.id)
  }
  def relateChatUserSwitch(source_id, line, fromname, toname) {
    assert fromname != null
    assert toname   != null

    def from_user   = relateChatNick(source_id, fromname)
    def to_user     = relateChatNick(source_id, toname)

    relateSourceEntity(source_id, insertChatUserSwitch(from_user, to_user, line).id)
  }


  // chatstatus
  def getChatUserStatusById(id) {
    return entity_chat_user_status.firstRow("select * from entity_chat_irc_user_status where id=$id")
  }
  def insertChatUserStatus(username, line) {
    assert username != null
    assert line != null

    def nick = insertChatNick(username)
    def message = insertChatMessage(line)
    entity_chat_user_status.add(id:message.id, nick_id: nick.id)
    return getChatUserStatusById(message.id)
  }
  def relateChatUserStatus(source_id, line, username) {
    assert username != null

    relateChatNick(source_id, username)
    relateSourceEntity(source_id, insertChatUserStatus(username, line).id)
  }

  // chatmessage
  def getChatUserMessageById(id) {
    return entity_chat_user_message.firstRow("select * from entity_chat_irc_user_message where id=$id")
  }
  def insertChatUserMessage(username, line) {
    def nick = insertChatNick(username)
    def message = insertChatMessage(line)
    entity_chat_user_message.add(id:message.id, nick_id: nick.id)
    return getChatUserMessageById(message.id)
  }
  def relateChatUserMessage(source_id, line, username) {
    assert username != null
    relateSourceEntity(source_id, insertChatUserMessage(username, line).id)
  }

  // chatmessage
  def getChatMessageById(id) {
    return entity_chat_message.firstRow("select * from entity_chat_message where id=$id")
  }
  def insertChatMessage(line) {
    def message = insertMessage(line)
    entity_chat_message.add(id:message.id)
    return getChatMessageById(message.id)
  }
  def relateChatMessage(source_id, line) {
    relateSourceEntity(source_id, insertChatMessage(line).id)
  }

  // chatstatus
  def getChatStatusById(id) {
    return entity_chat_status.firstRow("select * from entity_chat_status where id=$id")
  }
  def insertChatStatus(line) {
    def status = insertStatus(line)
    entity_chat_status.add(id:status.id)
    return getChatStatusById(status.id)
  }
  def relateChatStatus(source_id, line) {
    relateSourceEntity(source_id, insertChatStatus(line).id)
  }

  // message
  def getMessageById(id) {
    return entity_status.firstRow("select * from entity_message where id=$id")
  }
  def insertMessage(line) {
    def id = createNewEntity()
    entity_message.add(id:id, line:line.trim())
    return getMessageById(id)
  }
  def relateMessage(source_id, line) {
    relateSourceEntity(source_id, insertMessage(line).id)
  }

  // status
  def getStatusById(id) {
    return entity_status.firstRow("select * from entity_status where id=$id")
  }
  def insertStatus(line) {
    def id = createNewEntity()
    entity_status.add(id:id, line:line)
    return getStatusById(id)
  }
  def relateStatus(source_id, line) {
    relateSourceEntity(source_id, insertStatus(line).id)
  }


  // chatnick
  def getChatNickById(id) {
    entity_chat_nick.firstRow("select * from entity_chat_nick where id=$id")
  }
  def getChatNickByName(name) {
    return getChatNickById(getNickByName(name)?.id)
  }
  def insertChatNick(name) {
    def entity = getChatNickByName(name)
    if(!entity) {
      entity = insertNick(name)
      entity_chat_nick.add(id:entity.id)
    }
    return entity
  }
  def relateChatNick(source_id, name) {
    assert name != null

    def entity = insertChatNick(name)
    relateSourceEntity(source_id, entity.id)
    return entity
  }

  // nick
  def getNickById(id) {
    entity_nick.firstRow("select * from entity_nick where id=$id")
  }
  def getNickByName(name) {
    return entity_nick.firstRow("select * from entity_nick where name=$name")
  }
  def insertNick(name) {
    assert name != null

    if (name.startsWith("~")) {
        name = name.substring(1)
    }
    if (name.startsWith("+")) {
        name = name.substring(1)
    }
    if (name.startsWith("@")) {
        name = name.substring(1)
    }

    def entity = getNickByName(name)
    if(!entity) {
      def id = createNewEntity()
      entity_nick.add(id:id, name:name)
      entity = getNickById(id)
    }
    return entity
  }
  def relateNick(source_id, name) {
    assert name != null

    def entity = insertNick(name)
    relateSourceEntity(source_id, entity.id)
  }


  // geolocation

  def geolookup(hostname) {
    // def command = """geoiplookup -f /opt/local/var/macports/software/GeoLiteCity/20110201_0/opt/local/share/GeoIP/GeoIPCity.dat $hostname"""// Create the String
    def command = """geoiplookup $hostname"""// Create the String
    def proc = command.execute()
    proc.waitFor()
    def output = proc.in.text

    def location = null
    output.split("\n").each { line ->

      if(!line.contains("can't resolve hostname") && !line.contains("IP Address not found")) {
        if (line.startsWith("GeoIP Country Edition: ")) {
          line = line.substring("GeoIP Country Edition: ".size())
          if(!location) {
            location = new Expando()
          }
          line.split(",").eachWithIndex { word, index ->
            word = word.trim()
            switch(index) {
              case 0: location.country_code = word; break
              case 1: location.country      = word; break
            }
          }
            if(location.country_code.contains("GeoIP Country Edition: ")) {
            location.country_code = location.country_code.substring("GeoIP Country Edition: ".size())
          }
        } else if (line.startsWith("GeoIP City Edition, Rev 1: ")) {
          line = line.substring("GeoIP City Edition, Rev 1: ".size())
          if(!location) {
            location = new Expando()
          }
          line.split(",").eachWithIndex { word, index ->
            word = word.trim()
            switch (index) {
              case 0: location.country_code = word; break
              case 1: location.region_code = word; break
              case 2: location.locality = word; break
              case 3: location.postal = word; break
              case 4: location.latitude = word; break
              case 5: location.longitude = word; break
              case 6: location.x = word; break
                case 7: location.y = word; break
            }
          }
          if(location.country_code.contains("GeoIP Country Edition: ")) {
            location.country_code = location.country_code.substring("GeoIP Country Edition: ".size())
          }
        } else if (line.startsWith("GeoIP City Edition, Rev 0: ")) {
          line = line.substring("GeoIP City Edition, Rev 0: ".size())
          if(!location) {
            location = new Expando()
          }
          line.split(",").eachWithIndex { word, index ->
            word = word.trim()
            switch (index) {
              case 0: location.country_code = word; break
              case 1: location.region_code = word; break
              case 2: location.locality = word; break
              case 3: location.postal = word; break
              case 4: location.latitude = word; break
              case 5: location.longitude = word; break
            }
          }
          if(location.country_code.contains("GeoIP Country Edition: ")) {
            location.country_code = location.country_code.substring("GeoIP Country Edition: ".size())
          }
        }
      }
    }
    return location
  }

  def getGeolocationById(id) {
    return entity_geolocation.firstRow("select * from entity_geolocation where id = $id")
  }
  def getGeolocation(hostname) {
    return entity_geolocation.firstRow("select * from entity_geolocation where hostname = $hostname")
  }
  def insertGeolocation(hostname, ipaddress) {
    def geolocation = getGeolocation(hostname)
    if (!geolocation) {
      def location = geolookup(ipaddress)
      if(!location) {
        location = geolookup(hostname)
      }
      if(location) {
        def geolocation_id = createNewEntity()
        entity_geolocation.add ( id: geolocation_id, hostname: hostname, ipaddress: ipaddress, country_code: location.country_code, country: location.country, region_code: (location.region_code?location.region_code:"N/A"), locality: location.locality, postal: location.postal , latitude: location.latitude, longitude: location.longitude, x: location.x, y:location.y)
        geolocation = getGeolocationById(geolocation_id)
      }
    } else {
      log.info "insertGeolocation($hostname, $ipaddress): already found geo($geolocation)"
    }
    return geolocation
  }

  // host
  public static int ipToInt(final String addr) {
    String[] quads = addr.split("\\.");
    long ipnum = (long) 16777216 * Long.parseLong(quads[0])
     +(long) 65536 * Long.parseLong(quads[1])
     +(long) 256 * Long.parseLong(quads[2])
     +(long) 1 * Long.parseLong(quads[3]);
     return ipnum;
  }
  def String reverseDns(hostIp) {
    String result = hostIp
    try {
        def name = ReverseMap.fromAddress(hostIp)
        def rec = Record.newRecord(name, Type.PTR, DClass.IN)
        def query = Message.newQuery(rec)
        def response = new org.xbill.DNS.ExtendedResolver().send(query)
        def answers = response.getSectionArray(Section.ANSWER)
        if (answers) result = answers[0].rdataToString() else result = hostIp
    } catch (java.net.SocketTimeoutException e) {
        log.error "While transforming hostname: " + e
    }
    return result
  }

  def getHostById(id) {
    entity_host.firstRow("select * from entity_host where id=$id")
  }
  def getHostByHostname(name) {
    return entity_host.firstRow("select * from entity_host where hostname = $name")
  }
  def getHostByHostnameOrIp(name) {
    return entity_host.firstRow("select * from entity_host where hostname = $name or ipaddress = $name")
  }
  def insertHost(hostname) {
    def host = getHostByHostnameOrIp(hostname)
    if(!host) {
      def ipaddress = null
      try {
          def IP_PATTERN = Pattern.compile("(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[0-9])")
          def matcher = hostname =~ IP_PATTERN
          if (matcher.matches()) {
              def reverse = reverseDns(hostname)
              ipaddress = InetAddress.getByName(hostname).hostAddress
              hostname = reverse
          } else {
              ipaddress = InetAddress.getByName(hostname).hostAddress
          }
      } catch (java.net.UnknownHostException e) {
          // unknownHosts.add(hostname)
      }
      if (hostname.endsWith(".")) {
          hostname = hostname.substring(0, hostname.length() - 1)
      }

      def geolocation
      int i = 0
      boolean breakout = false
      while (ipaddress && ipaddress != "0.0.0.0" && !breakout && i < 5 && geolocation == null) {
          try {
              geolocation = insertGeolocation(hostname, ipaddress)
              breakout = true
          } catch (Exception e) {
              log.error(" getHost[$hostname/$ipaddress] Exception in try($i): " + e)
e.printStackTrace()
              geolocation = null
          }
          i++
      }

      def host_id = createNewEntity()
      entity_host.add(id:host_id, hostname:hostname,ipaddress: ipaddress, ipnumber:(ipaddress?ipToInt(ipaddress.toString()):0),geolocation_id:(geolocation?geolocation.id:null), tor_proxy: false)
      host = getHostById(host_id)
    }
    return host
  }
  def relateHost(source_id, hostname) {
    def host = insertHost(hostname)
    relateSourceEntity(source_id, host.id)
  }

  // country
  def getCountryById(id) {
    entity_country.firstRow("select * from entity_country where id=$id")
  }
  def getCountryByFullName(countryname) {
    return entity_country.firstRow("select * from entity_country where full_name = $countryname")
  }
  def insertCountry(countryname) {
    def country = getCountryByFullName(countryname)
    if(!country) {
      def country_id = createNewEntity()
      entity_country.add(id:country_id, full_name:countryname ,number:0 )
      country = getCountryById(country_id)
    }
    return country
  }
  def relateCountry(source_id, countryname) {
    def country = insertCountry(countryname)
    relateSourceEntity(source_id, country.id)
  }

  // entity
  def createNewEntity() {
    return entityDB.executeInsert(q_entity, [])[0][0]
  }
  def insertEntity(source_id) {
    def entity_id = createNewEntity()
    relateSourceEntity(source_id, entity_id)
    return entity_id
  }
  def relateSourceEntity(source_id, entity_id) {
    if(!mm_source_entities.firstRow("select * from mm_source_entities where mm_source_id=$source_id and mm_entity_id=$entity_id")) {
      mm_source_entities.add( mm_source_id: source_id, mm_entity_id: entity_id)
    } else {
      // nick promoted naar chatnick
    }
  }

  // helpers
  def extractUserAliasHost(chatline, l) {
    def username = l.substring(0, l.indexOf(" "))
    l = l.substring(l.indexOf("(") + 1)
    def user = relateChatNick(chatline, username)
    def hosts = extractAliasHost(chatline, l)
    return [username: username, user: user] + hosts
  }

  def extractAliasHost(chatline, l) {
    def aliasname = l.substring(0, l.indexOf("@"))
    if (aliasname.startsWith("~")) {
    aliasname = aliasname.substring(1)
    }
    if (aliasname.startsWith("+")) {
        aliasname = aliasname.substring(1)
    }
    if (aliasname.startsWith("@")) {
        aliasname = aliasname.substring(1)
    }
    def alias = relateNick(chatline, aliasname)

    def hostname = null
    def extra = null
    l = l.substring(l.indexOf("@") + 1)

    if (l.contains(" ")) {
        hostname = l.substring(0, l.indexOf(" "))
        l = l.substring(l.indexOf(" ") + 1).trim()
        if (l.length() > 0) {
            if (l.startsWith("* ")) {
                l = l.substring("* ".length())
            }
            extra = l

        }
    } else if (l.indexOf(")") != -1) {
        hostname = l.substring(0, l.indexOf(")"))
        l = l.substring(l.indexOf(")") + 1).trim()
        if (l.length() > 0) {
            if (l.startsWith(" * ")) {
                l = l.substring(" * ".length())
            }
            extra = l
        }

    } else {
        hostname = l
    }
    if (hostname.endsWith(")")) {
        hostname = hostname.substring(0, hostname.length() - 1)
    }
    def host = relateHost(chatline, hostname)
    return [aliasname: aliasname, alias: alias, hostname: hostname, host: host, extra: extra]
  }

  def extractEntities(chatfile, chatline) {
    def line = chatline.line

    if (line.equals("-")) {

    } else if (line.startsWith("Session ")) {
        if (line.startsWith("Session Start: ")) {
            relateChatChannelStatus(chatline.id, line)
        } else if (line.startsWith("Session Time: ")) {
            relateChatChannelStatus(chatline.id, line)
        } else if (line.startsWith("Session Close: ")) {
            relateChatChannelStatus(chatline.id, line)
        } else if (line.startsWith("Session Ident: ")) {
            relateChatChannelStatus(chatline.id, line)
        } else {
            relateChatChannelStatus(chatline.id, line)
            log.error "Session failure: " + line + " " + " unkown"
        }
    } else if (line.startsWith("[")) {
        def time = new GregorianCalendar(new Locale("NL", "nl"))
        line = line.substring(line.indexOf("] ") + 2)

        if (line.startsWith("Session ")) {
            if (line.startsWith("Session Ident: ")) {
                // Session Ident: babyluv (~babylove@gna.phonk.net)
                def l = line.substring("Session Ident: ".length())
                def result = extractUserAliasHost(chatline.id, l)
                relateChatUserStatus(chatline.id, line, result.username)

            } else {
                log.error "$chatline: Unknown status"
            }
        } else if (line.startsWith("User ")) {
            def l = line.substring("User ".length())
            def username = l.substring(0, l.indexOf(" "))
            l = l.substring(l.indexOf(" ") + 1)
            if (l.indexOf(" is from ") != -1) {
                l = l.substring(l.indexOf(" is from ") + " is from ".length())
                def countryname = l
                def country = relateCountry(chatline.id, countryname)
                relateChatUserStatus(chatline.id, line, username)

            } else {
                // log.info chatline + " UNKNOWN"
                def user = relateChatNick(chatline.id, username)
                relateChatChannelStatus(chatline.id, line)

            }
        } else if (line.startsWith("* ")) {
            def l = line.substring("* ".length())
            if (l.startsWith("Joins: ") || l.startsWith("Parts: ") || l.startsWith("Quits: ")) {
                l = l.substring(l.indexOf(" ") + 1)
                def result = extractUserAliasHost(chatline.id, l)
                relateChatUserStatus(chatline.id, line, result.username)

            } else if (l.indexOf(" is now known as ") != -1) {
                def fromname = l.substring(0, l.indexOf(" "))
                def from = relateChatNick(chatline.id, fromname)
                l = l.substring(l.indexOf(" ") + 1)
                def toname = l.substring("is now known as ".length())
                def to = relateChatNick(chatline.id, toname)
                // from.addToAliases(to)
                // addChatUserSwitchLine(chatline.id, line, from, to)
                relateChatUserSwitch(chatline.id, line, fromname, toname)

            } else if (l.startsWith("Now talking in ")) {
                relateChatChannelStatus(chatline.id, line)
            } else if (l.startsWith("You were kicked by ")) {
                // You were kicked by Branwen-` (TodderLuv)
                l = l.substring("You were kicked by ".length())
                def username = l.substring(0, l.indexOf(" "))
                // def user = relateChatNick(chatline.id, username)
                l = l.substring(l.indexOf(" (") + 2)
                relateChatUserStatus(chatline.id, line, username)

            } else if (l.startsWith("Attempting to rejoin channel ")) {
                //  Attempting to rejoin channel #!DaringDarlings
                relateChatChannelStatus(chatline.id, line)

            } else if (l.startsWith("Rejoined channel ")) {
                // * Rejoined channel #!DaringDarlings
                relateChatChannelStatus(chatline.id, line)

            } else if (l.indexOf("sets mode: ") != -1) {
                def username = l.substring(0, l.indexOf(" "))
                // def user = relateChatNick(chatline.id, username)
                relateChatUserStatus(chatline.id, line, username)

            } else {
                // rest
                if (l.contains(" ") && !l.startsWith("Unable to connect to server") && !l.startsWith("Connect cancelled") && !l.startsWith("Unable to connect to server") && !l.endsWith("EntityHost disconnected") && !l.startsWith("Connecting to ")) {
                    def username = l.substring(0, l.indexOf(" "))
                    // def user = relateChatNick(chatline.id, username)
                    relateChatUserStatus(chatline.id, line, username)

                } else {
                    relateChatChannelStatus(chatline.id, line)
                }
            }
        } else if (line.startsWith("<")) {
            def username = line.substring(1, line.indexOf(">"))
            // def user = relateChatNick(chatline.id, username)
            line = line.substring(line.indexOf(">")).trim()
            relateChatUserMessage(chatline.id, line, username)

        } else if (line.startsWith("d[")) {
            def username = line.substring(2, line.indexOf("]"))
            // def user = relateChatNick(chatline.id, username)
            line = line.substring(line.indexOf("]")).trim()
            relateChatUserMessage(chatline.id, line, username)

        } else if (line.startsWith("e[")) {
            def username = line.substring(2, line.indexOf("]"))
            // def user = relateChatNick(chatline.id, username)
            line = line.substring(line.indexOf("]")).trim()
            relateChatUserMessage(chatline.id, line, username)

        } else {
            if (line.contains(" ")) {
                def l = line.substring(line.indexOf(" ") + 1)
                if (l.startsWith("is ") && !l.startsWith("is away:") && !l.startsWith("is now your hidden host") && !l.startsWith("is logged in as ")) {
                    def username = line.substring(0, line.indexOf(" "))
                    l = l.substring(3)
                    if (line.contains("@")) {
                        def result = extractAliasHost(chatline.id, l)
                        relateChatUserStatus(chatline.id, line, username)
                        if (result.extra != null) {
                            // log.fatal " Extracted user? "+result.extra
                            def nick = relateNick(chatline.id, result.extra)
                        }
                    } else {
                         def user = relateChatNick(chatline.id, username)
                        relateChatChannelStatus(chatline.id, line)
                    }

                } else {
                    relateChatChannelStatus(chatline.id, line)

                }
            } else {
                relateChatChannelStatus(chatline.id, line)
            }
        }

    } else {
        relateChatChannelStatus(chatline.id, line)
    }
  }
  // database
  def DatabaseIsNowKnown() {
    def files = source_irc_files.rows().each { file ->
      source_irc_lines.eachRow("select * from source_irc_line where source_irc_file_id=$file.id and line like '%is now known as%'") { line ->
        log.info "[$line.id] {$line.linenumber} $line.line"
	    extractEntities(file, line)
      }
    }
  }

  public static void main(String[] args) { 
    def database = new DatabaseIsNowKnown()
  }
}
