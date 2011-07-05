@Grapes([
  @Grab('mysql:mysql-connector-java:5.1.13'),
  @Grab('commons-codec:commons-codec:1.4'),
  @Grab('commons-io:commons-io')
])

import groovy.sql.Sql
import org.apache.commons.io.FileUtils

def sql = Sql.newInstance("jdbc:mysql://localhost:3306/mirc", "grails", "grails", "com.mysql.jdbc.Driver")

// sql.execute("create table chatlines( id BIGINT(20) NOT NULL AUTO_INCREMENT, canonicalPath varchar(255), checksum varchar(255), timestamp TIMESTAMP NULL DEFAULT NULL, linenumber INT, line varchar(1024), PRIMARY KEY (id)) ENGINE=MyISAM") 
// create index chatlines_id_idx on chatlines(id);
// create index chatlines_timestamp_idx on chatlines(timestamp);
