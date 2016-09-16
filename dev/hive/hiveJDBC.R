#loading libraries
library("DBI")
library("rJava")
library("RJDBC")

#init of the classpath (works with hadoop 2.6 on CDH 5.4 installation)
hivecp = c("/usr/lib/hive/lib/hive-jdbc.jar", "/usr/lib/hadoop/client/hadoop-common.jar", "/usr/lib/hive/lib/libthrift-0.9.2.jar", "/usr/lib/hive/lib/hive-service.jar", "/usr/lib/hive/lib/httpclient-4.2.5.jar", "/usr/lib/hive/lib/httpcore-4.2.5.jar", "/usr/lib/hive/lib/hive-jdbc-standalone.jar")
.jinit(classpath=cp)

#initialisation de la connexion
drv <- JDBC("org.apache.hive.jdbc.HiveDriver", "/usr/lib/hive/lib/hive-jdbc.jar", identifier.quote="`")
conn <- dbConnect(drv, "jdbc:hive2://localhost:10000/default", "myuser", "")

#working with the connexion
show_databases <- dbGetQuery(conn, "show databases")
show_databases

library("RJDBC")
options( java.parameters = "-Xmx8g" )
drv <- JDBC("org.apache.hive.jdbc.HiveDriver", "/usr/lib/hive/lib/hive-jdbc.jar")
conn <- dbConnect(drv, "jdbc:hive2://localhost:10000/default", "rstudio-user", "")
sample_08 <- dbReadTable(conn, "airlines")


jdbc:sqlserver://data.rsquaredltd.com\SandP
jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]

install unixODBC unixODBC-devel
