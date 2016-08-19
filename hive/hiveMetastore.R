### Connect to Spark
library(sparklyr)
library(dplyr)
library(ggplot2)
Sys.setenv(SPARK_HOME="/usr/lib/spark")
config <- spark_config()
sc <- spark_connect(master = "yarn-client", config = config, version = '1.6.2')

### Load DBI
library(DBI)

### Browse the Hive Metastore
dbGetQuery(sc, "show databases")
dbGetQuery(sc, "show tables in default")
dbGetQuery(sc, "show tables in userdb")
dbGetQuery(sc, "describe userdb.students")

### Create a new database, a new table, and insert data
dbGetQuery(sc, "create database newdb")
dbGetQuery(sc, "drop table if exists newdb.pageviews")
dbGetQuery(sc, "create table newdb.pageviews (userid varchar(64), link string, came_from string)")
dbGetQuery(sc, "insert into table newdb.pageviews values ('jsmith', 'mail.com', 'sports.com'), ('jdoe', 'mail.com', null)")

### This query does not work from R but works from the command prompt
dbGetQuery(sc, "CREATE TABLE students (name VARCHAR(64), age INT, gpa DECIMAL(3, 2)) CLUSTERED BY (age) INTO 2 BUCKETS STORED AS ORC")

dbGetQuery(sc, "use newdb")
dbGetQuery(sc, "show tables in newdb")
