### Build EMR master node with Taxi Data
### Nathan Stephens
### 3/27/2017

###########################################
### Run as root
###########################################

## RSP

# Upate
sudo yum update

# R
sudo yum install -y R libcurl-devel openssl-devel git
  
# install RSP
wget -q https://download2.rstudio.org/current.ver -O /tmp/rsp.current.ver
wget -O /tmp/rstudio-server-rhel.rpm https://s3.amazonaws.com/rstudio-dailybuilds/rstudio-server-rhel-pro-$(cat /tmp/rsp.current.ver)-x86_64.rpm
sudo yum install -y --nogpgcheck /tmp/rstudio-server-rhel.rpm

# install packages
sudo Rscript -e 'install.packages("sparklyr",  repos = "http://cran.rstudio.com/")'
sudo Rscript -e 'install.packages("devtools",  repos = "http://cran.rstudio.com/")'
sudo Rscript -e 'install.packages("tidyverse", repos = "http://cran.rstudio.com/")'
sudo Rscript -e 'install.packages("leaflet",   repos = "http://cran.rstudio.com/")'
sudo Rscript -e 'install.packages("DT",        repos = "http://cran.rstudio.com/")'

###########################################

## Add rstudio user
sudo useradd -m rstudio
sudo echo rstudio | passwd rstudio --stdin
sudo usermod -a -G hadoop rstudio
sudo usermod -a -G hive rstudio


###########################################
### Run as rstudio
###########################################

## switch user
su rstudio
cd ~

## add rstudio directory
hadoop fs -mkdir /user/rstudio
hadoop fs -chown rstudio:rstudio /user/rstudio

## clone project
git clone https://github.com/rstudio/sparkDemos.git /home/rstudio/sparkDemos
cat >/home/rstudio/sparkDemos/sparkDemos.Rproj <<ENDOFCONTENT
Version: 1.0

RestoreWorkspace: Default
SaveWorkspace: Default
AlwaysSaveHistory: Default

EnableCodeIndexing: Yes
UseSpacesForTab: Yes
NumSpacesForTab: 2
Encoding: UTF-8

RnwWeave: Sweave
LaTeX: pdfLaTeX
ENDOFCONTENT

## Copy data

nohup /usr/bin/s3-dist-cp --src=s3n://rstudio-sparkdemo-data/nyc-taxi/csv/nyct2010 --dest=hdfs:///user/rstudio/nyct2010 >> nyct2010.log &
nohup /usr/bin/s3-dist-cp --src=s3n://rstudio-sparkdemo-data/nyc-taxi/parquet_nohead/trips --dest=hdfs:///user/rstudio/trips_par >> trips_par.log &
nohup /usr/bin/s3-dist-cp --src=s3n://rstudio-sparkdemo-data/nyc-taxi/parquet/trips_model_data --dest=hdfs:///user/rstudio/trips_model_data >> trips_model_data.log &


###########################################
### Open Hive
###########################################

hive

# Hive 1

CREATE EXTERNAL TABLE IF NOT EXISTS nyct2010(
gid int,
ctlabel float,
borocode int,
boroname string,
ct2010 int,
boroct2010 int,
cdeligibil string,
ntacode string,
ntaname string,
puma int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
;

LOAD DATA INPATH '/user/rstudio/nyct2010' INTO TABLE nyct2010;

# Hive 3

CREATE EXTERNAL TABLE IF NOT EXISTS trips_par(
id int,
cab_type_id int,
vendor_id string,
pickup_datetime timestamp,
dropoff_datetime timestamp,
store_and_fwd_flag string,
rate_code_id string,
pickup_longitude float,
pickup_latitude float,
dropoff_longitude float,
dropoff_latitude float,
passenger_count bigint,
trip_distance float,
fare_amount float,
extra bigint,
mta_tax string,
tip_amount float,
tolls_amount float,
ehail_fee string,
improvement_surcharge string,
total_amount float,
payment_type string,
trip_type string,
pickup_nyct2010_gid int,
dropoff_nyct2010_gid int)
stored as parquet;

LOAD DATA INPATH '/user/rstudio/trips_par' INTO TABLE trips_par;


# Hive 3
CREATE EXTERNAL TABLE IF NOT EXISTS trips_model_data(
pickup_datetime timestamp,
pickup_latitude float,
pickup_longitude float,
pickup_nyct2010_gid int,
pickup_boro string,
pickup_nta string,
dropoff_datetime timestamp,
dropoff_latitude float,
dropoff_longitude float,
dropoff_nyct2010_gid int,
dropoff_boro string,
dropoff_nta string,
cab_type string,
passenger_count bigint,
trip_distance float,
pay_type string,
fare_amount float,
tip_amount float,
other_amount float,
total_amount float)
stored as parquet;

LOAD DATA INPATH '/user/rstudio/trips_model_data' INTO TABLE trips_model_data;

