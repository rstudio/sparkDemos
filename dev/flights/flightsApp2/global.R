library(nycflights13)
library(tibble)
library(ggplot2)
library(dplyr)
library(sparklyr)
library(lubridate)
library(MASS)

Sys.setenv(SPARK_HOME="/usr/lib/spark")
system.time(sc <- spark_connect(master = "yarn-client", version = '2.0.0'))

# Cache airlines Hive table into Spark
#system.time(tbl_cache(sc, 'airlines'))

# We use a small subset of airlines in this application
#system.time(airlines_tbl <- tbl(sc, 'airlines'))
#system.time(airlines_tbl <- spark_read_csv(sc, "airlines", "hdfs:///airlines/airlines.csv", memory=TRUE))
#airlines_r <- airlines_tbl %>% arrange(description) %>% collect
airlines_r <- tibble::tibble(
  code = c("B6", "UA", "AA", "DL", "WN", "US"), 
  description = c("JetBlue Airways","United Air Lines Inc.",
                  "American Airlines Inc." , "Delta Air Lines Inc.",
                  "Southwest Airlines Co.","US Airways Inc.")
)

# We use the airports from nycflights13 package in this application
# airports_tbl <- copy_to(sc, nycflights13::airports, "airports", overwrite = TRUE)
# airports <- airports_tbl %>% collect
airports <- nycflights13::airports

# Cache flights Hive table into Spark
#system.time(tbl_cache(sc, 'flights'))
#system.time(flights_tbl <- tbl(sc, 'flights'))

#Instead of caching the flights data (which takes very long), we load the data in Parquet
#format from HDFS. First the following 2 commented lines must be run to save the data.
#system.time(flights_tbl <- tbl(sc, 'flights'))
#system.time(spark_write_parquet(flights_tbl, "hdfs:///flights-parquet-all"))
system.time(flights_tbl <- spark_read_parquet(sc, "flights_s", "hdfs:///flights-parquet-all", memory=FALSE))

years <- tibble::tibble(year = c(1987:2008))
years_sub <- tibble::tibble(year = c(1999:2008))
dests <- c("LAX","ORD","ATL","HNL")

delay <- flights_tbl %>%
  group_by(tailnum) %>%
  summarise(count = n(),
            dist = mean(distance),
            delay = mean(arrdelay),
            arrdelay_mean = mean(arrdelay),
            depdelay_mean = mean(depdelay)) %>%
  filter(count > 20,
         dist < 2000,
         !is.na(delay)) %>%
  collect

