library(sparklyr)
library(dplyr)
library(ggplot2)

Sys.setenv(JAVA_HOME="/usr/lib/jvm/java-7-oracle-cloudera/")
Sys.setenv(SPARK_HOME = '/opt/cloudera/parcels/CDH/lib/spark')


conf <- spark_config()
#conf$spark.num.executors <- 60
conf$spark.executor.cores <- 16
conf$spark.executor.memory <- "24G"
conf$spark.yarn.am.cores  <-   16
conf$spark.yarn.am.memory <- "24G"

sc <- spark_connect(master = "yarn-client", version="1.6.0", config = conf)

###

system.time(nrow(tbl(sc,"trips_model_data")))


test_table <- tbl(sc,"trips_model_data") %>%
  select(trip_distance,
         tip_amount,
         fare_amount)

system.time(sdf_register(test_table, "test_table"))
system.time(tbl_cache(sc, "test_table")) 


system.time({
  test_model <- tbl(sc, "test_table") %>%
    ml_linear_regression(tip_amount~trip_distance+fare_amount)})

test <- spark_read_csv(sc, name = "test", path = "hdfs:///user/nathan/test.csv")

library(ggplot2)
spark_plot_hist(test, "ct2010")
spark_plot_hist(test, "boroct2010")
spark_plot_boxbin(test, "ct2010", "boroct2010")
spark_plot_point(test, "ct2010", "boroct2010")

spark_plot_hist(trips_joined_tbl, "pickup_nyct2010_gid")
spark_plot_hist(trips_joined_tbl, "pickup_latitude")
spark_plot_boxbin(trips_joined_tbl, "pickup_nyct2010_gid", "dropoff_nyct2010_gid") # fails

### Histogram

source("dev/cloudera/sqlvis_histogram.R")
nyct2010_tbl <- tbl(sc, "nyct2010")
bigvis_compute_histogram(nyct2010_tbl, "ct2010") %>%
  bigvis_ggplot_histogram

trips_model_data_tbl %>%
  filter(pickup_nta == "Airport" & dropoff_boro != "Staten Island") %>%
  filter(fare_amount > 0 & fare_amount < 100) %>%
  sqlvis_compute_histogram("fare_amount") %>%
  sqlvis_ggplot_histogram(title = "Airport Pickup Fare Amount")

trips_model_data_tbl %>%
  filter(fare_amount > 0 & fare_amount < 100) %>%
  sqlvis_compute_histogram("fare_amount") %>%
  sqlvis_ggplot_histogram(title = "Fare Amount")

trips_model_data_tbl %>%
  filter(fare_amount > 0 & fare_amount < 100) %>%
  filter(pickup_boro == "Manhattan" & dropoff_boro == "Brooklyn") %>%
  sqlvis_compute_histogram("fare_amount") %>%
  sqlvis_ggplot_histogram(title = "Fare Amount Manhattan to Brooklyn")

trips_model_data_tbl %>%
  filter(tip_amount > 0 & tip_amount < 25) %>%
  filter(pickup_boro == "Manhattan" & dropoff_boro == "Brooklyn") %>%
  sqlvis_compute_histogram("tip_amount") %>%
  sqlvis_ggplot_histogram(title = "Tip Amount Manhattan to Brooklyn")

## Raster

source("dev/cloudera/bigvis_tile.R")

trips_model_data_tbl %>%
  bigvis_compute_tiles("pickup_longitude", "pickup_latitude") %>%
  sqlvis_ggplot_raster(title = "All Pickups")

trips_model_data_tbl %>%
  bigvis_compute_tiles("dropoff_longitude", "dropoff_latitude") %>%
  sqlvis_ggplot_raster(title = "All Dropoffs")

trips_model_data_tbl %>%
  filter(pickup_boro == "Manhattan") %>%
  bigvis_compute_tiles("pickup_longitude", "pickup_latitude") %>%
  sqlvis_ggplot_raster(title = "Manhattan Pickups")

trips_model_data_tbl %>%
  filter(pickup_boro == "Manhattan") %>%
  bigvis_compute_tiles("dropoff_longitude", "dropoff_latitude") %>%
  sqlvis_ggplot_raster(title = "Manhattan Dropoffs")

trips_model_data_tbl %>%
  filter(pickup_boro != dropoff_boro) %>%
  bigvis_compute_tiles("dropoff_longitude", "dropoff_latitude") %>%
  sqlvis_ggplot_raster(title = "Manhattan Dropoffs")

trips_model_data_tbl %>%
  filter(pickup_nta == "Lincoln Square" & dropoff_boro == "Manhattan") %>%
  bigvis_compute_tiles("dropoff_longitude", "dropoff_latitude") %>%
  sqlvis_ggplot_raster(title = "Lincon Square Pickup and Manhattan Dropoffs")

trips_model_data_tbl %>%
  filter(pickup_nta == "Airport" & dropoff_boro != "Staten Island") %>%
  bigvis_compute_tiles("dropoff_longitude", "dropoff_latitude") %>%
  sqlvis_ggplot_raster(title = "Airport Pickup")

trips_model_data_tbl %>%
  filter(fare_amount > 0 & fare_amount < 100) %>%
  filter(tip_amount > 0 & tip_amount < 25) %>%
  filter(pickup_boro == "Manhattan" & dropoff_boro == "Brooklyn") %>%
  bigvis_compute_tiles("fare_amount", "tip_amount") %>%
  sqlvis_ggplot_raster(title = "Tip and Fare Correlation") -> p

p
p + geom_abline(intercept = 0, 
                slope = c(10,15,20,22,25,27,30,33)/25, 
                col = 'red', alpha = 0.2, size = 1)

