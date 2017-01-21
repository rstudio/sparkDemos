library(sparklyr)
library(dplyr)

Sys.setenv(JAVA_HOME="/usr/lib/jvm/java-7-oracle-cloudera/")
Sys.setenv(SPARK_HOME = '/opt/cloudera/parcels/CDH/lib/spark')


conf <- spark_config()
#conf$spark.num.executors <- 60
conf$spark.executor.cores <- 16
conf$spark.executor.memory <- "24G"
conf$spark.yarn.am.cores  <-   16
conf$spark.yarn.am.memory <- "24G"

sc <- spark_connect(master = "yarn-client", version="1.6.0", config = conf)


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
