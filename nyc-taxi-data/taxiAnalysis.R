library(ggplot2)
library(leaflet)
library(readr)
library(geosphere)
library(tidyr)

library(sparklyr)
library(dplyr)
Sys.setenv(SPARK_HOME="/usr/lib/spark")
config <- spark_config()
sc <- spark_connect(master = "yarn-client", config = config, version = '1.6.1')

timestamp()
tbl_cache(sc, 'trips_par')
timestamp()
dt <- tbl(sc, 'trips_par')

### Data by pickup and dropoff and hour

dt %>%
  filter(!is.na(pickup_nyct2010_gid) & !is.na(dropoff_nyct2010_gid)) %>%
  mutate(pickup_hour = hour(pickup_datetime)) %>%
  mutate(trip_time = unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) %>%
  group_by(pickup_nyct2010_gid, dropoff_nyct2010_gid, pickup_hour) %>% 
  summarize(n = n(),
            trip_time_mean = mean(trip_time),
            trip_time_p25 = percentile(trip_time, 0.25),
            trip_time_p50 = percentile(trip_time, 0.50),
            trip_time_p75 = percentile(trip_time, 0.75),
            trip_dist_mean = mean(trip_distance),
            trip_dist_sd = sd(trip_distance),
            pickup_latitude = mean(pickup_latitude),
            pickup_longitude = mean(pickup_longitude),
            dropoff_latitude = mean(dropoff_latitude),
            dropoff_longitude = mean(dropoff_longitude),
            passenger_mean = mean(passenger_count),
            yellow_count = sum(ifelse(cab_type_id == 1, 1, 0)),
            green_count = sum(ifelse(cab_type_id == 2, 1, 0)),
            uber_count = sum(ifelse(cab_type_id == 3, 1, 0)),
            fare_amount = mean(fare_amount),
            tip_amount = mean(tip_amount)
  ) %>%
  sdf_register("pickup_dropoff_hour")

### Data by pickup and dropoff

dt %>%
  filter(!is.na(pickup_nyct2010_gid) & !is.na(dropoff_nyct2010_gid)) %>%
  mutate(trip_time = unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) %>%
  group_by(pickup_nyct2010_gid, dropoff_nyct2010_gid) %>% 
  summarize(n = n(),
            trip_time_mean = mean(trip_time),
            trip_time_p25 = percentile(trip_time, 0.25),
            trip_time_p50 = percentile(trip_time, 0.50),
            trip_time_p75 = percentile(trip_time, 0.75),
            trip_dist_mean = mean(trip_distance),
            trip_dist_sd = sd(trip_distance),
            pickup_latitude = mean(pickup_latitude),
            pickup_longitude = mean(pickup_longitude),
            dropoff_latitude = mean(dropoff_latitude),
            dropoff_longitude = mean(dropoff_longitude),
            passenger_mean = mean(passenger_count),
            yellow_count = sum(ifelse(cab_type_id == 1, 1, 0)),
            green_count = sum(ifelse(cab_type_id == 2, 1, 0)),
            uber_count = sum(ifelse(cab_type_id == 3, 1, 0)),
            fare_amount = mean(fare_amount),
            tip_amount = mean(tip_amount)
  ) %>%
  sdf_register("pickup_dropoff")

pickup_dropoff_tbl <- tbl(sc, "pickup_dropoff")
tbl_cache(sc, "pickup_dropoff")

pickup_dropoff_tbl %>% count

### Data by pickup

dt %>%
  filter(!is.na(pickup_nyct2010_gid)) %>%
  mutate(trip_time = unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) %>%
  group_by(pickup_nyct2010_gid) %>% 
  summarize(n = n(),
            trip_time_mean = mean(trip_time),
            trip_time_p25 = percentile(trip_time, 0.25),
            trip_time_p50 = percentile(trip_time, 0.50),
            trip_time_p75 = percentile(trip_time, 0.75),
            trip_dist_mean = mean(trip_distance),
            trip_dist_sd = sd(trip_distance),
            pickup_latitude = mean(pickup_latitude),
            pickup_longitude = mean(pickup_longitude),
            passenger_mean = mean(passenger_count),
            yellow_count = sum(ifelse(cab_type_id == 1, 1, 0)),
            green_count = sum(ifelse(cab_type_id == 2, 1, 0)),
            uber_count = sum(ifelse(cab_type_id == 3, 1, 0)),
            fare_amount = mean(fare_amount),
            tip_amount = mean(tip_amount)
  ) %>%
  sdf_register("pickup")

### Model on full data

model_tbl <- dt %>%
  mutate(pickup_hour = hour(pickup_datetime)) %>%
  mutate(pickup_week = weekofyear(pickup_datetime)) %>%
  mutate(trip_time_in_secs = unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) %>%
  filter(!is.na(fare_amount)) %>%
  filter(!is.na(vendor_id)) %>%
  filter(!is.na(pickup_hour)) %>%
  filter(!is.na(pickup_week)) %>%
  filter(!is.na(passenger_count)) %>%
  filter(!is.na(trip_time_in_secs)) %>%
  filter(!is.na(trip_distance))


model_formula <- formula(tip_amount ~ 
                           fare_amount + vendor_id + pickup_hour + pickup_week + 
                           passenger_count + trip_time_in_secs + trip_distance)

model_tbl %>% mutate(x = ifelse(is.na(pickup_nyct2010_gid), 1, 0)) %>% summarize(sum(x))

timestamp()
m1 <- ml_linear_regression(model_tbl, model_formula) # 13 minutes
timestamp()
m2 <- ml_random_forest(model_tbl, model_formula, type = "regression") # never fisnished after 30 minutes
timestamp()
m3 <- ml_decision_tree(model_tbl, model_formula, type = "regression")
timestamp()

### maps on pickup

pickup_tbl <- tbl(sc, "pickup")

pickup <- pickup_tbl %>% 
  filter(n >= 100) %>% 
  mutate(longitude = pickup_longitude) %>%
  mutate(latitude = pickup_latitude) %>%
  collect

m <- leaflet(pickup) %>% 
  setView(lng = -73.97926, lat = 40.67369, zoom = 12) %>%
  addProviderTiles("CartoDB.Positron") %>%
  #addMarkers(~pickup_longitude, ~pickup_latitude) %>%
  #addCircleMarkers(radius = ~sqrt(fare_amount), stroke = F) %>%
  addCircleMarkers(radius = ~log10(n), stroke = F)

m

ggplot(pickup, aes(pickup_longitude, pickup_latitude)) + geom_point()

pickup_dropoff <- pickup_dropoff_tbl %>%
  filter(pickup_nyct2010_gid == 899L & dropoff_nyct2010_gid == 1284L) %>%
  collect

m <- leaflet(pickup) %>% 
  setView(lng = -73.97926, lat = 40.67369, zoom = 12) %>%
  addProviderTiles("CartoDB.Positron") %>%
  #addMarkers(~pickup_longitude, ~pickup_latitude) %>%
  #addCircleMarkers(radius = ~sqrt(fare_amount), stroke = F) %>%
  addCircleMarkers(radius = ~log10(n), stroke = F)

gcIntermediate(
  select(pickup_dropoff, pickup_longitude, pickup_latitude),
  select(pickup_dropoff, dropoff_longitude, dropoff_latitude),
  n=100, addStartEnd=TRUE, sp=TRUE
) %>%
  leaflet() %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolylines()

pickup_dropoff %>%
  select(pickup_nyct2010_gid,dropoff_nyct2010_gid, pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude) %>%
  gather() %>%
  separate(key, c("prefix", "vars"), sep="_") %>%
  spread(vars, value) %>%
  leaflet() %>%
  addProviderTiles("CartoDB.Positron") %>%
  addCircleMarkers()


