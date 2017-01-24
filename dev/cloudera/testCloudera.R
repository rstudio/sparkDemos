library(sparklyr)
library(dplyr)
library(ggplot2)
library(ggvis)

Sys.setenv(JAVA_HOME="/usr/lib/jvm/java-7-oracle-cloudera/")
Sys.setenv(SPARK_HOME = '/opt/cloudera/parcels/CDH/lib/spark')

conf <- spark_config()
conf$spark.executor.cores <- 16
conf$spark.executor.memory <- "24G"
conf$spark.yarn.am.cores  <-   16
conf$spark.yarn.am.memory <- "24G"

sc <- spark_connect(master = "yarn-client", version="1.6.0", config = conf)

trips_model_data_tbl <- tbl(sc, "trips_model_data")
nyct2010_tbl <- tbl(sc, "nyct2010")

source("dev/cloudera/sqlvis_histogram.R")
source("dev/cloudera/sqlvis_raster.R")

tbl_cache(sc, "trips_model_data")

mydat_tbl <- spark_read_csv(sc, "mydat", "/user/nathan/test.csv")

### Histogram

sqlvis_compute_histogram(nyct2010_tbl, "ct2010") %>%
  sqlvis_ggplot_histogram(title = "Histogram")

sqlvis_compute_histogram(nyct2010_tbl, "ct2010") %>%
  sqlvis_ggvis_histogram

trips_model_data_tbl %>%
  filter(fare_amount > 0 & fare_amount < 100) %>%
  sqlvis_compute_histogram("fare_amount") %>%
  sqlvis_ggplot_histogram(title = "Fare Amount")

trips_model_data_tbl %>%
  filter(pickup_nta == "Airport" & dropoff_boro != "Staten Island") %>%
  filter(fare_amount > 0 & fare_amount < 100) %>%
  sqlvis_compute_histogram("fare_amount") %>%
  sqlvis_ggplot_histogram(title = "Airport Pickup Fare Amount")

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

trips_model_data_tbl %>%
  sqlvis_compute_tiles("pickup_longitude", "pickup_latitude", 300) %>%
  sqlvis_ggplot_raster(title = "All Pickups")

trips_model_data_tbl %>%
  sqlvis_compute_tiles("dropoff_longitude", "dropoff_latitude") %>%
  sqlvis_ggplot_raster(title = "All Dropoffs")

trips_model_data_tbl %>%
  filter(pickup_boro == "Manhattan") %>%
  sqlvis_compute_tiles("pickup_longitude", "pickup_latitude") %>%
  sqlvis_ggplot_raster(title = "Manhattan Pickups")

trips_model_data_tbl %>%
  filter(pickup_boro == "Manhattan") %>%
  sqlvis_compute_tiles("dropoff_longitude", "dropoff_latitude") %>%
  sqlvis_ggplot_raster(title = "Manhattan Pickup")

trips_model_data_tbl %>%
  filter(pickup_boro != dropoff_boro) %>%
  sqlvis_compute_tiles("dropoff_longitude", "dropoff_latitude") %>%
  sqlvis_ggplot_raster(title = "Manhattan Dropoffs")

trips_model_data_tbl %>%
  filter(pickup_nta == "Lincoln Square" & dropoff_boro == "Manhattan") %>%
  sqlvis_compute_tiles("dropoff_longitude", "dropoff_latitude") %>%
  sqlvis_ggplot_raster(title = "Lincon Square Pickup and Manhattan Dropoffs")

trips_model_data_tbl %>%
  filter(pickup_nta == "Airport" & dropoff_boro != "Staten Island") %>%
  sqlvis_compute_tiles("dropoff_longitude", "dropoff_latitude") %>%
  sqlvis_ggplot_raster(title = "Airport Dropoffs")

trips_model_data_tbl %>%
  filter(fare_amount > 0 & fare_amount < 100) %>%
  filter(tip_amount > 0 & tip_amount < 25) %>%
  filter(pickup_boro == "Manhattan" & dropoff_boro == "Brooklyn") %>%
  sqlvis_compute_tiles("fare_amount", "tip_amount") %>%
  sqlvis_ggplot_raster(title = "Tip and Fare Correlation") -> p

p
p + geom_abline(intercept = 0, 
                slope = c(10,15,20,22,25,27,30,33)/25, 
                col = 'red', alpha = 0.2, size = 1)

### facets

trips_model_data_tbl %>%
  sqlvis_compute_tiles_g("pickup_longitude", "pickup_latitude", "year(pickup_datetime)", 50) %>%
  sqlvis_ggplot_raster_g(title = "All Pickups", ncol = 3)

trips_model_data_tbl %>%
  filter(pickup_boro == "Manhattan" & year(pickup_datetime) > 2009) %>%
  sqlvis_compute_tiles_g("pickup_longitude", "pickup_latitude", "year(pickup_datetime)", 500) %>%
  sqlvis_ggplot_raster_g(title = "Manhattan Pickups", ncol = 3)

trips_model_data_tbl %>%
  filter(pickup_nta == "Airport" & dropoff_boro != "Staten Island" & year(pickup_datetime) > 2009) %>%
  sqlvis_compute_tiles_g("dropoff_longitude", "dropoff_latitude", "year(pickup_datetime)", 500) %>%
  sqlvis_ggplot_raster_g(title = "Airport Dropoffs", ncol = 3)

trips_model_data_tbl %>%
  filter(fare_amount > 0 & fare_amount < 100) %>%
  filter(tip_amount > 0 & tip_amount < 25) %>%
  filter(year(pickup_datetime) > 2009) %>%
  #filter(pickup_boro == "Manhattan" & dropoff_boro == "Brooklyn") %>%
  sqlvis_compute_tiles_g("fare_amount", "tip_amount", "year(pickup_datetime)") %>%
  sqlvis_ggplot_raster_g(title = "Tip and Fare Correlation by Year", ncol = 3)

trips_model_data_tbl2 <- trips_model_data_tbl %>%
  mutate(year = year(pickup_datetime)) %>%
  filter(year == 2015) %>%
  select(fare_amount, tip_amount, cab_type) %>%
  sdf_register("trips_model_data2")


trips_model_data_tbl2 %>%
  filter(fare_amount > 0 & fare_amount < 100) %>%
  filter(tip_amount > 0 & tip_amount < 25) %>%
  sqlvis_compute_tiles_g("fare_amount", "tip_amount", "cab_type") %>%
  sqlvis_ggplot_raster_g(title = "Tip and Fare Correlation by Cab Type", ncol = 2)




### workspace
data <- trips_model_data_tbl
x_field = "pickup_longitude"
y_field = "pickup_latitude"
resolution = 50

data_prep <- data %>%
  mutate_(group = "year(pickup_datetime)") %>%
  select_(g = "group", x = x_field, y = y_field) %>%
  filter(!is.na(x), !is.na(y))

s <- data_prep %>%
  summarise(max_x = max(x),
            max_y = max(y), 
            min_x = min(x),
            min_y = min(y)) %>%
  mutate(rng_x = max_x - min_x,
         rng_y = max_y - min_y,
         resolution = resolution) %>%
  collect()

counts <- data_prep %>% 
  mutate(res_x = round((x-s$min_x)/s$rng_x*resolution, 0),
         res_y = round((y-s$min_y)/s$rng_y*resolution, 0)) %>%
  count(g, res_x, res_y) %>%
  collect

tmp <- list(counts = counts,
     limits = s,
     vnames = c(x_field, y_field)
)  

s <- tmp$limits
d <- tmp$counts
v <- tmp$vnames

xx <- setNames(seq(1, s$resolution, len = 3), round(seq(s$min_x, s$max_x, len = 3),2))
yy <- setNames(seq(1, s$resolution, len = 3), round(seq(s$min_y, s$max_y, len = 3),2))

ggplot(d, aes(res_x, res_y)) + 
  geom_raster(aes(fill = n)) +
  coord_fixed() +
  facet_wrap(~ g, ncol = 4) +
  scale_fill_distiller(palette = "Spectral", trans = "log", name = "Frequency") +
  scale_x_continuous(breaks = xx, labels = names(xx)) +
  scale_y_continuous(breaks = yy, labels = names(yy)) +
  labs(x = v[1], y = v[2])

source("dev/cloudera/sqlvis_raster.R")



