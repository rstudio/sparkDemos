### Big data tile plot

# data <- tbl(sc, "trips_model_data")
# x_field <- "pickup_longitude"
# y_field <- "pickup_latitude"
# resolution <- 50

sqlvis_compute_tiles <- function(data, x_field, y_field, resolution = 500){
  
  data_prep <- data %>%
    select_(x = x_field, y = y_field) %>%
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
    count(res_x, res_y) %>%
    collect

  list(counts = counts,
       limits = s,
       vnames = c(x_field, y_field)
       )  

}

sqlvis_ggplot_raster <- function(data, ...) {

  s <- data$limits
  d <- data$counts
  v <- data$vnames

  xx <- setNames(seq(1, s$resolution, len = 6), round(seq(s$min_x, s$max_x, len = 6),2))
  yy <- setNames(seq(1, s$resolution, len = 6), round(seq(s$min_y, s$max_y, len = 6),2))

  ggplot(d, aes(res_x, res_y)) + 
    geom_raster(aes(fill = n)) +
    coord_fixed() +
    scale_fill_distiller(palette = "Spectral", trans = "log", name = "Frequency") +
    scale_x_continuous(breaks = xx, labels = names(xx)) +
    scale_y_continuous(breaks = yy, labels = names(yy)) +
    labs(x = v[1], y = v[2], ...)
    
}    

