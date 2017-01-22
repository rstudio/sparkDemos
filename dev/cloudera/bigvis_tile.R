### Big data tile plot

bigvis_compute_tiles <- function(data, x_field, y_field, resolution = 500){
  
  data_prep <- data %>%
    select_(x = x_field, y = y_field) %>%
    filter(!is.na(x), !is.na(y))
  
  s <- data_prep %>%
    summarise(max_x = max(x),
              max_y = max(y), 
              min_x = min(x),
              min_y = min(y)) %>%
    mutate(rng_x = max_x - min_x,
           rng_y = max_y - min_y) %>%
    collect()
  
  image_frame_pre <- data_prep %>% 
    mutate(res_x = round((x-s$min_x)/s$rng_x*resolution, 0),
           res_y = round((y-s$min_y)/s$rng_y*resolution, 0)) %>%
    count(res_x, res_y) %>%
    collect
  
  image_frame_pre %>%
    rename(freq = n) %>%
    mutate(alpha = round(freq / max(freq), 2)) %>%
    rename_(.dots=setNames(list("res_x", "res_y"), c(x_field, y_field)))
  
}

bigvis_ggplot_tiles <- function(data){
  data %>%
    select(x = 1, y = 2, Freq = 4) %>%
    ggplot(aes(x, y)) + 
    geom_tile(aes(fill = Freq)) +
    xlab(colnames(data)[1]) +
    ylab(colnames(data)[2])
}
