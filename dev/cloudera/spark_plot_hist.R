spark_plot_hist <- function(data,
                            x_field,
                            breaks=30)
{
  #----- Pre calculating the max x brings down the time considerably
  max_x <-   data %>%
    select_(x=x_field) %>%
    summarise(xmax = max(x)) %>%
    collect()
  max_x <- max_x$xmax[1]
  
  #----- The entire function is one long pipe 
  data %>%
    select_(x=x_field) %>%
    filter(!is.na(x)) %>%
    mutate(bucket = round(x/(max_x/(breaks-1)),0)) %>%
    group_by(bucket) %>%
    summarise(top=max(x),
              bottom=min(x),
              count=n()) %>%
    arrange(bucket) %>%
    collect %>%
    ggplot() +
    geom_bar(aes(x=((top-bottom)/2)+bottom, y=count), color="black", stat = "identity") +
    labs(x=x_field) +
    theme_minimal() +
    theme(legend.position="none")}