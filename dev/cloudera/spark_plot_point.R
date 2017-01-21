spark_plot_point<- function(data,
                            x_field=NULL,
                            y_field=NULL,
                            color_field=NULL)
{
  
  data %>%         
    select_(x=x_field, y=y_field) %>%
    group_by(x,y) %>%
    tally() %>%
    collect() %>%
    ggplot() +
    geom_point(aes(x=x, y=y, color=n)) +
    labs(x=x_field, y=y_field)
  
}