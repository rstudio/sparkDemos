### Big data histogram
sqlvis_compute_histogram <- function(data, x_name, bins = 30){

  data_prep <- data %>%
    select_(x_field = x_name) %>%
    filter(!is.na(x_field)) %>%
    mutate(x_field = as.double(x_field))
    
  s <- data_prep %>%
    summarise(max_x = max(x_field), min_x = min(x_field)) %>%
    mutate(bin_value = (max_x - min_x) / bins) %>%
    collect()
  
  new_bins <- as.numeric(c((0:(bins - 1) * s$bin_value) + s$min_x, s$max_x))

  plot_table <- data_prep %>%
    ft_bucketizer(input.col = "x_field", output.col = "key_bin", splits = new_bins) %>%
    group_by(key_bin) %>%
    tally() %>%
    collect()
  
  all_bins <- data.frame(
    key_bin = 0:(bins - 1), 
    bin = 1:bins, 
    bin_ceiling = head(new_bins, -1)
  )
  
  plot_table %>%
    full_join(all_bins, by="key_bin") %>%
    arrange(key_bin) %>%
    mutate(n = ifelse(!is.na(n), n, 0)) %>%
    select(bin = key_bin, count = n, bin_ceiling) %>%
    rename_(.dots = setNames(list("bin_ceiling"), x_name))
  
  }

sqlvis_ggplot_histogram <- function(plot_table, ...){
  plot_table %>%
    select(x = 3, y = 2) %>%
    ggplot(aes(x, y)) +
    geom_bar(stat = "identity", fill = "cornflowerblue") +
    theme(legend.position = "none") +
    labs(x = colnames(plot_table)[3], y = colnames(plot_table)[2], ...)
  }

sqlvis_ggvis_histogram <- function(plot_table, ...){
  plot_table %>%
    select(x = 3, y = 2) %>%
    ggvis(x = ~x, y = ~y) %>%
    layer_bars() %>%
    add_axis("x", title = colnames(plot_table)[3]) %>%
    add_axis("y", title = colnames(plot_table)[2])
}

