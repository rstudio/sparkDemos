### Big data histogram
bigvis_compute_histogram <- function(data, x, bins = 30){

  x_name <-x
  
  ranges <- data %>%
    dplyr::select_(x_field=x) %>%
    dplyr::filter(!is.na(x_field)) %>%
    dplyr::summarise(xmax = max(x_field),
                     xmin = min(x_field)) %>%
    dplyr::collect()
  
  max_x <- ranges$xmax[1]
  min_x <- ranges$xmin[1]
  
  bin_value <- (max_x - min_x) / (bins)
  
  all_bins <- data.frame(key_bin=0:(bins-1), bin=1:bins, bin_ceiling=(0:(bins-1)*bin_value)+min_x)
  new_bins <- as.numeric(c((0:(bins-1)*bin_value)+min_x, max_x))

  plot_table <- data %>%
    dplyr::select_(x_field=x) %>%
    dplyr::filter(!is.na(x_field)) %>%
    dplyr::mutate(x_field = as.double(x_field)) %>%
    sparklyr::ft_bucketizer(input.col = "x_field", output.col = "key_bin", splits=new_bins) %>%
    dplyr::group_by(key_bin) %>%
    dplyr::tally() %>%
    dplyr::collect()
  
  plot_table %>%
    dplyr::full_join(all_bins, by="key_bin") %>%
    dplyr::arrange(key_bin) %>%
    dplyr::mutate(n = ifelse(!is.na(n), n, 0)) %>%
    dplyr::select(bin = key_bin, count = n, bin_ceiling)

  }

bigvis_ggplot_histogram <- function(plot_table){
  plot_table %>%
    ggplot2::ggplot(ggplot2::aes(x=bin_ceiling, y=count, fill=count), color="black") +
    ggplot2::geom_bar(stat = "identity") +
    ggplot2::theme(legend.position = "none")
  }

bigvis_ggvis_histogram <- function(plot_table){
  plot_table %>%
    ggvis::ggvis(x=~bin_ceiling, y=~count) %>%
    ggvis::layer_bars() %>%
    ggvis::add_axis("y", title="")
}

