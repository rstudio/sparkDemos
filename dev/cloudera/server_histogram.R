#' Build a Histogram
#'
#' Computes bins server side and creates the histogram inside R.
#'
#' @param data A Spark DataFrame (tbl_spark).
#' @param x Quoted name of column to use in the plot.
#' @param bins Number of bins to use. Defaults to 30.
#' @param output Type of plot returned. Current options are: "ggplot", "ggvis" & "shiny". The option "data" returns a data.frame.
#' @export
server_histogram <- function(
  data,
  x,
  bins=30
)
{
  
  #------------------ Part 1 - Server-side pre-aggregation -----------------------
  
  x_name <-x
  
  #----- Spark
  # pre-calculating max reduces the final query time
  ranges <-   data %>%
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
  # Spark SQL query
  plot_table <- data %>%
    dplyr::select_(x_field=x) %>%
    dplyr::filter(!is.na(x_field)) %>%
    dplyr::mutate(x_field = as.double(x_field)) %>%
    sparklyr::ft_bucketizer(input.col = "x_field", output.col = "key_bin", splits=new_bins) %>%
    dplyr::group_by(key_bin) %>%
    dplyr::tally() %>%
    dplyr::collect()
  
  plot_table <- plot_table %>%
    dplyr::full_join(all_bins, by="key_bin") %>%
    dplyr::arrange(key_bin) %>%
    dplyr::mutate(n = ifelse(!is.na(n), n, 0)) %>%
    dplyr::select(bin = key_bin,
                  count = n,
                  bin_ceiling) 
}

server_layer <- function(plot_table, output="ggplot"){
  #---------------------------- Part 2 - Output creation ---------------------------
  
  #----- ggplot2
  if(output=="ggplot"){
    return(
      ggplot2::ggplot(plot_table) +
        ggplot2::geom_bar(ggplot2::aes(x=bin_ceiling, y=count, fill=count), color="black", stat = "identity") +
        ggplot2::theme(legend.position="none")
    )}
  
  #----- ggvis
  if(output=="ggvis"){
    return(
      plot_table %>%
        ggvis::ggvis(x=~bin_ceiling, y=~count) %>%
        ggvis::layer_bars()  %>%
        ggvis::add_axis("y", title="")
    )
  }
  
  #----- ggvis w/ shiny
  if(output=="shiny"){
    return(
      plot_table %>%
        ggvis::ggvis(x=~bin_ceiling, y=~count) %>%
        ggvis::layer_bars() %>%
        ggvis::add_tooltip(all_ggvis_values, "hover")
      
    )
  }
  
  #----- data
  if(output=="data")return(plot_table)
}


# Used to create the tooltip for the 'shiny' output
#' @export
all_ggvis_values <- function(x) {
  if(is.null(x)) return(NULL)
  paste0(names(x), ": ", format(x), collapse = "<br />")
}
