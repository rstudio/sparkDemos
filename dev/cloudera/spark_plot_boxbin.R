#' Create a plot inside Spark
#'
#' This function rescales the x,y fields to the size of the 'resolution' argument, passing the rescaled cordinates
#' back to R.  R then in turn creates a PNG file and then loads it into a base plot. 
#' Currently only x,y and color are the only aesthetics
#'
#' @param data A Spark DataFrame.
#' @param x_field Quoted name of a field in the Spark DataFrame that will be used as the X axis
#' @param y_field Quoted name of a field in the Spark DataFrame that will be used as the Y axis
#' @param color_field Quoted name of a field in the Spark DataFrame that will be used for the color. If left empty, the function will use the frequency of X,Y
#' @param resolution Integer of the size resulting image.  
#' @param lab_x Label for X. Defaults to X field name
#' @param lab_y Label for Y.  Defaults to Y field name
#' @param lab_legend Label for the legend. Defaults to the color field name or 'Frequency'.
#' @param legend_top Controls if the legend will be at the top of the plot or not, defaults to "yes" (TRUE)
#' @param legend_right Controls if the legend will be in the right side of the plot or not, defaults to "yes" (TRUE)
#' @export
spark_plot_boxbin <- function(data,
                             x_field=NULL,
                             y_field=NULL,
                             color_field=NULL,
                             resolution=500,
                             title=NULL,
                             lab_x=NULL,
                             lab_y=NULL,
                             lab_legend=NULL,
                             legend_top=TRUE,
                             legend_right=TRUE)
{
  
  #---------------------- PART 1 - Spark DataFrame -------------------------------------
  
  #----- To keep the resulting Spark SQL as simple as possible, the 'ifelse' statement
  #----- switches between using the frequency of records in an given x,y coordinate or
  #----- a continuos variable (color_field) which if used, the min/max are collected
  #----- for use in the legend
  
  ifelse(!is.null(color_field), {
    data <- data %>%
      select_(x=x_field,
              y=y_field,
              color=color_field) %>%
      filter(!is.na(x), !is.na(y))
    
    limits_frame <- data %>%
      summarise(max_x = max(x),
                max_y = max(y), 
                min_x = min(x),
                min_y = min(y),
                min_color = min(color),
                max_color = max(color)) %>% 
      collect()
  },    {
    data <- data %>%
      select_(x=x_field,
              y=y_field) %>%
      filter(!is.na(x), !is.na(y))
    limits_frame <- data %>%
      summarise(max_x = max(x),
                max_y = max(y), 
                min_x = min(x),
                min_y = min(y)) %>%
      collect()
  })
  
  
  
  #---- Rescaling, my final formula adds the min of x & y back to account for negative numbers
  image_frame <- data %>% 
    mutate(res_x = round((x-min(x))/(max(x)-min(x))*resolution, 0),
           res_y = round((y-min(y))/(max(y)-min(y))*resolution, 0)) %>%
    group_by(res_x, res_y) 
  
  #---- if statement adds either the summary of frequency or the field, this is also to 
  #---- keep the resulting Spark SQL as simple as possible
  #---  and finishing off the Spark SQL with rebasing the color to between 0 to 1
  ifelse(!is.null(color_field),
         {image_frame <- image_frame %>%
           summarise(res_color=mean(color)) %>%
           mutate(res_color = round(res_color/limits_frame$max_color[1],2)) %>%
           collect()
         },
         {image_frame <- summarise(image_frame, res_color=n())
         
         #---- Getting the upper and lower counts
         color_frame <- image_frame %>%
           summarise(max_color = max(res_color),
                     min_color=min(res_color)) %>%
           collect()
         
         image_frame <- image_frame %>%
           mutate(res_color = round(res_color/max(res_color),2)) %>%
           collect()} )
  
  
  
  
  #---------------------- PART 2 - Plot creation -------------------------------------
  
  size_x <- max(image_frame$res_x) 
  size_y <- max(image_frame$res_y)
  
  #--- If we change background to 0 and foreground to res_color, the image will 
  #--- be inverted
  background <- 1
  foreground <- 1-image_frame$res_color
  
  #--- Creating matrices of the as big as the edges of x and y 
  image_matrix <- matrix(data=background, nrow=size_x, ncol=size_y)  
  #original_matrix <- image_matrix
  base_matrix <- image_matrix
  negative_matrix <- image_matrix
  
  #--- Creates 3 layers, positive, negative and base
  for(cds in 1:nrow(image_frame)){
    image_matrix[size_y-image_frame$res_y[cds], image_frame$res_x[cds]] <- foreground[cds]
    negative_matrix[size_y-image_frame$res_y[cds], image_frame$res_x[cds]] <- image_frame$res_color[cds]
    base_matrix[size_y-image_frame$res_y[cds], image_frame$res_x[cds]] <- 0}
  
  #---- Build PNG file
  final_array <- array(c( negative_matrix, image_matrix,base_matrix), c(size_x,size_y, 3))
  no_markers <- 9
  markers <- 0:no_markers
  x_markers <- limits_frame$min_x[1] + ((limits_frame$max_x[1]-limits_frame$min_x[1])/no_markers*markers)
  y_markers <- limits_frame$min_y[1] + ((limits_frame$max_y[1]-limits_frame$min_y[1])/no_markers*markers)
  png::writePNG(final_array, file.path("spark_plot_temp.png"))
  
  #---- Build the plot
  img <- png::readPNG(file.path("spark_plot_temp.png")) 
  plot(x_markers, 
       y_markers, 
       type='n', main=title, xlab=ifelse(!is.null(lab_x), lab_x, x_field), ylab=ifelse(!is.null(lab_y), lab_y, y_field))
  
  lim <- par()
  rasterImage(img, lim$usr[1], lim$usr[3], lim$usr[2], lim$usr[4])
  
  legend_position <- paste(ifelse(legend_top,"top", "bottom"), ifelse(legend_right, "right","left"),sep="")
  
  invisible(
    ifelse(!is.null(color_field),
           legend(legend_position, legend=c(limits_frame$min_color[1],limits_frame$max_color[1]), lwd=c(3,3), col=c("green","red"), title=ifelse(!is.null(lab_legend),lab_legend, color_field), cex=0.75)
           ,       
           legend(legend_position, legend=c(color_frame$min_color[1],color_frame$max_color[1]), lwd=c(3,3), col=c("green","red"), title="Frequency", cex=0.75)
           
    )
  )
  unlink(file.path("spark_plot_temp.png"))
}