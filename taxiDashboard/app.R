
global <- function() {
  
  Sys.setenv(SPARK_HOME="/usr/lib/spark")
  config <- spark_config()
  sc <- spark_connect(master = "yarn-client", config = config, version = '1.6.1')
  
  tbl_cache(sc, 'trips_par')
  shiny_trips_tbl <<- tbl(sc, 'trips_par')
  
  distinct_gid <- function(data, gid, cutoff = 100000){
    data %>%
      filter_(!is.na(gid)) %>%
      group_by_(gid) %>%
      count %>%
      filter(n > cutoff) %>%
      select_(gid) %>%
      arrange_(gid) %>%
      collect
  }
  
  pickup_nyct2010_gid <<- shiny_trips_tbl %>%
    distinct_gid("pickup_nyct2010_gid") %>%
    unlist %>%
    unname

  dropoff_nyct2010_gid <<- shiny_trips_tbl %>%
    distinct_gid("dropoff_nyct2010_gid") %>%
    unlist %>%
    unname

}

ui <- fluidPage(
   
   titlePanel("NYC Taxi Data"),
   
   sidebarLayout(
      sidebarPanel(
        selectInput("pickup",  "Taxi origin", pickup_nyct2010_gid, 1250),
        selectInput("dropoff",  "Taxi destination", dropoff_nyct2010_gid, 2056)
      ),
      
      mainPanel(
         plotOutput("distPlot")
      )
   )
)

server <- function(input, output) {

  withProgress(message = "dplyr:", detail = "filter, mutate, summarize", {
    
  shiny_pickup_dropoff <- reactive({
    shiny_trips_tbl %>%
    filter(pickup_nyct2010_gid == input$pickup & dropoff_nyct2010_gid == input$dropoff) %>%
    mutate(pickup_hour = hour(pickup_datetime)) %>%
    mutate(trip_time = unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) %>%
    group_by(pickup_hour) %>% 
    summarize(n = n(),
              trip_time_p10 = percentile(trip_time, 0.10),
              trip_time_p25 = percentile(trip_time, 0.25),
              trip_time_p50 = percentile(trip_time, 0.50),
              trip_time_p75 = percentile(trip_time, 0.75),
              trip_time_p90 = percentile(trip_time, 0.90)) %>%
    collect
  })
  
  })
  
  output$distPlot <- renderPlot({
    ggplot(shiny_pickup_dropoff(), aes(x = pickup_hour)) +
    geom_line(aes(y = trip_time_p50, alpha = "Median")) +
    geom_ribbon(aes(ymin = trip_time_p25, ymax = trip_time_p75, alpha = "25–75th percentile")) +
    geom_ribbon(aes(ymin = trip_time_p10, ymax = trip_time_p90, alpha = "10–90th percentile")) +
    scale_y_continuous("trip duration in minutes") +
    ggtitle(paste("Pickup = ", input$pickup, ";", "Dropoff =", input$dropoff))
   })
  
}

shinyApp(ui = ui, server = server, onStart = global)

