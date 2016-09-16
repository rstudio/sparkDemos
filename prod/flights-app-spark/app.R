# R Packages
library(nycflights13)
library(dplyr)
library(ggplot2)
library(DT)
library(leaflet)
library(geosphere)
library(sparklyr)

# Connect to local Spark instance
sc <- spark_connect(master = "local", version = '2.0.0')

# Copy flights data into Spark
copy_to(sc, flights, "flights_s", overwrite = TRUE)
flights_tbl <- tbl(sc, 'flights_s')

# Copy airlines data into Spark
copy_to(sc, airlines, "airlines_s", overwrite = TRUE)
airlines_tbl <- tbl(sc, 'airlines_s')

# Prepare mode data
model_data <- flights_tbl %>%
  filter(!is.na(arr_delay) & !is.na(dep_delay) & !is.na(distance)) %>%
  filter(dep_delay > 15 & dep_delay < 240) %>%
  filter(arr_delay > -60 & arr_delay < 360) %>%
  left_join(airlines_tbl, by = c("carrier" = "carrier")) %>%
  mutate(gain = dep_delay - arr_delay) %>%
  select(origin, dest, carrier, airline = name, distance, dep_delay, arr_delay, gain)

# Partition data into train and validation
partitions <- model_data %>%
  sdf_partition(train_data = 0.5, valid_data = 0.5, seed = 777)

# Train a linear model in Spark
lm1 <- ml_linear_regression(partitions$train_data, gain ~ distance + dep_delay + carrier)

# Score the validation data
pred_tbl <- sdf_predict(lm1, partitions$valid_data)

# Create scored look up data for Shiny app 
lookup_tbl <- pred_tbl %>%
  group_by(origin, dest, carrier, airline) %>%
  summarize(
    flights = n(),
    distance = mean(distance),
    avg_dep_delay = mean(dep_delay),
    avg_arr_delay = mean(arr_delay),
    avg_gain = mean(gain),
    pred_gain = mean(prediction)
  )

# Cache the look up table
sdf_register(lookup_tbl, "lookup")
tbl_cache(sc, "lookup")

# Find distinct airport codes
carrier_origin <- c("JFK", "LGA", "EWR")
carrier_dest <- c("BOS", "DCA", "DEN", "HNL", "LAX", "SEA", "SFO", "STL")

# Shiny UI
ui <- fluidPage(

  # Set display mode to bottom
  tags$script(' var setInitialCodePosition = function() 
               { setCodePosition(false, false); }; '),
  
  # Title
  titlePanel("NYCFlights13 Time Gained in Flight"),
 
  # Create sidebar 
  sidebarLayout(
    sidebarPanel(
      radioButtons("origin", "Flight origin:",
                   carrier_origin, selected = "JFK"),
      br(),
      
      radioButtons("dest", "Flight destination:",
                   carrier_dest, selected = "SFO")
      
      ),
    
    # Show a tabset that includes a plot, model, and table view
    mainPanel(
      tabsetPanel(type = "tabs", 
                  tabPanel("Plot", plotOutput("plot")), 
                  tabPanel("Map", leafletOutput("map")), 
                  tabPanel("Data", dataTableOutput("datatable"))
      )
    )
    )
)

# Shiny server function
server <- function(input, output) {
  
  # Identify origin lat and log
  origin <- reactive({
    req(input$origin)
    filter(nycflights13::airports, faa == input$origin)
  })
  
  # Identify destination lat and log
  dest <- reactive({
    req(input$dest)
    filter(nycflights13::airports, faa == input$dest)
  })
  
  # Create plot data
  plot_data <- reactive({
    req(input$origin, input$dest)
    lookup_tbl %>%
      filter(origin==input$origin & dest==input$dest) %>%
      ungroup() %>%
      select(airline, flights, distance, avg_gain, pred_gain) %>%
      collect
  })
  
  # Plot observed versus predicted time gain for carriers and route
  output$plot <- renderPlot({
    ggplot(plot_data(), aes(factor(airline), pred_gain)) + 
      geom_bar(stat = "identity", fill = '#2780E3') +
      geom_point(aes(factor(airline), avg_gain)) +
      coord_flip() +
      labs(x = "", y = "Time gained in flight (minutes)") +
      labs(title = "Observed gain (point) vs Predicted gain (bar)")
  })

  # Output the route map  
  output$map <- renderLeaflet({
    gcIntermediate(
      select(origin(), lon, lat),
      select(dest(), lon, lat),
      n=100, addStartEnd=TRUE, sp=TRUE
    ) %>%
      leaflet() %>%
      addProviderTiles("CartoDB.Positron") %>%
      addPolylines()
  })  
  
  # Print table of observed and predicted gains by airline
  output$datatable <- renderDataTable(
    datatable(plot_data()) %>%
      formatRound(c("flights", "distance"), 0) %>%
      formatRound(c("avg_gain", "pred_gain"), 1)
  )
  
}

# Run Shiny
shinyApp(ui = ui, server = server)
