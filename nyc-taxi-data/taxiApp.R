library(sparklyr)
library(dplyr)
library(shiny)

Sys.setenv(SPARK_HOME="/usr/lib/spark")
config <- spark_config()
sc <- spark_connect(master = "yarn-client", config = config, version = '1.6.1')

tbl_cache(sc, 'trips_csv_2015_12')
trips_tbl <- tbl(sc, 'trips_csv_2015_12')

ui <- fluidPage(
  
  titlePanel("NYC Taxi Trips"),
  
  sidebarLayout(
    sidebarPanel(
      selectInput("hour", "Hour of the day", 0:23, 12)
    ),
    
    mainPanel(
      tableOutput("fare")
    )
  )
)

server <- function(input, output) {
  
  fare <- reactive({
    trips_tbl %>%
      mutate(pickup_hour = hour(pickup_datetime)) %>%
      filter(pickup_hour == input$hour) %>%
      summarize(fare_amount = mean(fare_amount)) %>%
      collect
  })
  
  output$fare <- renderTable({
    fare()
  })
  
}

shinyApp(ui = ui, server = server)