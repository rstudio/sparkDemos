library(shinydashboard)

header <- dashboardHeader(
  title = "Flights Data Analysis"
)
sidebar <- dashboardSidebar(
  sidebarMenu(
    menuItem("Flights by year and airline", tabName = "years"),
    menuItem("Delay Density", tabName = "delay_density"),
    menuItem("Cancelled flights", tabName = "cancelled"),
    menuItem("Flights by day of week", tabName = "dayofweek")
  )
)


body <- dashboardBody(
  tabItems(
    tabItem("years",
            fluidRow(
              column(width = 8,
                     box(width = NULL, solidHeader = TRUE,
                         plotOutput('yearsPlot')
                     )
              ),
              column(width = 3,
                     box(width = NULL, status = "warning",
                         uiOutput("years_selection"),
                         radioButtons("years_selection", label = h3("Select a year"),
                                      years_sub$year, selected = 2000) 
                     )
              ),
              column(width = 3,
                     box(width = NULL, status = "warning",
                         uiOutput("carrier_selection"),
                         radioButtons("carrier_selection", label = h3("Select an airline"),
                                      airlines_r$description, selected = "American Airlines Inc.")
                     )
              )
              
            )
    ),
    tabItem("delay_density",
            fluidRow(
              column(width = 9,
                     box(width = NULL, solidHeader = TRUE,
                         plotOutput('densityPlot')
                     )
              ),
              column(width = 3,
                     box(width = NULL, status = "warning",
                         uiOutput("density_selection"),
                         radioButtons("density_selection", label = h3("Select arrival or departure"),
                                      choices = c(
                                        Departure = "depdelay_mean",
                                        Arrival = "arrdelay_mean"
                                      ),
                                      selected = "arrdelay_mean")              
                     )
              )
              
            )
    ),
    tabItem("cancelled",
            fluidRow(
              column(width = 9,
                     box(width = NULL, solidHeader = TRUE,
                         plotOutput('cancelPlot')
                     )
              ),
              column(width = 3,
                     box(width = NULL, status = "warning",
                         uiOutput("years_cancel_selection"),
                         radioButtons("years_cancel_selection", label = h3("Select a year"),
                                      years_sub$year, selected = 2008)
                     )
              )
            )
    ),
    tabItem("dayofweek",
            fluidRow(
              column(width = 9,
                     box(width = NULL, solidHeader = TRUE,
                         plotOutput('dayPlot')
                     )
              ),
              column(width = 3,
                     box(width = NULL, status = "warning",
                         uiOutput("day_selection"),
                         radioButtons("day_selection", label = h3("Select a year"),
                                      years_sub$year, selected = 2008)
                     )
              )
            )
    )    
    
  )
)

dashboardPage(
  header,
  sidebar,
  body
)