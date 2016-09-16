library(shinydashboard)
library(dplyr)
library(maps)
library(geosphere)
library(lubridate)
library(MASS)

source("global.R")

function(input, output, session) {
  
  selected_carriers <- reactive(input$airline_selections)
  selected_density <- reactive(input$density_selection)
  selected_year <- reactive(input$years_selection)
  selected_airline <- reactive(filter(airlines_r, description==input$carrier_selection))
  selected_carrier <- reactive(selected_airline()$code)
  selected_dest_year <- reactive(input$years_dest_selection)
  selected_cancel_year <- reactive(input$years_cancel_selection)
  selected_day_year <- reactive(input$day_selection)
  
  output$yearsPlot <- renderPlot ({
    xlim <- c(-171.738281, -56.601563)
    ylim <- c(12.039321, 71.856229)
    pal <- colorRampPalette(c("#f2f2f2", "red"))
    colors <- pal(100)
    map("world", col="#f2f2f2", fill=TRUE, bg="black", lwd=0.05, xlim=xlim, ylim=ylim)
    #map("world", col="#191919", fill=TRUE, bg="#000000", lwd=0.05, xlim=xlim, ylim=ylim)
    year_selected = selected_year()
    flights_count <- flights_tbl %>% filter(year == year_selected) %>%
      group_by(uniquecarrier, origin, dest) %>%
      summarize( count = n()) %>%
      collect    
    flights_count$count <- unlist(flights_count$count)
    fsub <- filter(flights_count, uniquecarrier == selected_carrier(), count > 200)    
    fsub <- fsub[order(fsub$count),]
    maxcnt <- max(fsub$count)
    for (j in 1:length(fsub$uniquecarrier)) {
      air1 <- airports[airports$faa == fsub[j,]$origin,]
      air2 <- airports[airports$faa == fsub[j,]$dest,]
      if (dim(air1)[1] != 0 & dim(air2)[1] != 0) {
        inter <- gcIntermediate(c(air1[1,]$lon, air1[1,]$lat), c(air2[1,]$lon, air2[1,]$lat), n=100, addStartEnd=TRUE)
        colindex <- round( (fsub[j,]$count / maxcnt) * length(colors) )
        
        lines(inter, col=colors[colindex], lwd=0.8)
        lines(inter, col="black", lwd=0.8)
      }
    }
    
  })    
  
  output$densityPlot <- renderPlot ({
    r <- ggplot(delay, aes_string("dist", selected_density())) +
      geom_point(aes(size = count), alpha = 1/2) +
      geom_smooth() +
      scale_size_area(max_size = 2)
    print(r)
  })
  
  output$destPlot <- renderPlot ({
    year_selected <- selected_dest_year()
    flights_by_dest <- flights_tbl %>% filter(year == year_selected) %>%
      filter(dest %in% dests) %>%
      group_by(dest, dayofweek, month, uniquecarrier) %>%
      select(dest, dayofweek, month, uniquecarrier) %>%
      collect
    d <- ggplot(data = flights_by_dest, aes(x = month, fill=dest)) + stat_density()
    r <- ggplot(data = flights_by_dest) + 
      geom_bar(mapping = aes(x = month, fill = dest), position = "dodge")
    print(d)
  })
  
  output$cancelPlot <- renderPlot ({
    c_year_selected <- selected_cancel_year()
    flights_cancelled <- flights_tbl %>%
      filter(year == c_year_selected) %>%
      group_by(dest, month, cancelled) %>%
      summarise(
        count = n(),
        delay = mean(arrdelay, na.rm = TRUE),
        arrdelay_mean = mean(arrdelay, na.rm = TRUE),
        depdelay_mean = mean(depdelay, na.rm = TRUE)
      ) %>%
      filter(count > 20, dest != "HNL", cancelled == 1) %>%
      collect
    
    c <- ggplot(flights_cancelled, aes_string("month", "count")) +
      geom_point(alpha = 1/2, position = "jitter") +
      geom_smooth() +
      scale_size_area(max_size = 2)
    print(c)
  })
  
  output$dayPlot <- renderPlot ({
    year_day_selected <- selected_day_year() 
    flights_by_year <- flights_tbl %>% 
      filter(year== year_day_selected , Dest %in% dests) %>%
      group_by(year, month, dayofmonth, dest) %>%
      summarise(n = n()) %>%
      collect
    
    daily <- flights_by_year %>% 
      mutate(date = make_datetime(year, month, dayofmonth)) %>%
      group_by(date) 
    
    daily <- daily %>% 
      mutate(wday = wday(date, label = TRUE))
    
    d <- ggplot(daily, aes(wday, n, color=dest)) + 
      geom_boxplot() 
    print(d)
  })
}