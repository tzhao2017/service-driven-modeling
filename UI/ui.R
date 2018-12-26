library(shiny)
library(leaflet)
library(RColorBrewer)

# Define UI for application that draws a histogram
shinyUI(
  fluidPage(
    
#     tags$style(type = "text/css", "html, body {width:100%;height:100%}"),
#     leafletOutput("map", width = 400, height = 300),
    titlePanel("Real-time Decision Support Services for Water Allocation"),
      sidebarLayout(
        sidebarPanel(
          dateInput("date", 
                    label = h3("Water Request Date"), 
                    value = "2015-03-10"),
          dateRangeInput("dates", label = h3("RAPID Model Period")),
          selectInput("select", label = h3("Scenarios"), 
                      choices = list("Real Scenario" = 1, "Historical High Runoff Scenario" = 2,
                                     "Drier Scenario (-20%)" = 3), selected = 1),
          #imageOutput("tukey", height = 200),
          submitButton("Submit")
        ),
        mainPanel(
          tabsetPanel(type = "tabs", 
                      tabPanel("WaterUsers", leafletOutput("map")), 
                      tabPanel("WaterAllocationRecommendation", imageOutput("preImage"))
          )
          )
  )
  )
)