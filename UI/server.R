library(shiny)
library(leaflet)
library(RColorBrewer)


# Define server logic required to draw a histogram
shinyServer(function(input, output) { 
    points = read.csv("plot.csv")
    output$map <- renderLeaflet({
      pal <- colorFactor("YlGnBu", points$group, n=7)
      
      
      leaflet(data = points) %>% addTiles() %>%
        setView(lng = -99.1579, lat = 30.0376, zoom = 10)%>%
        addWMSTiles(
          "http://basemap.nationalmap.gov/arcgis/services/USGSHydroNHD/MapServer/WMSServer?",
          layers = "0",
          options = WMSTileOptions(format = "image/png", transparent = TRUE),
          attribution = "") %>%
        #fitBounds(-92, 29, -99, 31)%>%
        addCircleMarkers(~longitude, ~latitude, weight = 1, color = pal(points$group), fillColor =  pal(points$group),fillOpacity = 1)%>%
        addLegend("bottomright", pal = pal, values = ~points$group,
                  title = "TCEQ Priority Group",
                  labFormat = labelFormat(prefix = "Group"),
                  opacity = 1)
    })
    output$preImage <- renderImage({
      # When input$n is 3, filename is ./images/image3.jpeg
      filename <- normalizePath(file.path('./images',
                                          paste('picture1', '.png', sep='')))
      list(src = filename,
           contentType = 'image/png',
           width = 500,
           height = 500
          )
    }, deleteFile = FALSE)
  
      
  }
)