library(shiny)
library(shinythemes)
library(shinyREDCap)
library (reactable)
library(httr)
library(stringi)
library(jsonlite)
library(DT)
library(reticulate)

# Setting up UI
ui <- fluidPage(theme = shinytheme('cerulean'),
  navbarPage(
    
    "REDCap App (V1.0)",
    
    # Data fetching and pipeline running tab
    tabPanel("Data Fetch & Pipeline Runner",
      
      # Filter sidebar
      sidebarPanel(
        
        tags$h3('Filters:'),
        
        helpText('View and export demographic information as filtered.'),
        
        textInput('project',
                  label = 'Projects:', ''),
        
        textInput('fields',
                  label = 'Fields:', ''),
        
        textInput('forms',
                  label = 'Forms:', ''),
        
        textInput('arm',
                  label = 'Arm:', ''),
        
        textInput('events',
                  label = 'Events:', ''),
        
        textInput('pipe',
                  label = 'Pipe:', ''),
        
        textInput('records',
                  label = 'Records:', ''),
        
        # Execute fetch
        actionButton('updateUrl', 'Analyze Output'),
        
        # Outputted URL
        h3('Current URL:'),
        
        verbatimTextOutput('url')
        
      ),
      
      # Displays resulting data table from filters
      mainPanel(
  
        h1('Data Viewer:'),
        
        tableOutput('table'),
        
        # Download data table as csv
        downloadButton('downloadData', 'Download')
      )
    ),
    
    # Project adding tab
    tabPanel(
      "Add a Project",
      
      sidebarPanel(
        
        # User requirements for adding a project to REDCap database
        tags$h3('Project Details:'),
        
        helpText('Add a project to REDCap. Provide project name and API information.'),
        
        textInput('new_proj', 'Project Name:', ''),
        
        textInput('new_URL', 'Project URL:', ''),
        
        textInput('new_token', 'Project Token:', ''),
        
        # Execute adding a project
        actionButton('add_Proj', 'Add Project')
        
      ),
      
      mainPanel(
        
        # Display success/error in adding project
        h1('Status:'),
        
        verbatimTextOutput('status')
      )
    ),
    
    # Adding a pipe tab
    # TODO: Implement Pipe adding functionality
    tabPanel(
      
      "Add a Pipe",
      
      textAreaInput('pipecode', 'Code for Pipes:', width='1000px', height = '500px'),
      
      verbatimTextOutput('code'),
      
      downloadButton('downloadPipe', 'Download Pipe')
    )
  )
)



# Setting up server
server <- function(input, output) {

  # Basic url for data fetching without parameters
  baseurl <- 'http://127.0.0.1:8000/redcap/'
  
  # Reactive string of parameters for url fetch
  querystring <- reactiveVal()
  
  
  # Reactively updating filter variables with user input
  projecturl <- reactive({
    if (input$project == '')
      return(NULL)
    else
      return(paste0(input$project, '/?'))
      
  })
  fieldsurl <- reactive({
    if (input$fields == '')
      return(NULL)
    else
      return(paste0('fields=', input$fields))
  })
  formsurl <- reactive({
    if (input$forms == '')
      return(NULL)
    else
      return(paste0('forms=', input$forms))
  })
  armurl <- reactive({
    if (input$arm == '')
      return(NULL)
    else
      return(paste0('arm=', input$arm))
  })
  eventsurl <- reactive({
    if (input$events == '')
      return(NULL)
    else
      return(paste0('events=',input$events))
  })
  pipeurl <- reactive({
    if (input$pipe == '')
      return(NULL)
    else
      return(paste0('pipe=',input$pipe))
  })
  recordsurl <- reactive({
    if (input$records == '')
      return(NULL)
    else
      return(paste0('records=', input$records))
  })
  
  # When user executes data fetch (pressing action button)
  observeEvent(input$updateUrl, {
    
    # Creates string with up-to-date filter parameters
    new_query <-
      stri_paste(recordsurl(), armurl(), eventsurl(), fieldsurl(), pipeurl(), formsurl(), sep='&', ignore_null = TRUE)
    
    # Assigns string to reactive string
    querystring(new_query)
    
    # Fetch the url with query string
    r <- content(GET(paste0(baseurl, projecturl(), querystring())))
    
    # Read fetched url content as a csv
    table <- read.csv(text=r)
    
    # Output data table to UI
    output$table <- renderTable({
      table
    })
    
    # Download data from table as a .csv
    output$downloadData <- downloadHandler(
      filename = function(){
        # file name follows format: 'data-{date of download}.csv'
        paste("data-", Sys.Date(), ".csv", sep="")
      },
      content = function(file){
        write.csv(table, file)
      }
    )
  
  })
  
  # Output reactive URL to UI
  output$url <- renderText({
    paste0(baseurl, projecturl(), querystring())
  })
  
}


shinyApp(ui=ui, server=server)