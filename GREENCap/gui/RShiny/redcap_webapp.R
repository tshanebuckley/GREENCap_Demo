library(shiny)
library(shinythemes)
library(shinyREDCap)
library (reactable)
library(httr)
library(stringi)
library(jsonlite)
library(DT)
library(reticulate)
library(shinyWidgets)


# Allow for multiple options to be selected per filter

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
        
        actionButton('getFields', 'Get Options'),
        
        uiOutput('formsDrop'),
        
        uiOutput('fieldsDrop'),
        
        uiOutput('armsDrop'),
        
        uiOutput('eventsDrop'),
        
        uiOutput('recordsText'),
        
        # textInput('fields',
        #           label = 'Fields:', ''),
        # 
        # textInput('forms',
        #           label = 'Forms:', ''),
        # 
        # textInput('arm',
        #           label = 'Arm:', ''),
        # 
        # textInput('events',
        #           label = 'Events:', ''),
        # 
        # textInput('pipe',
        #           label = 'Pipe:', ''),
        # 
        # textInput('records',
        #           label = 'Records:', ''),
        
        # Execute fetch
        h3(''),
        
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

  # Basic url for data fetching
  baseurl <- 'http://127.0.0.1:8000/redcap/'
  
  # Reactive string of parameters for url fetch
  querystring <- reactiveVal()
  
  # Parameter specific urls for getting dropdowns
  formsurl <- 'http://127.0.0.1:8000/forms/'
  fieldsurl <- 'http://127.0.0.1:8000/fields/'
  armsurl <- 'http://127.0.0.1:8000/arms/'
  eventsurl <- 'http://127.0.0.1:8000/events/'
  
  
  # Retrieving project name to begin fetching dropdowns
  projecturl <- reactive({
    if (input$project == '')
      return(NULL)
    else
      return(input$project)
    
  })
  
  
  # When user executes data fetch (pressing action button)
  observeEvent(input$getFields, {
    
    
    # Fetch the urls for each parameter and output them
    forms <- content(GET(paste0(formsurl, projecturl())))
    formsOptions <- c('\n')
    for(i in 1:length(forms)){
      formsOptions <- c(formsOptions, forms[i])
    }
    
    output$formsDrop <- renderUI({
      if (length(forms) == 0)
        return(NULL)
      selectInput('forms', 'Forms:', formsOptions)
    })
      
    fields <- content(GET(paste0(fieldsurl, projecturl())))
    fieldsOptions <- c('\n')
    for(i in 1:length(fields)){
      fieldsOptions <- c(fieldsOptions, fields[i])
    }
    
    output$fieldsDrop <- renderUI({
      if (length(fields) == 0)
        return(NULL)
      pickerInput('fields', 'Fields:', fieldsOptions, multiple = FALSE, options = pickerOptions(maxOptions = 10000, liveSearch = TRUE))
    })
    
    # for arms, only get nums as 'type'
    armsNums <- content(GET(paste0(armsurl, projecturl(), '/nums')))
    armsOptions <- c('\n')
    for(i in 1:length(armsNums)){
      armsOptions <- c(armsOptions, armsNums[i])
    }

    output$armsDrop <- renderUI({
      if (length(armsOptions) == 1)
        return(NULL)
      selectInput('arms', 'Arms:', armsOptions)
    })
    
    events <- content(GET(paste0(eventsurl, projecturl())))
    eventsOptions <- c('\n')
    for(i in 1:length(events)){
      eventsOptions <- c(eventsOptions, events[i])
    }
    
    output$eventsDrop <- renderUI({
      if (length(events) == 0)
        return(NULL)
      selectInput('events', 'Events:', eventsOptions)
    })
    
    output$recordsText <- renderUI({
      textInput('records', 'Records:')
    })
  
  })
  
  # Reactively updating filter variables with user input
  
  fieldsPar <- reactive({
    if (input$fields == '\n')
      return(NULL)
    else
      return(paste0('fields=', input$fields))
  })
  formsPar <- reactive({
    if (input$forms == '\n')
      return(NULL)
    else
      return(paste0('forms=', input$forms))
  })
  
  armsPar <- reactive({
    if (input$arms == '\n')
      return(NULL)
    else
      return(paste0('arms=', input$arms))
  })
  
  eventsPar <- reactive({
    if (input$events == '\n')
      return(NULL)
    else
      return(paste0('events=',input$events))
  })
  
  # pipesPar() <- reactive({
  #   if (input$pipe == '')
  #     return(NULL)
  #   else
  #     return(paste0('pipe=',input$pipe))
  # })
  
  recordsPar <- reactive({
    if (input$records == '')
      return(NULL)
    else
      return(paste0('records=', input$records))
  })
  
  # Actions when fetch button is pressed
  observeEvent( input$updateUrl, {
    
    # Creates string with up-to-date filter parameters
    new_query <-
    stri_paste(formsPar(), fieldsPar(), eventsPar(), armsPar(), recordsPar(), sep='&', ignore_null = TRUE)
    
    # Assigns string to reactive string
    querystring(new_query)
    
    
    # fetch finalized data query
    r <- content(GET(paste0(baseurl,projecturl(), '/?',querystring())))
    
    # process json to display as data table
    json_file <- lapply(r, function(x) {
      x[sapply(x, is.null)] <- NA
      unlist(x)
    })
    
    # create dataframe for output
    df <- data.frame(do.call('cbind', json_file))
    
  
    #Output data table to UI
    output$table <- renderTable({
      df
    })

    
    # Download data from table as a .csv
    # file name follows format: 'data-{date of download}.csv'
    output$downloadData <- downloadHandler(
      filename = function(){

        paste("data-", Sys.Date(), ".csv", sep="")
      },
      content = function(file){
        write.csv(df, file)
      }
    )
    
  })
  
  #Output reactive URL to UI
  output$url <- renderText({
    paste0(baseurl,projecturl(), '/?', querystring())
  })
  
}


shinyApp(ui=ui, server=server)