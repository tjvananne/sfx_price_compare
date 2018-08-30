


library(dplyr)
library(odbc)
library(DBI)
library(lubridate)



# load in password info -------------------------------------------
    
    db_secret    <- read.csv("credentials/db_secret.csv", stringsAsFactors = F)
    
    # which environment - create the connection
    db_secret <- db_secret[db_secret$env == "dev", ]
    conn = DBI::dbConnect(odbc::odbc(),
                          Driver = "ODBC Driver 17 for SQL Server",
                          Server = db_secret$server,
                          Database = db_secret$database,
                          UID = db_secret$user_id,
                          PWD = db_secret$password,
                          Port = db_secret$port)

  
# database functions -------------------------------------------------
        
    
db_insert_into_ASIN <- function(p_conn, p_ASIN, p_Cat1, p_Cat2=NULL, p_Cat3=NULL) {
    
    if(!is.null(p_Cat2) & !is.null(p_Cat3)) {
        dbExecute(conn=p_conn, statement=
            paste0("INSERT INTO ASIN (ASIN, Category1, Category2, Category3)
            VALUES ('", p_ASIN, "','", p_Cat1, "','", p_Cat2, "','", p_Cat3, "');"))
    } else if (!is.null(p_Cat2) & is.null(p_Cat3)) {
        dbExecute(conn=p_conn, statement=
            paste0("INSERT INTO ASIN (ASIN, Category1, Category2)
            VALUES ('", p_ASIN, "','", p_Cat1, "','", p_Cat2, "');"))
        
    } else if (is.null & is.null) {
        dbExecute(conn=p_conn, statement=
            paste0("INSERT INTO ASIN (ASIN, Category1)
            VALUES ('", p_ASIN, ");"))
    } else {
        stop("An error occurred")
    }
    
}

    
    
# will have to figure something out about Reverb/Delay combo pedals...
db_insert_into_ASIN(conn, "B074VDY8FM", "Guitar Pedal", "Reverb")

dbGetQuery(conn=conn, "SELECT * FROM ASIN")
    
    
# NOTES -------------------------------------------------------------------------------
    
    
#' I should write a shiny Gadget "Viewer" pain shiny app for inputing these ASINs...
#' It would warn me whenever I tried to input one that was already in the database. "Fail gracefully"
#' 
#' I could then work on packaging it up into an electron desktop application that had a full
#' admin-panel page for connecting to a database and populating it with values in the manual
#' entry database tables.
#' 
#' For the desktop version, I would have to just create a very hardcoded thing to look for
#' code updates. If the github latest hash value has changed, it will automatically update
#' when the user next opens the app. (how to know exactly how many scripts we'll need to 
#' check for? Will we ever need additional scripts? Can we make it flexible enough to 
#' know that it will need to check for additional scripts in the future? Can it just check
#' for all scripts in a certain directory? Run, load, source all of those scripts in that
#' one directory, then it becomes flexible.) The desktop version would just utilize APIs that
#' the web and mobile version would also be "hitting" to get their data and responses and html
#' to render. This may require some native wrapper packages for Android and iOS.
#' 
#' Using electron JS to build a shiny desktop application
#' https://www.youtube.com/watch?v=O56WR-yQFC0&index=2&t=16s&list=LL9Nk6lDyvEIT0cUx9odwBHg
#' Build a page of this app that will edit it's own config file, then when you close
#' and restart the app next, it will pick up the config changes and adapt.
#' 
#' If you build a product:
#' 
#' https://github.com/ficonsulting/RInno  (repo specifically for creating desktop apps from r shiny)
#' This would be able to allow for user login, then check a database to see if the user is active 
#' (has paid) within the last 30 days (like 50c) and not let them on unless they're active.
#' 
#' If you build this product and let it be free, and use it to collect anonymous data on the
#' users (or maybe not anonymous if they consent), you could use that data to build very
#' interesting models. You could start tapping into the data that giants like Amazon already
#' have. Become the middle man that people turn to before visiting Amazon, or Guitar center, or
#' wherever. Tap into the stream of people who are price consious, but don't have the time or
#' tech savvy to track prices effectively on their own (without maybe purchasing a product or
#' using a site with excessive advertisements and creepy cookie agreements and bad UI).
#' 
#' This is an area ripe for differentiation.
#' 
#' Build it for free, collect data, build machine learning models on user behavior (with
#' their consent). People aren't creeped out by innocent recommendations anymore. They've
#' come to expect it with many of their digital products now. Facebook suggests new friends.
#' Amazon suggests similar products based on your behaviour in the past. Google stores cookies
#' based on your queries that can then show up as Google-based ads on various websites for 
#' the exact item you queried hours before.
#' 
#' 
#' Build in a community ASIN entering box. Show them how to find the product's ASIN. Show
#' them how to copy it into the box and press "ENTER". Tell them what an error message
#' would look like if they tried to enter an ASIN that already exists in our database
#' tables. If it isn't, do some obvious checks to make sure it actually is a possible
#' ASIN value. If so, pass their ASIN on to a query to the product advertising API and
#' look at it's results. If it doesn't appear to be a valid AMAZON-returned "type" or
#' category or whatever, then cache that result into one of our tables and tell the user
#' that the product will need to be inspected by a member of the SFX team.
#' 
#' Upon inspection, we'll manually make the decision to add it to the actual table that
#' is used for price checking. Otherwise, it will only live in our ASIN / type cache. 
#' eventually, it'll be really cool/interesting to have a large quantity of ASINs and
#' their types cached. 
    
    
