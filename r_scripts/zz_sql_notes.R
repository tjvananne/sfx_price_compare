

library(odbc)
library(DBI)


# load in password info -------------------------------------------

db_secret    <- read.csv("credentials/db_secret.csv", stringsAsFactors = F)
db_secret <- db_secret[db_secret$env == "dev", ]
conn = DBI::dbConnect(odbc::odbc(),
                      Driver = "ODBC Driver 17 for SQL Server",
                      Server = db_secret$server,
                      Database = db_secret$database,
                      UID = db_secret$user_id,
                      PWD = db_secret$password,
                      Port = db_secret$port)


# common constraints used for fields in a database:
# NOT NULL
# UNIQUE
# PRIMARY KEY
# FOREIGN KEY
# CHECK
# DEFAULT


ASIN_data <- dbGetQuery(conn, statement = "SELECT * FROM ASIN2;")
# write.csv(ASIN_data, "ASIN_data_2018_09_02.csv", row.names = F)

# drop a table
dbExecute(conn, statement = "DROP TABLE ASIN2;")


# list all tables in database:
dbGetQuery(conn, statement = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';")


# list all constraints in the database:
dbGetQuery(conn, statement = "SELECT * FROM sys.objects WHERE type_desc like '%CONSTRAINT';")



# list all table relationships from the perspective of the foreign keys
dbGetQuery(conn, statement = 
"SELECT
    fk.name 'FK Name',
    tp.name 'Parent table',
    cp.name, cp.column_id,
    tr.name 'Refrenced table',
    cr.name, cr.column_id
FROM 
    sys.foreign_keys fk
INNER JOIN 
    sys.tables tp ON fk.parent_object_id = tp.object_id
INNER JOIN 
    sys.tables tr ON fk.referenced_object_id = tr.object_id
INNER JOIN 
    sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
INNER JOIN 
    sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
INNER JOIN 
    sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
ORDER BY
    tp.name, cp.column_id;")


library(dplyr)
ASIN <- dbGetQuery(conn, "SELECT * FROM ASIN;")
ASIN_Category <- dbGetQuery(conn, "SELECT * FROM ASIN_Category;")

ASIN_Category <- rename(ASIN_Category, ASIN_Category=ASIN)

ASIN_ASIN_Category_join <- merge(
    x=ASIN_Category,
    y=ASIN,
    by="ASIN_id",
    all.x=T,
    all.y=T
)



# NOTES -------------------------------------------------------------------------------

#' you could add machine learning for tagging Category1 and Category2 values. I think
#' Category2 would be most interesting, since Category1 will be "Guitar Pedals" for the
#' foreseeable future unless I have other pet projects I'd like to spin off of this (GPUs?)
#' 
#' Also, give people a place to "attach" YouTube video reviews to the pedals "details" page.
#' enable up/down-votes for those who actually register for the site.
#' 


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






