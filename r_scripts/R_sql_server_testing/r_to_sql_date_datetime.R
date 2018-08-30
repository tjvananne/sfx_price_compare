

#' In this script, I'll be testing dates generated in R and how
#' those translate into date, datetime, and datetime2 in SQL server
#' 
#' Lubridate and SQL Server play very nicely together apparently.


library(base64enc)  # for signing amazon api requests
library(RCurl)
library(digest)
library(XML)
library(dplyr)
library(odbc)
library(DBI)
library(lubridate)
# library(RMySQL)  # switched from MySQL to MS SQL Server



# load in password info -------------------------------------------

amz_secret   <- read.csv("credentials/amz_secret.csv", stringsAsFactors = F)    
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

# create a table for testing date stuff
dbExecute(conn=conn, statement=
    "CREATE TABLE test_date_fields (
        TestDateField date,
        TestDatetimeField datetime,
        TestDatetime2Field datetime2
    );")


# loop through with a sleep timer to add some fake data
for(i in 1:10) {
    print(i)
    # create some fake data
    TestDateField       <- lubridate::today()
    TestDatetimeField   <- lubridate::now()
    TestDatetime2Field  <- lubridate::now()
    
    # attempt to put that fake data into the database
    dbExecute(conn=conn, statement=
    paste0("INSERT INTO test_date_fields
    (TestDateField, TestDatetimeField, TestDatetime2Field)
    VALUES ('", TestDateField, "', '", TestDatetimeField, "', '", TestDatetime2Field, "');"))
    
    dbGetQuery(conn=conn, statement = 
                   "SELECT * FROM test_date_fields;")
    
    Sys.sleep(2)
}


# test out a few different ways to extract data
dbGetQuery(conn=conn, statement = "SELECT * FROM test_date_fields;")
dbGetQuery(conn=conn, statement = "SELECT YEAR(TestDateField) as YEAR FROM test_date_fields;")
dbGetQuery(conn=conn, statement = "SELECT YEAR(TestDatetimeField) as YEAR FROM test_date_fields;")





