
#' In this script, I'll be testing how to handle slowly changing dimensions
#' with transactions in R
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
