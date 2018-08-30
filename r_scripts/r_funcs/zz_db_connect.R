
#library(RMySQL)
library(odbc)
library(DBI)

con <- DBI::dbConnect(odbc(),
                      Driver = "ODBC Driver 17 for SQL Server",
                      Server = "sfx-amz.database.windows.net",
                      Database = "sfx-amz-dev",
                      UID = id,
                      PWD = pw,
                      Port = 1433)


