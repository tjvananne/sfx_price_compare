
library(DBI)
library(RMySQL)

con <- DBI::dbConnect(RMySQL::MySQL(), 
                      user="taylor", 
                      password="securepw", 
                      host="127.0.0.1", 
                      port=3306, 
                      dbname="amz_price")


