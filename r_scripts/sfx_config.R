

# dependencies ---------------------------------------------------------------------------------------------------

library(dplyr)
library(digest)
library(base64enc)
library(lubridate)
library(RCurl)
library(XML)




# credentials --------------------------------------------------------------------------------------------


creds      <- read.csv('credentials/credentials.csv', stringsAsFactors = F)
cred_env   <- creds$env == 'dev'  # <-- change this here to isolate credentials for an environment
db_name    <- creds$db_name[cred_env]
db_user    <- creds$db_user[cred_env]
db_pw      <- creds$db_pw[cred_env]
db_host    <- creds$db_host[cred_env]
db_port    <- creds$db_port[cred_env]
amz_key    <- creds$amz_key[cred_env]
amz_secret <- creds$amz_secret[cred_env]
rm(cred_env, creds)


# database names --------------------------------------------------------------------------------------------


tbl_stage <- "asin_stage01"
tbl_fact  <- "asin_fact01"
tbl_list  <- "asin_list01"



# relative file paths ---------------------------------------------------------------------------------------

path_to_asin_input   <- 'r_scripts/manual_entry_input/manual_selections.csv'
path_to_asin_output  <- 'r_scripts/manual_entry_output/manual_selections.csv'




