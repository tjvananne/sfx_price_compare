

# this script is to review the items inside of stage table that haven't been fed into the asin list table yet
# I'd like for this script to pull in all records in stage that aren't in list or fact tables
# I'll then create some form of exported xlsx/csv file to manually inspect in order to determine if I want
# to track pricing for that ASIN or not. that file is the output of this script.




list.files("using_API/01_asin_crawler")
source("using_API/01_asin_crawler/sfx_asin_crawler_functions.R")





