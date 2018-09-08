


library(base64enc)  # for signing amazon api requests
library(RCurl)
library(digest)
library(XML)
library(dplyr)
library(odbc)
library(DBI)
library(lubridate)
library(futile.logger)  # TODO: add logging capability so we have more insight into jobs run on remote server



# notes / next steps ----------------------------------------------

# I need to make test cases for any data I believe the Amazon API is 
# capable of returning to me. I'd like for these test cases to mimic
# really using the Amazon API, but not actually make calls to it or
# rely on the web at all. I would also like for these tests to not 
# rely on the database being up and running as well. The tests will
# be developed with the knowledge of what pain points look like between
# R and SQL Server, but will be developed in a way that looks like
# the "plugin" architecture.


# load in db and amazon credentials ------------------------------
    
amz_secret   <- read.csv("credentials/amz_secret.csv", stringsAsFactors = F)    
db_secret    <- read.csv("credentials/db_secret.csv", stringsAsFactors = F)

source("r_scripts/lib_name_db_tables_by_env.R")


# which environment - create the connection
db_secret <- db_secret[db_secret$env == GBL_env, ]
conn = DBI::dbConnect(odbc::odbc(),
                      Driver = "ODBC Driver 17 for SQL Server",
                      Server = db_secret$server,
                      Database = db_secret$database,
                      UID = db_secret$user_id,
                      PWD = db_secret$password,
                      Port = db_secret$port)


    
# Helper functions ------------------------------------------------

# these helper functions are primarily for logic to be used inside
# of the calls to the amazon api.

# helper timestamp function
generate_utc_timestamp_ <- function(ts) {
    # x1: timestamp in current system timezone
    # x2: format with tz argument changes the timezone to GMT/UTC
    # x3: lubridate objects are easier to work with and manipulate
    x1 <- as.POSIXct(ts)
    x2 <- format(x1, tz = "GMT", usetz = F)
    x3 <- lubridate::ymd_hms(x2)
    
    # reformat the string manually (kinda sloppy, but lots of control)
    return(paste0(
        sprintf("%04d", lubridate::year(x3)),   '-', 
        sprintf("%02d", lubridate::month(x3)),  '-',
        sprintf("%02d", lubridate::day(x3)),    'T', 
        sprintf("%02d", lubridate::hour(x3)),   ':',
        sprintf("%02d", lubridate::minute(x3)), ':', 
        sprintf("%02d", lubridate::second(x3)), 'Z'))
}

    # # Unit tests:
    # # pass in "Sys.time()" format time stamp
    # # output is compatible with lubridate's ymd_hms
    # Runit_test <- generate_utc_timestamp_(Sys.time())
    # lubridate::ymd_hms(generate_utc_timestamp_(Sys.time()))



# is this inefficient?
time_since_epoch <- function() {
    library(lubridate)
    x1 <- as.POSIXct(Sys.time())
    x2 <- format(x1, tz="GMT", usetz=F)
    x3 <- lubridate::ymd_hms(x2)
    epoch <- lubridate::ymd_hms('1970-01-01 00:00:00')
    time_since_epoch <- (x3 - epoch) / dseconds()
    return(time_since_epoch)
}

    # # Unit tests:
    # # returns seconds since epoch (midnight 1-1-1970)
    # # function is not efficient, but we're only 
    # # interested in accuracy/reliability, not efficiency
    # time_since_epoch()




    
# Amazon functions ---------------------------
    
# I've split the responsibility of requesting data and
# parsing the response of that request into two separate
# functions. 

# build and send request to amz api for product searching - default keyword is "guitar pedal"
amz_itemlookup_request <- function(p_access_key, p_secret, p_associatetag, p_ASIN, p_responsegrp="ItemAttributes",
                                   p_endpoint="webservices.amazon.com", p_uri="/onca/xml") {
    #' p_access_key: amazon access key for product advertising API
    #' p_secret: amazon secret key for product advertising API
    #' p_associatetag: your store-id or a different amazon associate tag associated with your affiliate account
    #' p_ASIN: the ASIN (amazon id) for the product you're requesting information for
    #' p_responsegrp: the type of information you want about this product
    #' p_endpoint: just keep the default.
    #' p_uri: just keep the default.
    #' NOTES: 
    
    # # debugging:
    # # browser()
    # p_access_key = amz_secret$amz_key
    # p_secret = amz_secret$amz_secret
    # p_associatetag = amz_secret$amz_tag
    # p_ASIN = "B004OK17QS"
    # 
    # p_responsegrp="ItemAttributes"
    # p_endpoint="webservices.amazon.com"
    # p_uri="/onca/xml"
    
    # create a timestamp
    request_timestamp <- time_since_epoch()
    request_datetime  <- lubridate::now()
    
    # parameters of the query 
    params <- list(
        "AWSAccessKeyId" = p_access_key,
        "AssociateTag"   = p_associatetag,
        "ItemId"         = p_ASIN,
        "Operation"      = "ItemLookup",
        "ResponseGroup"  = p_responsegrp,
        "Service"        = "AWSECommerceService")
    
    # attach timestamp after setting up parameters
    params$Timestamp = generate_utc_timestamp_(Sys.time())
    params$Version   = "2013-08-01"
    
    # url encode all parameters, paste them together with "=", collapse with "&"
    params_url_encoded <- lapply(params, URLencode, reserved=T)
    paired_params      <- paste0(names(params_url_encoded), '=', params_url_encoded)
    canonical_str_req  <- paste0(paired_params, collapse="&")
    
    print(canonical_str_req)
    
    # construct the query manually
    str_to_sign <- paste0('GET\n', p_endpoint, "\n", p_uri, "\n", canonical_str_req)
    
    # hash the string with secret key and return raw, then encode in base64 (just made this match php code snip)
    signature_raw <- digest::hmac(key = p_secret, object=str_to_sign, algo="sha256", raw = TRUE)
    signature64   <- base64enc::base64encode(signature_raw)
    
    # build final request:
    fin_req <- paste0(
        "http://", p_endpoint, p_uri, "?", canonical_str_req, "&Signature=",
        URLencode(signature64, reserved = T))
    
    
    # send the request with a specific user agent string
    raw_response <- RCurl::getURL(fin_req, httpheader = 
        c('User-Agent' = paste0("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ", 
                                "(KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36")))
    
    # wrap up in a return list
    return_list <- list(
        raw_response = raw_response,
        timestamp    = request_timestamp,
        datetime     = request_datetime,
        asin         = p_ASIN)
    
    return(return_list)
}



# parse the entire itemsearch response
amz_itemlookup_parse <- function(p_itemsearch_response_list) {
    # p_itemsearch_response_list: list returned from amz_itemsearch_request
        # [[1]] this is the raw response from amazon
        # [[2]] this is the utc epoch timestamp at the time the request was signed
        # [[3]] this is the datetime at the time the request was signed
        # [[4]] the asin that was looked up
    
    p_itemsearch_response <- p_itemsearch_response_list[[1]]
    timestamp             <- p_itemsearch_response_list[[2]]
    datetime              <- p_itemsearch_response_list[[3]]
    this_ASIN             <- p_itemsearch_response_list[[4]]
    
    # parse, root, check user agent
    parse_response <- XML::xmlTreeParse(p_itemsearch_response)
    r  <- XML::xmlRoot(parse_response)
    UserAgent <- r[['OperationRequest']][['HTTPHeaders']][['Header']] %>% xmlGetAttr("Value")
    
    # short circuit the function for debugging purposes...
    # return(r)   
    
    # isolate the items, then we can iterate through them in a loop
    r_Items          <- r[['Items']][names(r[['Items']]) == 'Item']
    r_Item           <- r_Items[["Item"]]
    r_ItemAttributes <- r_Item[["ItemAttributes"]]
    
    
    results_df <- data.frame(
        
        # items from amz_itemsearch_request function
        EffTimestampUTC = timestamp,
        DateTime        = datetime,
        ASIN            = this_ASIN,
        
        # from Item Attributes
        Brand           = xmlValue(r_ItemAttributes[["Brand"]]), 
        ListPrice       = xmlValue(r_ItemAttributes[["ListPrice"]][["Amount"]]), 
        ListPriceCd     = xmlValue(r_ItemAttributes[["ListPrice"]][["CurrencyCode"]]),
        
        # from Offer Summary
        LowestNewPrice      = xmlValue(r_Item[["OfferSummary"]][["LowestNewPrice"]][["Amount"]]),
        LowestNewPriceCd    = xmlValue(r_Item[["OfferSummary"]][["LowestNewPrice"]][["CurrencyCode"]]),
        LowestUsedPrice     = xmlValue(r_Item[["OfferSummary"]][["LowestUsedPrice"]][["Amount"]]),
        LowestUsedPriceCd   = xmlValue(r_Item[["OfferSummary"]][["LowestUsedPrice"]][["CurrencyCode"]]),
        LowestRefurbPrice   = xmlValue(r_Item[["OfferSummary"]][["LowestRefurbishedPrice"]][["Amount"]]),
        LowestRefurbPriceCd = xmlValue(r_Item[["OfferSummary"]][["LowestRefurbishedPrice"]][["CurrencyCode"]]),
        
        # from Item Attributes
        Manufacturer    = xmlValue(r_ItemAttributes[["Manufacturer"]]), 
        Model           = xmlValue(r_ItemAttributes[["Model"]]), 
        MPN             = xmlValue(r_ItemAttributes[["MPN"]]),
        NumberOfItems   = xmlValue(r_ItemAttributes[["NumberOfItems"]]),
        PackageQuantity = xmlValue(r_ItemAttributes[["PackageQuantity"]]),
        PartNumber      = xmlValue(r_ItemAttributes[["PartNumber"]]),
        ProductTypeName = xmlValue(r_ItemAttributes[["ProductTypeName"]]),
        Title           = xmlValue(r_ItemAttributes[["Title"]]),
        UPC             = xmlValue(r_ItemAttributes[["UPC"]]),
        Warranty        = xmlValue(r_ItemAttributes[["Warranty"]]),
        stringsAsFactors = F)
    
    return(results_df)
}

# # Unit test
# test_item_attributes <- amz_itemlookup_request(
#     p_access_key = amz_secret$amz_key,
#     p_secret = amz_secret$amz_secret,
#     p_associatetag = amz_secret$amz_tag,
#     p_ASIN = "B004OK17QS")
# test_item_attributes_parsed <- amz_itemlookup_parse(test_item_attributes)
# names(test_item_attributes_parsed)



    
    
# database functions -------------------------------------------------
        

# replacement for sqlAppendTable
# I had to implement this myself, but DBI's sqlAppendTable() function has issues
db_sql_append_table <- function(p_df, p_tbl) {
    # p_df: data.frame that contains the data to append/insert into the table
    # the names must be the same as those in the database
    # p_tbl: the name of the database table to insert/append into
    
    # browser()
    p_df <- data.frame(p_df, stringsAsFactors = F)
    num_rows <- nrow(p_df)
    num_cols <- ncol(p_df)
    # requires_quotes <- sapply(p_df, class) %in% c("character", "factor")
    requires_quotes <- sapply(p_df, function(x) {
        any(class(x) %in% c("character", "factor", "POSIXct", "POSIXt"))
    })
    # changed "requires_quotes" logical vector to include date-types (POSIXct / POSIXt)
    # these require 'single quotes' for database inserts.
    
    # remove any single quotes within the content of character fields
    p_df_col_names <- names(p_df)
    for(i in 1:length(p_df_col_names)) {
        if(class(p_df[[p_df_col_names[i]]])[[1]] %in% c("character")) {
            p_df[p_df_col_names[i]] <- gsub("'", "", p_df[p_df_col_names[i]])
        } 
    }
    
    commas <- rep(", ", num_rows)
    quotes <- rep("'", num_rows)
    
    str_columns <- ' ('
    column_names <- names(p_df)
    for(i in 1:num_cols) {
        
        if(i < num_cols) {
            str_columns <- paste0(str_columns, column_names[i], ", ")
        } else {
            str_columns <- paste0(str_columns, column_names[i], ") ")
        }
    }
    
    
    str_query <- paste0("INSERT INTO ", p_tbl, str_columns, "\nVALUES\n")   
    str_values <- rep("(", num_rows)
    
    
    for(i in 1:num_cols) {
        
        # not the last column in the insert statement...
        if(i < num_cols) {
            
            if(requires_quotes[i]) {
                # this column requires quotes, add them before and after the value
                str_values <- mapply(paste0, str_values, quotes, p_df[[column_names[i]]], quotes, commas)        
            } else {
                # doesn't require quotes...
                str_values <- mapply(paste0, str_values, p_df[[column_names[i]]], commas)
            }
            
            # this IS the last column in the insert statement - end with close paren
        } else {
            if(requires_quotes[i]) {
                # this column requires quotes, add them before and after the value
                str_values <- mapply(paste0, str_values, quotes, p_df[[column_names[i]]], quotes, ")")
            } else {
                # doesn't require quotes
                str_values <- mapply(paste0, str_values, p_df[[column_names[i]]], ")")
            }
        }
    }
    
    # build out the entire query from the pieces we've built so far
    str_values <- paste0(str_values, collapse=",\n")
    str_query <- paste0(str_query, str_values)
    str_query <- paste0(str_query, ";")
    
    return(str_query)
    
}


# Execution Notes ------------------------------------------------------------------------
    
#' Steps
#' 1. Read from ASIN table
#' 2. pass i'th ASIN into the product API and parse
#' 3. over-write the Amz_Product table with the freshest dimension data available
#'     - (eventually keep slowly changing dimensions here? - not sure how interesting that would be)
#'     - Also eventually track the "features" from these products. I want to enable text analytics or
#'       at least text searching with SolR or something.
#' 4. query Amz_ListPrice (and all other price fact tables) where Is_Active flag is a 1
#'     (might make more sense to query this table once for all active records instead of
#'     once for each individual ASIN...)
#' 5. compare values for the field that that fact table records. New list price vs existing active
#'    list price. New used price vs existing used price. We can start with list price and 
#'    implement these one at a time. Ideally, there would be one function that can dynamically
#'    handle any of these comparisons
#'        - this comparison should be able to handle empty values in the Amz_ListPrice / price tables
#' 6. If a value has changed (is different from the existing Is_Active == 1 record), then
#'    begin a transaction: set existing Is_Active flag to 0, insert the new row, set the
#'    new Is_Active flag to 1, commit the transaction
#'        - Could eventually write some stored procedures for this, but for now I'd like all
#'          logic to live in one place so I can maintain it here.
    
    
# These steps are missing the ASIN -> ASIN_id surrogate key lookups
# ASIN_id is what ties all the tables together, but ASIN is the natural
# key that will be used to lookup the ASIN_id
# I've included the natural key in all tables that use ASIN_id to make
# table resets a little easier (at least while actively developing)
    

# Execution Loop ------------------------------------------------------------------------
    

# this giant for loop is mildly ridiculous. let's think about breaking this into smaller
# functions for easier testing.
    
        
# query the ASIN table
tbl_ASIN <- dbGetQuery(conn = conn, statement = paste0("SELECT * FROM ", GBL_tbl_name_ASIN, ";"))

    
# I don't think this is necessary at this point, but just doing it to inspect
tbl_Amz_Product <- dbGetQuery(conn = conn, statement = paste0("SELECT * FROM ", GBL_tbl_name_Amz_Product, ";"))
    
# query the Amz_ListPrice table for all active records (will be blank first time)
tbl_Amz_ListPrice <- dbGetQuery(conn = conn, statement = 
    paste0(
        "SELECT * FROM ", GBL_tbl_name_Amz_ListPrice,  
        " WHERE ListPrice_IsActive = 1;"))

tbl_Amz_ListPrice_all <- dbGetQuery(conn, statement = "SELECT * FROM prod_Amz_ListPrice;") %>%
    arrange(ASIN_id, ListPrice_Effdt)


            # AD HOC DOWN SAMPLE TO TARGET SPECIFIC PROBLEM CHILDREN ASINs;
# tbl_ASIN <- tbl_ASIN[tbl_ASIN$ASIN_id %in% (tbl_Amz_ListPrice$ASIN_id[is.na(tbl_Amz_ListPrice$ListPrice)]),]



# begin the loop here to pass ASINs into the Amz API, compare with existing active records, and update as necessary
for(i in 1:nrow(tbl_ASIN)) {
    print(paste0("--------------->  ", i, "  <---------------"))
    
    
    # isolate ASIN and ASIN_id for this request
    this_ASIN      <- tbl_ASIN$ASIN[i]
    this_ASIN_id   <- tbl_ASIN$ASIN_id[i]
    this_DateTime2 <- lubridate::now() 

    
    # request and parse data
    print("Querying Amazon API for fresh data...")
    this_ASIN_API_data <- amz_itemlookup_request(
            p_access_key   = amz_secret$amz_key,
            p_secret       = amz_secret$amz_secret,
            p_associatetag = amz_secret$amz_tag,
            p_ASIN         = this_ASIN) %>%
        amz_itemlookup_parse() %>%
        mutate(ASIN_id = this_ASIN_id,
               DateTime2 = this_DateTime2)
    
    
    # Amz_Product table logic -------------------------------------------------------------
    
    this_Amz_Product_data <- this_ASIN_API_data %>%
        select(
            ASIN_id,
            Product_ASIN = ASIN,
            Product_DateTime = DateTime2,
            Product_Title = Title,
            Product_Brand = Brand,
            Product_Manufacturer = Manufacturer,
            Product_Model = Model,
            Product_MPN = MPN,
            Product_NumberOfItems = NumberOfItems,
            Product_PartNumber = PartNumber,
            Product_TypeName = ProductTypeName,
            Product_UPC = UPC) 
    
    # remove columns that have an NA value - this is how to "insert a NULL" into sql server
    Product_table_col_names <- names(this_Amz_Product_data)
    for(j in 1:length(Product_table_col_names)) {
        # print(i)
        if(is.na(this_Amz_Product_data[Product_table_col_names[j]])) {
            this_Amz_Product_data[Product_table_col_names[j]] <- NULL
        }
    }; rm(j) 
    
    
    
    # if this ASIN is already in the Amz_Product table
    if(this_ASIN_id %in% tbl_Amz_Product$ASIN_id) {
        
        # in transaction: then delete the row and insert again with fresh data from API
        # this is not slowly changing dimension, this is an overwriting dimension
        print("Executing Product table kill-and-fill for this ASIN...")
        dbWithTransaction(conn, code = {
            dbExecute(conn, statement = paste0("DELETE FROM ", GBL_tbl_name_Amz_Product, " WHERE ASIN_id = ", this_ASIN_id, ";"))
            dbExecute(conn, statement = db_sql_append_table(this_Amz_Product_data, GBL_tbl_name_Amz_Product))
        })
    } else {
        
        # if this ASIN_id isn't already in the data, then just insert
        print("ASIN wasn't found in product table, inserting it now...")
        dbExecute(conn, statement = db_sql_append_table(this_Amz_Product_data, GBL_tbl_name_Amz_Product))
    }
    
    # Amz_ListPrice logic ----------------------------------------------------------------
    
    
    # convert the data from Amz API to what our database table expects
    this_Amz_ListPrice_data <- this_ASIN_API_data %>%
        select(
            ASIN_id,
            ASIN,
            ListPrice_Effdt = DateTime,
            ListPrice       = ListPrice) %>%
        mutate(ListPrice_IsActive = 1)
    
    
    # remove columns that have an NA value
    ListPrice_table_col_names <- names(this_Amz_ListPrice_data)
    for(j in 1:length(ListPrice_table_col_names)) {
        # print(i)
        if(is.na(this_Amz_ListPrice_data[ListPrice_table_col_names[j]])) {
            this_Amz_ListPrice_data[ListPrice_table_col_names[j]] <- NULL
        }
    }; rm(j)
    
    
    
    if(this_ASIN_id %in% tbl_Amz_ListPrice$ASIN_id) {
        # the ASIN_id exists already in the ListPrice table
        
        
        # this_ListPrice can be NA but not NULL (NULLs can't exist in "cell" of R data.frame)
        this_ListPrice <- tbl_Amz_ListPrice$ListPrice[tbl_Amz_ListPrice$ASIN_id == this_ASIN_id][[1]]
        
        
        
        # if both are missing, nothing changed. If one is missing, something changed. 
        # if neither are missing, then compare the values to see if something changed.
        # Note: this_ListPrice comes from R querying SQL so it has NA instead of Null. 
        #       but this_Amz_ListPrice_data$ListPrice is NULL because we removed all
        #       columns that had NA values above. j was the index for that loop.
        print("ASIN already exists in Amz_ListPrice table - checking if price changed since last request...")
        count_missing_ListPrice <- sum(is.na(this_ListPrice), is.null(this_Amz_ListPrice_data$ListPrice))
        if(count_missing_ListPrice == 1) {
            something_changed <- TRUE      
        } else if(count_missing_ListPrice == 2) {
            something_changed <- FALSE     
        } else if(count_missing_ListPrice == 0) {
            something_changed <- as.integer(this_ListPrice) != as.integer(this_Amz_ListPrice_data$ListPrice)
        }
        
        
        
        if(something_changed) {
            # The price we got from the API is different than what is "Active" in the database table
            print("Price has changed, setting old active record to inactive, inserting this new record...")
            dbWithTransaction(conn, code = {
                dbExecute(conn, statement = 
                    paste0(
                        "UPDATE ", GBL_tbl_name_Amz_ListPrice,
                        " SET ListPrice_IsActive = 0
                        WHERE ASIN_id = ", this_ASIN_id, ";"))
                dbExecute(conn, statement = db_sql_append_table(p_df = this_Amz_ListPrice_data, p_tbl = GBL_tbl_name_Amz_ListPrice))
            })
            
            
            
        } else {
            print("Price hasn't changed for this product, no insertion required...")
        }
        
        
        
        # don't want this_ListPrice to have a chance to be defined for other loop iterations
        rm(this_ListPrice)
        
        
        
    } else {
        # the ASIN_id does not exist in this table, just do a simple insert.
        print("ListPrice record didn't exist, just insert a row...")
        dbExecute(conn, statement = db_sql_append_table(p_df = this_Amz_ListPrice_data, p_tbl = GBL_tbl_name_Amz_ListPrice))
    }
    
    
    
    # now sleep for a random amount of time between 2-6 seconds
    sleep_dur <- round(runif(1, min=2, max=6), 2)
    print(paste0("Done. Sleeping for ", sleep_dur, " seconds..."))
    Sys.sleep(sleep_dur)
}

    
