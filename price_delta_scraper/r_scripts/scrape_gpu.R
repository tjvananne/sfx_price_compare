

library(XML)
library(dplyr)
library(openxlsx)
library(DBI)
library(RMySQL)
library(lubridate)



# load in password info -------------------------------------------
    
    list.files("secret")
    amz_secret   <- read.csv("secret/amz_secret.csv", stringsAsFactors = F)    
    mysql_secret <- read.csv("secret/mysql_secret.csv", stringsAsFactors = F)
    
    
    # which environment?
    mysql_secret <- mysql_secret[mysql_secret$mysql_env == "dev", ]
    

    
    
# Helper functions ------------------------------------------------

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
    
 
    
    # build and send request to amz api for product searching - default keyword is "guitar pedal"
    amz_itemlookup_request <- function(p_access_key, p_secret, p_associatetag, p_ASIN, p_responsegrp="ItemAttributes",
                                       p_endpoint="webservices.amazon.com", p_uri="/onca/xml") {
        
        # create a timestamp
        request_timestamp <- time_since_epoch()
        
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
            asin         = p_ASIN)
        
        return(return_list)
    }
    
    
    
    # parse the entire itemsearch response
    amz_itemlookup_parse <- function(p_itemsearch_response_list) {
        # p_itemsearch_response_list: list returned from amz_itemsearch_request
            # [[1]] this is the raw response from amazon
            # [[2]] this is the utc epoch timestamp at the time the request was signed
            # [[3]] the asin that was looked up
        
        p_itemsearch_response <- p_itemsearch_response_list[[1]]
        timestamp             <- p_itemsearch_response_list[[2]]
        this_ASIN             <- p_itemsearch_response_list[[3]]
        
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
    
    
    # # "Unit testing"
    # x <- amz_itemlookup_request(
    #     p_access_key = amz_secret$amz_accesskey,
    #     p_secret = amz_secret$amz_secret,
    #     p_associatetag = amz_secret$amz_associatetag,
    #     p_responsegrp = "ItemAttributes,OfferSummary",
    #     p_ASIN = "B06Y15DWXR")
    # 
    # 
    # x_df <- amz_itemlookup_parse(x)
    
    
    
    
    
    
    
# database functions -------------------------------------------------
    


# database connection function - just simply returns the connection    
conn_to_db <- function(db_user, db_pw, db_host, db_port, db_name) {
    conn = DBI::dbConnect(RMySQL::MySQL(),
                          user = db_user,
                          password = db_pw,
                          host = db_host,
                          port = db_port,
                          dbname = db_name)
    return(conn)
}

    # # unit test
    # conn <- conn_to_db(
    #     db_user  = mysql_secret$mysql_user,
    #     db_pw    = mysql_secret$mysql_pw,
    #     db_host  = mysql_secret$mysql_host,
    #     db_port  = mysql_secret$mysql_port,
    #     db_name  = mysql_secret$mysql_db)

    
# insert into database function
db_insert_row <- function(p_conn, p_df, p_tbl) {
    # p_conn: connection to the database
    # p_df: data.frame of the fields we want to append to the df table
    # p_tbl: table in the database we want to append to
    DBI::dbExecute(conn = p_conn, DBI::sqlAppendTable (
        con    = p_conn, 
        table  = p_tbl,
        values = p_df))
}


# query for asin database function
db_query_most_recent_by_id <- function(p_conn, p_id, p_id_value, p_date, p_tbl, p_verbose=T) {
    # p_conn: connection to the database
    # p_id: the name of the id we're grouping by to find max date
    # p_id_value: id (asin) number we want to look up in the database
    # p_date: date (or epoch timestamp) we want to use in order to pull most recent data for this id
    # p_tbl: database table we're querying
    
    # note: this query was designed with MySQL in mind
    
    # query: given a unique identifier (ASIN), give me the most recent record for that id
    # this is what we'll use to compare to the price coming straight from the API
    sql_statement <- sprintf(
    "SELECT *
     FROM %s tbl1
        JOIN (
        SELECT %s, MAX(%s) as %s
        FROM %s
        WHERE %s = '%s' 
        GROUP BY %s) AS tbl2
        ON tbl1.%s = tbl2.%s AND tbl1.%s = tbl2.%s;",
    p_tbl, p_id, p_date, p_date, p_tbl, p_id, p_id_value, p_id, p_id, p_id, p_date, p_date)
    
    # print out the statement
    if(p_verbose) {
        print("SQL Statement:")
        print(sql_statement)
    }
    
    # execute query
    this_result <- DBI::dbGetQuery(conn=p_conn, statement = sql_statement)
    return(this_result)
}

    # # unit test
    # db_query_most_recent_by_id(
    #     p_conn     = conn,
    #     p_id       = "ASIN",
    #     p_id_value = "B01MG0733A",
    #     p_date     = "EffTimestampUTC",
    #     p_tbl      = "gpu_price")



# compare values in dataframe to see if we need to insert row into db
check_if_df_vals_changed <- function(p_df1, p_df2, p_cols) {
    # p_df1: first data frame that we want to compare values
    # p_df2: second data frame that we want to compare values
    # p_cols: names of the columns that we want to compare in df1 and df2
    
    # browser()
    # note, this is designed for single-row data.frames
    if(nrow(p_df1) > 1 | nrow(p_df2) > 1) {stop("single-row dfs only")}
    
    # set up some space for the logicals
    bool_vector <- vector(mode = "logical", length = length(p_cols))
    
    # loop through each column name and compare the values
    for(i in 1:length(p_cols)) {
        this_col <- p_cols[i]
        # if they are both NA, then it should be false
        if(is.na(p_df1[1, this_col]) & is.na(p_df2[1, this_col])) {
            bool_vector[i] <- FALSE
        } else {
            bool_vector[i] <- p_df1[1, this_col] != p_df2[1, this_col]
        }
    }
    
    # this means it either was NA and now isn't, or it wasn't NA and now it is
    bool_vector[is.na(bool_vector)] <- TRUE
    return(any(bool_vector))
}
    
    # unit test
    df1 <- data.frame(a = c(1), b = c("4"), stringsAsFactors = F)
    df2 <- data.frame(a = c(1), b = c("4"), stringsAsFactors = F)
    df3 <- data.frame(a = c(9), b = c("4"), stringsAsFactors = F)
    df4 <- data.frame(a = NA, b = c(NA), stringsAsFactors = F)

    is.na(df4[1, 'a'])  # this is true
    check_if_df_vals_changed(df1, df3, c("a", "b"))
    check_if_df_vals_changed(df1, df4, c("a", "b"))
    

db_empty_table <- function(p_conn, p_tbl) {
    # p_conn: connection to database
    # p_tbl: database table name
    
    sql_statement <- sprintf("DELETE FROM %s", p_tbl)
    dbExecute(conn = p_conn, statement = sql_statement)
}

    # # Unit test
    # db_empty_table(conn, "gpu_price")    



# Execution Loop ------------------------------------------------------------------------

    
# Logic:

# for each ASIN in the csv/xlsx:
# SQL Select in the final table for this ASIN:
#     if this ASIN doesn't exist in final table (zero rows returned), then just hit the API and load it into the final table
#     if this ASIN does exist in table, then hit the API and compare the price to what is found in the final table (compare all price fields including offers)
#         if the price from the API is missing or isn't numeric, then add this record to the error table
#         if the price from the API is the same as the final table, then update the timestamp and price in the "last price" table (kill/fill - update, don't insert.)
#         if the price from the API is different than the final table, insert a new row in the final table with the new price and new timestamp (this is the goal)



# read in list of products to track
    list.files("data")
    GPUs <- openxlsx::read.xlsx(xlsxFile = "data/GPU_curated_list.xlsx")
    
    
    # connect to database
    conn <- conn_to_db(
        db_user  = mysql_secret$mysql_user,
        db_pw    = mysql_secret$mysql_pw,
        db_host  = mysql_secret$mysql_host,
        db_port  = mysql_secret$mysql_port,
        db_name  = mysql_secret$mysql_db)
    
    
    # loop through list of GPUs to track
    for(i in 10:nrow(GPUs)) {
        
        # isolate one asin at a time
        this_asin <- GPUs$ASIN[i]
        print(paste0("iteration: ", i, "   ASIN: ", this_asin))
        
        
        # 0) hit Amazon api for this asin
        args(amz_itemlookup_request)
        this_item_lookup <- amz_itemlookup_request(
            p_access_key   = amz_secret$amz_accesskey,
            p_secret       = amz_secret$amz_secret,
            p_associatetag = amz_secret$amz_associatetag,
            p_ASIN         = this_asin,
            p_responsegrp  = "ItemAttributes,Offers")
        
        this_item_lookup_parsed <- amz_itemlookup_parse(this_item_lookup)
        
        
        # 1) SQL select in final table for this ASIN (isolate to most recent row of data)
        this_most_recent_record <- db_query_most_recent_by_id(
            p_conn     = conn,
            p_id       = "ASIN",
            p_id_value = this_asin,
            p_date     = "EffTimestampUTC",
            p_tbl      = "gpu_price")
        
        
        # if no record of this ASIN, just put it in the table
        if(nrow(this_most_recent_record) < 1) {
            
            db_insert_row(
                p_conn  = conn,
                p_df    = this_item_lookup_parsed,
                p_tbl   = "gpu_price")
            
        } else {
            # there was data, so compare database value to amazon result
            names(this_item_lookup_parsed)
            
            something_changed <- check_if_df_vals_changed(
                p_df1 = this_item_lookup_parsed,
                p_df2 = this_most_recent_record,
                p_cols = c("ListPrice", "LowestNewPrice", "LowestUsedPrice", "LowestRefurbPrice"))
            
                
            if(something_changed) {
                db_insert_row(
                    p_conn  = conn,
                    p_df    = this_item_lookup_parsed,
                    p_tbl   = "gpu_price")
            }
            
        }   
        
        # sleep for 2 - 16 seconds (choose at random from a uniform distribution)
        Sys.sleep(runif(1, 2, 16))
    }
    
    
    
    
    
    