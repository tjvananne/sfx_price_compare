


# AMAZON API FUNCTIONS ------------------------------------------------------------------



# helper timestamp function
generate_utc_timestamp_ <- function(ts) {
    library(lubridate)
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




# build price windows - this is what helps us narrow our search in the API
set_up_price_windows <- function(p_minprice, p_maxprice, p_step) {
    df_pricewindows_ <- data.frame(minprice_window = seq(p_minprice, p_maxprice, p_step), stringsAsFactors = F)
    df_pricewindows_$maxprice_window <- seq((p_minprice + (p_step - 1)), (p_maxprice + (p_step - 1)), p_step)
    return(df_pricewindows_)    
}




# build and send request to amz api for product searching - default keyword is "guitar pedal"
amz_itemsearch_request <- function(p_access_key, p_secret, p_minprice, p_maxprice, p_page=1, 
                                   p_endpoint="webservices.amazon.com", p_uri="/onca/xml") {
  
    # parameters of the query 
    params <- list(
        "AWSAccessKeyId" = p_access_key,
        "AssociateTag"   = "st010-20",
        "ItemPage"       = as.character(p_page),
        "Keywords"       = "guitar pedal",
        "MaximumPrice"   = as.character(p_maxprice),
        "MinimumPrice"   = as.character(p_minprice),
        "Operation"      = "ItemSearch",
        "ResponseGroup"  = "Images,ItemAttributes,Offers,Reviews,SalesRank",
        "SearchIndex"    = "MusicalInstruments",
        "Service"        = "AWSECommerceService",
        "Sort"           = "price")
  
    # attach timestamp after setting up parameters
    params$Timestamp = generate_utc_timestamp_(Sys.time())
    
    # url encode all parameters, paste them together with "=", collapse with "&"
    params_url_encoded <- lapply(params, URLencode, reserved=T)
    paired_params      <- paste0(names(params_url_encoded), '=', params_url_encoded)
    canonical_str_req  <- paste0(paired_params, collapse="&")
    
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
  
    return(raw_response)
}







# isolate total number of pages from api itemsearch request
amz_itemsearch_find_total_pgs <- function(p_itemsearch_response) {
  
    # parse, root, check user agent
    parse_response <- XML::xmlTreeParse(p_itemsearch_response)
    r  <- XML::xmlRoot(parse_response)
    
    # identify the maximum number of pages we have to look at for this pricing window
    r_total_pages <- xmlValue(r[['Items']][['TotalPages']])
    return(r_total_pages)
}





# parse the entire itemsearch response
amz_itemsearch_parse <- function(p_itemsearch_response) {
  
    # browser()
    
    # parse, root, check user agent
    parse_response <- XML::xmlTreeParse(p_itemsearch_response)
    r  <- XML::xmlRoot(parse_response)
    UserAgent <- r[['OperationRequest']][['HTTPHeaders']][['Header']] %>% xmlGetAttr("Value")
    
    # identify the maximum number of pages we have to look at for this pricing window
    r_total_pages <- xmlValue(r[['Items']][['TotalPages']])
    
    # isolate the items, then we can iterate through them in a loop
    r_Items <- r[['Items']][names(r[['Items']]) == 'Item']
    names(r_Items)                  
    numb_items <- length(r_Items) # should be 10? unless maybe last "page" of the overall request  
    if(numb_items < 1) {return(NA)}
    
    # loop for item attributes
    for(i in 1:numb_items) {
    
        # set up some in-loop collectors
        if(i == 1) {
            Titles         <- character(numb_items)
            ASINS          <- character(numb_items)
            # ImageMed       <- character(numb_items)
            # DetailLongURL  <- character(numb_items)  # is this even necessary? 
            # DetailShortURL <- character(numb_items)
            SalesRanks     <- character(numb_items)
            Brands         <- character(numb_items)
            Model          <- character(numb_items)
            ProdType       <- character(numb_items)
            PkgQuant       <- character(numb_items)
            ListPrice      <- numeric(numb_items)
            CurrencyCd     <- character(numb_items)
            ReviewURL      <- character(numb_items)
            Dim_H          <- character(numb_items)
            Dim_L          <- character(numb_items)
            Dim_We       <- character(numb_items)
            Dim_Wi       <- character(numb_items)
            Dim_H_units  <- rep(NA, numb_items)
            Dim_L_units  <- rep(NA, numb_items)
            Dim_We_units <- rep(NA, numb_items)
            Dim_Wi_units <- rep(NA, numb_items)
            Feature1     <- character(numb_items)
            Feature2     <- character(numb_items)
            Feature3     <- character(numb_items)
            Feature4     <- character(numb_items)
            Feature5     <- character(numb_items)
            # Manuf       <- character(numb_items)  # meh, brand is better
            # MPN         <- character(numb_items)  # manufacturer Product number? - not very good...
            # PartNumb    <- character(numb_items)  # bad
            # Publisher   <- character(numb_items)  # bad
            # PubDate     <- character(numb_items)  # bad
        }
        
        # this is how you explore:
        # xmlSApply(r_Items[[i]], xmlName)
        # xmlValue(r_Items[[i]][['DetailPageURL']]) %>% nchar()
        # xmlValue(r_Items[[5]][['MediumImage']])
        
        # fill in the collectors
        Titles[i]      <- xmlValue(r_Items[[i]][['ItemAttributes']][['Title']])
        ASINS[i]       <- xmlValue(r_Items[[i]][['ASIN']])
        SalesRanks[i]  <- xmlValue(r_Items[[i]][['SalesRank']])
        Brands[i]      <- xmlValue(r_Items[[i]][['ItemAttributes']][['Brand']])
        Model[i]       <- xmlValue(r_Items[[i]][['ItemAttributes']][['Model']])
        PkgQuant[i]    <- xmlValue(r_Items[[i]][['ItemAttributes']][['PackageQuantity']])
        ProdType[i]    <- xmlValue(r_Items[[i]][['ItemAttributes']][['ProductTypeName']])
        ListPrice[i]   <- xmlValue(r_Items[[i]][['ItemAttributes']][['ListPrice']][['Amount']])
        CurrencyCd[i]  <- xmlValue(r_Items[[i]][['ItemAttributes']][['ListPrice']][['CurrencyCode']])
        ReviewURL[i]   <- URLencode(xmlValue(r_Items[[i]][['CustomerReviews']][['IFrameURL']]))
        Dim_H[i]       <- xmlValue(r_Items[[i]][['ItemAttributes']][['ItemDimensions']][['Height']])
        Dim_L[i]       <- xmlValue(r_Items[[i]][['ItemAttributes']][['ItemDimensions']][['Length']])
        Dim_We[i]      <- xmlValue(r_Items[[i]][['ItemAttributes']][['ItemDimensions']][['Weight']])
        Dim_Wi[i]      <- xmlValue(r_Items[[i]][['ItemAttributes']][['ItemDimensions']][['Width']])
        if(!is.na(Dim_H[i])) {Dim_H_units[i]  <- xmlGetAttr(r_Items[[i]][['ItemAttributes']][['ItemDimensions']][['Height']], "Units")}
        if(!is.na(Dim_L[i])) {Dim_L_units[i]  <- xmlGetAttr(r_Items[[i]][['ItemAttributes']][['ItemDimensions']][['Length']], "Units")}
        if(!is.na(Dim_We[i])) {Dim_We_units[i]  <- xmlGetAttr(r_Items[[i]][['ItemAttributes']][['ItemDimensions']][['Weight']], "Units")}
        if(!is.na(Dim_Wi[i])) {Dim_Wi_units[i]  <- xmlGetAttr(r_Items[[i]][['ItemAttributes']][['ItemDimensions']][['Width']], "Units")}
        # Manuf[i]       <- xmlValue(r_Items[[i]][['ItemAttributes']][['Manufacturer']])
        # Publisher[i]   <- xmlValue(r_Items[[i]][['ItemAttributes']][['Publisher']])
        # PubDate[i]     <- xmlValue(r_Items[[i]][['ItemAttributes']][['PublicationDate']])
        # MPN[i]         <- xmlValue(r_Items[[i]][['ItemAttributes']][['MPN']])
        # PartNumb[i]    <- xmlValue(r_Items[[i]][['ItemAttributes']][['PartNumber']])
        
        # features must be picked out a bit differently
        feature_nodes <- r_Items[[i]][['ItemAttributes']][names(r_Items[[i]][['ItemAttributes']]) == "Feature"]
        features      <- sapply(feature_nodes, xmlValue)
        if(length(features) > 0) {Feature1[i]   <- features[1]}
        if(length(features) > 1) {Feature2[i]   <- features[2]}
        if(length(features) > 2) {Feature3[i]   <- features[3]}
        if(length(features) > 3) {Feature4[i]   <- features[4]}
        if(length(features) > 4) {Feature5[i]   <- features[5]}
    }
  
    # package it up into a data frame
    results_df <- data.frame(
        ASIN = ASINS,
        Product_Title = Titles,
        Product_Brand = Brands,
        Product_Price = ListPrice,
        Currency_Code = CurrencyCd,
        Epoch_Time = time_since_epoch(),
        Reviews_URL = URLencode(ReviewURL),
        Product_Model = Model,
        Package_Quantity = PkgQuant,
        Product_Type = ProdType,
        Product_Feature1 = Feature1,
        Product_Feature2 = Feature2,
        Product_Feature3 = Feature3,
        Product_Feature4 = Feature4,
        Product_Feature5 = Feature5,
        Dim_Height = Dim_H,
        Dim_Length = Dim_L,
        Dim_Width = Dim_Wi,
        Dim_Weight = Dim_We,
        Dim_Height_Units = Dim_H_units,
        Dim_Length_units = Dim_L_units,
        Dim_Width_units = Dim_Wi_units,
        Dim_Weight_units = Dim_We_units,
        stringsAsFactors = F)
    
    results_df <- data.frame(lapply(results_df, trimws), stringsAsFactors = F)
    results_df <- data.frame(lapply(results_df, function(x) ifelse(x == '', NA, x)), stringsAsFactors = F)
    return(results_df)
}

    


# DATABASE FUNCTIONS -------------------------------------------------------------------


# relies on data read in from config file (credentials.csv)
connect_to_db <- function() {
    
    # this relies on data external to this function - read in from the config file
    # this is bad form, but I'm allowing it for now
    con <- DBI::dbConnect(RMySQL::MySQL(), 
                          user=db_user, 
                          password=db_pw, 
                          host=db_host, 
                          port=db_port, 
                          dbname=db_name)
    return(con)
    
}





# Returns ASINs from the database that match the list of ASINs we sent into the query to check for
are_these_ASIN_in_this_tbl <- function(p_con, p_tblname, p_ASIN_vec) {
    query_string_ <- paste0("SELECT ASIN FROM ", p_tblname, " WHERE ASIN IN ('", 
                          paste0(p_ASIN_vec, collapse="','"), "');")
    print(query_string_)
    df_result_ <- DBI::dbGetQuery(conn=p_con, statement=query_string_)
    return(df_result_)
}







# split api asin data up into data that needs to be staged vs data that needs to be put into slowly changing dimensions in fact table
prep_for_scd2_or_stage <- function(p_con, p_asin_df_page, p_required_cols) {
    # 3 potential scenarios:
    #    1) ASIN is not in fact table --> asin_not_in_fact --> execute the asin staging function
    #    2) ASIN is tracked in fact table but the price is the same as what we have in fact table --> do nothing
    #    3) ASIN is tracked in fact table and the price is different --> asin_to_scd --> send it to SCD function
    
    # raw asin_df_page - filter to complete cases in our required columns
    this_asin_df <- p_asin_df_page[complete.cases(p_asin_df_page[, p_required_cols]), ]
    
    # do we have any good results to process at all?
    if(length(this_asin_df$ASIN) > 0) {
        asin_in_fact <- are_these_ASIN_in_this_tbl(p_con, tbl_fact, this_asin_df$ASIN)
    } else {
        # none of the ASINs on this page are clean enough for stage/fact - return empty DF (easier to handle than NA/Null)
        dummy_df_ <- data.frame()
        return(list("asin_to_scd"=dummy_df_, "asin_to_stage"=dummy_df_))
    }
    
    # rename so we know which price came from API and which came from fact table
    names(asin_in_fact)[names(asin_in_fact) == "Product_Price"] <- "Price_From_Fact"
    asin_not_in_fact <- this_asin_df[!this_asin_df$ASIN %in% asin_in_fact$ASIN, ]
    
    # this_asin_df holds the API columns we want, but here we want to isolate only the rows that are already in fact
    asin_in_api_and_fact  <- this_asin_df[this_asin_df$ASIN %in% asin_in_fact$ASIN, c("ASIN", "Product_Price", "Epoch_Time")]
    
    # this merge enables us to compare the price from Fact and price from API
    asin_api_fact_compare <- merge(asin_in_api_and_fact, asin_in_fact, by="ASIN", all=T, sort=F)
    
    # isolate the rows where API price is different from Fact table current price - these will be sent do slowly changing dimension func
    asin_to_scd <- asin_api_fact_compare[asin_api_fact_compare$Price_From_Fact != asin_api_fact_compare$Product_Price &
                                             !is.na(asin_api_fact_compare$Product_Price), c("ASIN", "Product_Price", "Epoch_Time")]

    # return two dfs, one of asin to scd, one of asin to just stage (if it's in fact, we don't need to stage it)
    return(list(
        "asin_to_scd"   = asin_to_scd,
        "asin_to_stage" = asin_not_in_fact 
    ))
    
}




# Pass in a single row where we want to execute slowly changing dimensions in fact table (already know we need to do it)
scd2_helper_price_ <- function(p_conn, p_ASIN, p_tbl, p_Epoch_Time, p_Product_Price) {
    
    # wrapped in transaction - we don't want the update or insert to occur separately, ONLY as a pair
    DBI::dbWithTransaction(conn=p_conn, code = {
        
        # In-place update of existing current record for that ASIN
        upd_qry <- paste0("UPDATE ", p_tbl, 
                          " SET Is_Current = 'N', Price_Eff_End = ", p_Epoch_Time, 
                          " WHERE ASIN = '", p_ASIN, "'", 
                          " AND Is_Current = 'Y';")
        DBI::dbSendQuery(conn=p_conn, statement=upd_qry)
        
        # Insert the new current record for that ASIN
        insert_qry <- paste0("INSERT INTO ", p_tbl, " (ASIN, Product_Price, Price_Eff_Start, Price_Eff_End, Is_Current)",
                             " VALUES ('", p_ASIN, "', ", p_Product_Price, ", ", p_Epoch_Time, ", 9999999999, 'Y');" )
        DBI::dbSendQuery(conn=con, statement=insert_qry)
        
    })
}




# wrapper to be able to handle slowly changing dimensions for multiple fields
scd2_asins_api_to_fact <- function(p_con, p_df_to_scd) {
    
    if(nrow(p_df_to_scd) > 0) {
        for(i in 1:nrow(p_df_to_scd)) {
            scd2_helper_price_(
                p_conn=p_con,
                p_ASIN=p_df_to_scd$ASIN[i],
                p_tbl=tbl_fact,
                p_Epoch_Time=p_df_to_scd$Epoch_Time[i],
                p_Product_Price=p_df_to_scd$Product_Price[i]
            )
        }
    }
}




# kill and fill the stage table with a page of results
kill_and_fill_stage_page <- function(p_con, p_asin_to_stage) {
    
    # wrap in transaction so we never kill without a fill
    DBI::dbWithTransaction(conn=p_con, code = {
        
        # here's our kill
        DBI::dbExecute(p_con, statement= paste0("delete from asin_stage01 where asin in ('", 
                                                paste0(p_asin_to_stage$ASIN, collapse="', '"), "')"))
        
        # here's our fill
        DBI::dbExecute(conn=p_con, DBI::sqlAppendTable(p_con, 'asin_stage01', p_asin_to_stage))
    })
    
}

