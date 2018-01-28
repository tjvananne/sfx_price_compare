

# NOTE: these are the most useful/high-level DBI functions
# Use these: they are highest level
# DBI::dbGetQuery - selecting
# DBI::dbExecute  - everything else
# DBI::sqlAppendTable - super useful for large inserts




# source in files, load libs, connect to db, setup-----------------------------------------------------------------

    source("r_scripts/sfx_asin_crawler_functions.R")
    source("r_scripts/sfx_config.R")
    con <- connect_to_db()
    df_pricewindows <- set_up_price_windows(3000, 50000, 200)
    stage_req_cols <- c("ASIN", "Product_Title", "Product_Brand", "Product_Price", "Currency_Code", "Epoch_Time")
    sleep_time <- 60

    
    
    
    
# begin outer loop to go through the the price windows ------------------------------------------------------------
    
    
    # Notes:
    # outer loop will iterate through df_pricewindows rows
        # inner loop will iterate through PAGES within a price window
    
    
    # to pick up where we left off
    # window_start <- 109  
    window_start <- 1
    
    # start outer loop
    for(i in window_start:nrow(df_pricewindows)) {
        
        print(paste0("***************on price window: ", i))
        
        this_minprice <- df_pricewindows$minprice_window[i]
        this_maxprice <- df_pricewindows$maxprice_window[i]
        print(paste0(this_minprice, " to ", this_maxprice))
      
      
        # process this first page of results
        asin_xml_page <- amz_itemsearch_request(
            p_access_key = amz_key,
            p_secret     = amz_secret,
            p_minprice   = this_minprice,
            p_maxprice   = this_maxprice,
            p_page       = 1)
        asin_df_page  <- amz_itemsearch_parse(asin_xml_page) 
        if(!is.na(asin_df_page)) {
            asin_total_page_num <- amz_itemsearch_find_total_pgs(asin_xml_page)
            
                    # embedding a few test cases into this first page processing
                    dev_testing <- F
                    if(dev_testing) {
                        # cache out for dev testing
                        cache_path <- "using_API/01_asin_crawler/cache_data/active_testing/"
                        # write.csv(asin_df_page, file.path(cache_path, "asin_df_page_01.csv"), row.names = F)    
                        # asin_df_page <- read.csv(file.path(cache_path, "asin_df_page_01.csv"), stringsAsFactors=F)
                        
                        # setup for testing SCD
                        # kill/fill in fact table
                        DBI::dbExecute(con, statement="delete from asin_fact01 where fact_pk <> '0';")  # <-- delete / reset
                        scd_test_data <- asin_df_page[5:7, ] %>% 
                            mutate(Price_Eff_Start = Epoch_Time,
                                   Price_Eff_End = as.numeric(paste0(rep(9, 10), collapse='')),
                                   Is_Current = "Y") %>%
                            select(ASIN, Product_Price, Price_Eff_Start, Price_Eff_End, Is_Current)
                        DBI::dbExecute(con, DBI::sqlAppendTable(con, 'asin_fact01', scd_test_data))
                        
                        # manual change to test SCD
                        asin_df_page$Product_Price[5] <- 5235
                        # asin_df_page$Product_Price[7] <- 235
                        asin_df_page$Epoch_Time <- paste0(rep(3, 10), collapse='')
                    }
            
            # route to SCD or Stage
            scd_or_stage_list <- prep_for_scd2_or_stage(con, p_asin_df_page=asin_df_page, p_required_cols=stage_req_cols)
            
            # SCD if they're tracked in fact table and price is different
            if(nrow(scd_or_stage_list$asin_to_scd) > 0) {
                scd2_asins_api_to_fact(con, scd_or_stage_list$asin_to_scd)
            }
            
            # otherwise, kill/fill the stage table with this page of results
            if(nrow(scd_or_stage_list$asin_to_stage) > 0) {
                kill_and_fill_stage_page(p_con=con, p_asin_to_stage=scd_or_stage_list$asin_to_stage)
            }
        }
        
        
        # start loop for pages 2 through <however many pages there are
        for(j in 2:(min(as.integer(asin_total_page_num), 10))) {  
            
            if(j < 2) {break} 
            if(j == 2) {Sys.sleep(sleep_time)}
            print(paste0("on page: ", j))
            
            # loop through same steps for all others
            asin_xml_page <- amz_itemsearch_request(
                p_access_key = amz_key,
                p_secret     = amz_secret,
                p_minprice   = this_minprice,
                p_maxprice   = this_maxprice,
                p_page       = j)
            asin_df_page <- amz_itemsearch_parse(asin_xml_page) 
            if(is.na(asin_df_page)) {next}
            
            # route to SCD or stage
            scd_or_stage_list <- prep_for_scd2_or_stage(con, p_asin_df_page=asin_df_page, p_required_cols=stage_req_cols)
            
            
            # SCD (first check if NA, then check if nrow > 0)
            if("data.frame" %in% class(scd_or_stage_list$asin_to_scd)) {
                if(nrow(scd_or_stage_list$asin_to_scd) > 0) {
                    scd2_asins_api_to_fact(con, scd_or_stage_list$asin_to_scd)
                }
            }
            
            
            # Stage
            if("data.frame" %in% class(scd_or_stage_list$asin_to_stage)) {
                if(nrow(scd_or_stage_list$asin_to_stage) > 0) {
                    kill_and_fill_stage_page(p_con=con, p_asin_to_stage=scd_or_stage_list$asin_to_stage)
                }
            }
            
            # sleep regardless (Because we hit API regardless of outcome)
            Sys.sleep(sleep_time)
        }
    }
        
        
          