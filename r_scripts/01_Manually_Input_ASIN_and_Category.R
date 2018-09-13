
#' This is looking good. Next step will be to 



# set up ----------------------------------------------------------------------------------
library(dplyr)
library(odbc)
library(DBI)
library(lubridate)

db_secret    <- read.csv("credentials/db_secret.csv", stringsAsFactors = F)

# which environment - create the connection
db_secret <- db_secret[db_secret$env == "prod", ]
conn = DBI::dbConnect(odbc::odbc(),
                      Driver = "ODBC Driver 17 for SQL Server",
                      Server = db_secret$server,
                      Database = db_secret$database,
                      UID = db_secret$user_id,
                      PWD = db_secret$password,
                      Port = db_secret$port)

 
# notes for how these functions should work --------------------------------------------------

# This function touches these tables:
    # ASIN
    # ASIN_Category
    
# Function steps:
    # if the ASIN passed exists within the ASIN table
        # if it exists in ASIN_Category (Use that ASIN_id associated with it to look up)
            # If exists in ASIN_Category with the same Category1 and Category2 value the user is trying to enter it as
                # return an error message saying that already exists
            # else: insert the ASIN with the Category values and ASIN_id into ASIN_Category
        # else: insert the ASIN with Category values and ASIN_id into ASIN_Category
    # else: insert the ASIN into the ASIN table then insert the ASIN with Category values and ASIN_id into ASIN_Category
    


# functions -----------------------------------------------------------------------------------

# helper function for inserting into the ASIN_Category table
db_insert_into_ASIN_Category <- function(p_conn, p_ASIN_id, p_ASIN, p_Cat1, p_Cat2, p_Cat3=NULL) {
    
    if(!is.null(p_Cat3)) {
        dbExecute(conn=p_conn, statement=
                      paste0(
                          "INSERT INTO prod_ASIN_Category (ASIN_id, ASIN, Category1, Category2, Category3)
                VALUES (", p_ASIN_id, ",'", p_ASIN, "','", p_Cat1, "','", p_Cat2, "','", p_Cat3, "');"))
    } else {
        dbExecute(conn=p_conn, statement=
                      paste0(
                          "INSERT INTO prod_ASIN_Category (ASIN_id, ASIN, Category1, Category2)
                VALUES (", p_ASIN_id, ",'", p_ASIN, "','", p_Cat1, "','", p_Cat2, "');"))
    }
}


db_insert_new_ASIN <- function(p_conn, p_ASIN, p_Category1, p_Category2, p_Category3=NULL) {
    
    # p_conn: connection to database
    # p_ASIN: the ASIN (amazon ID) for this product ("B0719CBYXJ")
    # p_Category1: the highest-level category to assign to this product ("Guitar Pedal")
    # p_Category2: the next level category to assign to this product ("Reverb")
    # p_Category3: an optional argument to assign an even more detailed category to the product
    
    # Notes: the ASIN_Category table constrains unique values to these cols:
        # ASIN
        # Category1
        # Category2
        # So, for example: You can have the same ASIN in the table, as long as either 
        # category1 or category2 are different values. I did this because of obvious
        # overlaps in guitar pedal types. There are many "reverb / delay" pedals and
        # I want both of these to be searchable without forcing awkward "Reverb & Delay" 
        # categories.
    
    
    # browser()
    # # parameters to function
    # p_conn      <- conn
    # p_ASIN      <- "B0719CBYXJ"
    # p_Category1 <- "Guitar Pedal"
    # p_Category2 <- "Reverb"
    # p_Category3 <- NULL # default this arg to NULL
    
    # inside function:
    this_ASIN <- dbGetQuery(p_conn, 
        statement = paste0(
            "SELECT TOP 1 ASIN_id, ASIN
            FROM prod_ASIN
            WHERE ASIN = '", p_ASIN, "';"))
    
    
    if(nrow(this_ASIN)) {
        # exists in ASIN, now test for existence in ASIN_Category
        this_ASIN_Category <- dbGetQuery(p_conn,
         statement = paste0(
             "SELECT *
             FROM prod_ASIN_Category
             WHERE ASIN_id = ", this_ASIN$ASIN_id, ";"))
        
        if(nrow(this_ASIN_Category)) {
            # exists in ASIN_Category - test to see if Category1 and Category2 already exist
            data_is_new <- any(this_ASIN_Category$Category1 != p_Category1, 
                               this_ASIN_Category$Category2 != p_Category2)
            
            # insert this data into ASIN_Category:
            if(data_is_new) {
                db_insert_into_ASIN_Category(
                    p_conn    = p_conn,
                    p_ASIN_id = this_ASIN$ASIN_id, 
                    p_ASIN    = p_ASIN, 
                    p_Cat1    = p_Category1, 
                    p_Cat2    = p_Category2, 
                    p_Cat3    = p_Category3)
            } else {stop("This ASIN / Category1 / Category2 Combination already exists!")}
            
        } else {
            # exists in ASIN, but not in ASIN_Category - just add it now
            
            # insert this data into ASIN_Category:
            db_insert_into_ASIN_Category(
                p_conn    = p_conn,
                p_ASIN_id = this_ASIN$ASIN_id, 
                p_ASIN    = p_ASIN, 
                p_Cat1    = p_Category1, 
                p_Cat2    = p_Category2, 
                p_Cat3    = p_Category3)
        }
    } else {
        # doesn't exist in ASIN nor ASIN_Category
        
        # insert into ASIN
        dbExecute(p_conn, statement = 
            paste0(
                "INSERT INTO prod_ASIN (ASIN)
                VALUES ('", p_ASIN, "');"))
        
        # query for the ASIN_id
        this_ASIN <- dbGetQuery(p_conn, 
            statement = paste0(
                "SELECT TOP 1 ASIN_id, ASIN
                FROM prod_ASIN
                WHERE ASIN = '", p_ASIN, "';"))
        
        # insert into ASIN_Category:
        db_insert_into_ASIN_Category(
            p_conn    = p_conn,
            p_ASIN_id = this_ASIN$ASIN_id, 
            p_ASIN    = p_ASIN, 
            p_Cat1    = p_Category1, 
            p_Cat2    = p_Category2, 
            p_Cat3    = p_Category3)
    }
}


# examples for how to call this function ------------------------------------------


    db_insert_new_ASIN(
        p_conn      = conn, 
        p_ASIN      = "B00E4WUU32",
        p_Category1 = "Guitar Pedal",
        p_Category2 = "Overdrive")

    
    # # 1 B0719CBYXJ Guitar Pedal    Reverb      <NA>  
    # db_insert_new_ASIN(conn, "B0719CBYXJ", "Guitar Pedal", "Reverb")
    # 
    # 
    # # ok it works.
    # db_insert_new_ASIN(conn, "B074VDY8FM", "Guitar Pedal", "Reverb")
    # db_insert_new_ASIN(conn, "B074VDY8FM", "Guitar Pedal", "Delay")  # <-- this should work
    # db_insert_new_ASIN(conn, "B074VDY8FM", "Guitar Pedal", "Delay")  # <-- this should fail


    # # doing a little data conversion from the previous schema
    # ASIN_data <- ASIN_data %>%
    #     filter(!ASIN %in% c("B074VDY8FM", "B0719CBYXJ"))
    # 
    # for(i in 1:nrow(ASIN_data)) {
    #     
    #     print(i)
    #     db_insert_new_ASIN(conn, ASIN_data$ASIN[i], ASIN_data$Category1[i], ASIN_data$Category2[i])
    #     Sys.sleep(2)
    #     
    # }



