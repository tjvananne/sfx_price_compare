
# if these fail, install them - come back and build in that logic

library(odbc)
library(DBI)


# gotcha's --------------------------------------------------------

#' 1. Insertion of "NULL" integers into database... basically just don't insert anything for that column
#' 2. single quotes found within 



# load in password info -------------------------------------------
    
    db_secret    <- read.csv("credentials/db_secret.csv", stringsAsFactors = F)
    db_secret <- db_secret[db_secret$env == "dev", ]
    conn = DBI::dbConnect(odbc::odbc(),
                          Driver = "ODBC Driver 17 for SQL Server",
                          Server = db_secret$server,
                          Database = db_secret$database,
                          UID = db_secret$user_id,
                          PWD = db_secret$password,
                          Port = db_secret$port)

    
  
# database functions -------------------------------------------------
        
    
    db_list_tables <- function(p_conn, p_database) {
        # p_conn: odbc connection to the database (per DBI)
        # p_database: name of the database to list the tables from (db_secret$database)
        # returns: character vector of the names of the database tables in this database
        
        # come back and test this once there are multiple databases in this instance that
        # have tables with the same name... I don't think I need the p_database arg for this function.
        
        tables <- dbGetQuery(p_conn, "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';")
        tables <- tables[tables$TABLE_CATALOG == p_database, ]
        return(tables$TABLE_NAME)
    }
    db_list_tables(p_conn=conn, p_database=db_secret$database)    

    
    
    # just until development is finished... kinda dangerous to just keep this in the script as an option.
    db_drop_table <- function(p_conn, p_tbl) {
        # p_conn: odbc connection to the database
        # p_tbl: table name of the table you want to drop
        dbExecute(conn=p_conn, statement=  gsub("<table_name>", p_tbl, "DROP TABLE dbo.<table_name>;"))
    }
    # db_drop_table(p_conn=conn, p_tbl="Person")
    
    
    # once again, only for development. avoid keeping this in the main project files.
    # db_delete_table_contents <- function(p_conn, p_tbl) {
    #     # p_conn: connection to database
    #     # p_tbl: database table name
    #     
    #     sql_statement <- sprintf("DELETE FROM %s", p_tbl)
    #     dbExecute(conn = p_conn, statement = sql_statement)
    # }
    
        # # how to call
        # db_delete_table_contents(conn, "gpu_price")    
    
    
    
    
# Set up database if it isn't set up yet ----------------------------------------------------
    
# db_drop_table(conn, "ASIN")
if(!"ASIN" %in% db_list_tables(p_conn=conn, db_secret$database)) {
    dbExecute(conn=conn, statement=
    "CREATE TABLE ASIN (
        ASIN_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
        ASIN varchar(15) NOT NULL,
        CONSTRAINT UNQ_ASIN UNIQUE (ASIN)
    );"
                  )
}

 
# db_drop_table(conn, "ASIN")
if(!"ASIN_Category" %in% db_list_tables(p_conn=conn, db_secret$database)) {
    dbExecute(conn=conn, statement=
    "CREATE TABLE ASIN_Category (
        ASIN_id INT NOT NULL CONSTRAINT FK_ASIN_id_Category REFERENCES ASIN(ASIN_id),
        ASIN varchar(15) NOT NULL,
        Category1 varchar(150) NOT NULL,
        Category2 varchar(150) NOT NULL,
        Category3 varchar(150),
        CONSTRAINT UNQ_ASIN_Category UNIQUE (ASIN, Category1, Category2));"
    )
}
    
            
    
# create Product table if it doesn't exist
# db_drop_table(conn, "Amz_Product")
if(!"Amz_Product" %in% db_list_tables(p_conn=conn, db_secret$database)) {
    dbExecute(conn=conn, statement=
    
    "CREATE TABLE Amz_Product (
        ASIN_id INT NOT NULL CONSTRAINT FK_ASIN_id_Product REFERENCES ASIN(ASIN_id),
        Product_ASIN varchar(15),
        Product_DateTime datetime2,
        Product_Title varchar(200),
        Product_Brand varchar(150),
        Product_Manufacturer varchar(150),
        Product_Model varchar(150),
        Product_MPN varchar(150),
        Product_NumberOfItems INT,
        Product_PartNumber varchar(150),
        Product_TypeName varchar(100),
        Product_UPC varchar(100)
    );"
    )
}


# create Amz_ListPrice table if it doesn't exist
# db_drop_table(conn, "Amz_ListPrice")
if(!"Amz_ListPrice" %in% db_list_tables(p_conn=conn, db_secret$database)) {
    
    # defaults to IsActive is TRUE (1) because if you're inserting a price, it should
    # be active at that exact moment. The burden is on the transaction query handling
    # slowly changing dimensions to mark the existing IsActive == 1 record to have its
    # value set to 0 explicitly. This is an acceptable level of logic to remember based
    # on the small size of this project.
    dbExecute(conn=conn, statement=
        "CREATE TABLE Amz_ListPrice (
            ASIN_id INT NOT NULL CONSTRAINT FK_ASIN_id_ListPrice REFERENCES ASIN(ASIN_id),
            ASIN varchar(15),
            ListPrice_Effdt datetime2 NOT NULL,
            ListPrice INT NOT NULL,
            ListPrice_IsActive BIT NOT NULL DEFAULT 1
        );"    
    )
    
}


    
    
    
    