

#' R multi-row MS SQL Server insert into (from dataframe)
#' should be done in base r



# FUNCTION VERSION ------------------------------------------------------

# replacement for sqlAppendTable
db_sql_append_table <- function(p_df, p_tbl) {
    # p_df: data.frame that contains the data to append/insert into the table
        # the names must be the same as those in the database
    # p_tbl: the name of the database table to insert/append into
    
    num_rows <- nrow(p_df)
    num_cols <- ncol(p_df)
    requires_quotes <- sapply(p_df, class) %in% c("character", "factor")
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


# test...
cat(db_sql_append_table(iris[1:10,], "dbo.Iris_tbl"))


# DATABASE CONNECTION ---------------------------------------------------------------------

library(odbc)
library(DBI)
# library(RMySQL)  # switched from MySQL to MS SQL Server

# I have my data.base info in a db_secret.csv file
db_secret    <- read.csv("credentials/db_secret.csv", stringsAsFactors = F)
db_secret    <- db_secret[db_secret$env == "dev", ]

# connect to the database (mine is remote/cloud)
conn = DBI::dbConnect(odbc::odbc(),
                      Driver   = "ODBC Driver 17 for SQL Server",
                      Server   = db_secret$server,
                      Database = db_secret$database,
                      UID      = db_secret$user_id,
                      PWD      = db_secret$password,
                      Port     = db_secret$port)


# uncomment and run this if you want to drop the table and retest
# dbExecute(conn=conn, statement = "DROP TABLE test_sql_insert")

# create the table
dbExecute(conn=conn, statement = 
    "CREATE TABLE test_sql_insert 
    (
        sepal_length  float,
        sepal_width   float,
        petal_length  float,
        petal_width   float,
        species       varchar(25));")






# FUNCTION TESTING -------------------------------------------------------------------

# testing a small number of rows being inserted (150)
df <- iris
names(df) <- gsub("\\.", "_", tolower(names(df)))
names(df)


dbExecute(conn=conn, statement = db_sql_append_table(df, "test_sql_insert"))



# "stress" testing.. does this work all the way up to almost 1000 rows? (that is the limit for 
# sql server insert statement without using an insert into + select statement together)
# https://stackoverflow.com/questions/37471803/sql-server-maximum-rows-that-can-be-inserted-in-a-single-insert-statment
library(dplyr)
list_of_iris_dfs <- list(df)


# add dataframes to this list until we hit the row limit of 1000 for sql server inserts
i <- 1
while(sum(sapply(list_of_iris_dfs, nrow)) + nrow(df) < 1000) {
    list_of_iris_dfs[[i]] <- df
    i <- i + 1
}

many_dfs <- dplyr::bind_rows(list_of_iris_dfs)
dbExecute(conn=conn, statement = db_sql_append_table(many_dfs, "test_sql_insert"))

results_in_db <- dbGetQuery(conn=conn, statement="SELECT * FROM test_sql_insert")



# FUNCTION VERSION (less comments) ------------------------------------------------------

    # replacement for sqlAppendTable
    db_sql_append_table <- function(p_df, p_tbl) {
        # p_df: data.frame that contains the data to append/insert into the table
        # the names must be the same as those in the database
        # p_tbl: the name of the database table to insert/append into
        
        num_rows <- nrow(p_df)
        num_cols <- ncol(p_df)
        requires_quotes <- sapply(p_df, class) %in% c("character", "factor")
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
            
            # not the last column; follow up with a comma
            if(i < num_cols) {
                if(requires_quotes[i]) {
                    str_values <- mapply(paste0, str_values, quotes, p_df[[column_names[i]]], quotes, commas)        
                } else {
                    str_values <- mapply(paste0, str_values, p_df[[column_names[i]]], commas)
                }
                
            # this is the last column; follow up with closing parenthesis
            } else {
                if(requires_quotes[i]) {
                    str_values <- mapply(paste0, str_values, quotes, p_df[[column_names[i]]], quotes, ")")
                } else {
                    str_values <- mapply(paste0, str_values, p_df[[column_names[i]]], ")")
                }
            }
        }
        
        # build out the query; collapse values with comma & newline; end with semicolon;
        str_values <- paste0(str_values, collapse=",\n")
        str_query <- paste0(str_query, str_values)
        str_query <- paste0(str_query, ";")
        return(str_query)
    }



# PROCEDURAL / IMPERATIVE VERSION ----------------------------------------------------


# parameters:
df <- iris
tbl <- "dbo.iris"

# first lines of code within the function
num_rows <- nrow(df)
num_cols <- ncol(df)
requires_quotes <- sapply(df, class) %in% c("character", "factor")
commas <- rep(", ", num_rows)
quotes <- rep("'", num_rows)
# paren_open <- rep("(", num_rows)
# paren_close <- rep(")", num_rows)

str_columns <- ' ('
column_names <- names(df)
for(i in 1:num_cols) {
    
    if(i < num_cols) {
        str_columns <- paste0(str_columns, column_names[i], ", ")
    } else {
        str_columns <- paste0(str_columns, column_names[i], ") ")
    }
}
rm(i)


str_query <- paste0("INSERT INTO ", tbl, str_columns, "\nVALUES\n")   
cat(str_query)

str_values <- rep("(", num_rows)
str_values

for(i in 1:num_cols) {
    print(i)
    
    # not the last column in the insert statement...
    if(i < num_cols) {
        
        if(requires_quotes[i]) {
            # this column requires quotes, add them before and after the value
            str_values <- mapply(paste0, str_values, quotes, df[[column_names[i]]], quotes, commas)        
        } else {
            # doesn't require quotes...
            str_values <- mapply(paste0, str_values, df[[column_names[i]]], commas)
        }
        
    # this IS the last column in the insert statement - end with close paren
    } else {
        if(requires_quotes[i]) {
            # this column requires quotes, add them before and after the value
            str_values <- mapply(paste0, str_values, quotes, df[[column_names[i]]], quotes, ")")
        } else {
            # doesn't require quotes
            str_values <- mapply(paste0, str_values, df[[column_names[i]]], ")")
        }
    }
}

print(str_values)
str_values <- paste0(str_values, collapse="\n")
cat(str_values)

str_query <- paste0(str_query, str_values)
str_query <- paste0(str_query, ";")

cat(str_query)





