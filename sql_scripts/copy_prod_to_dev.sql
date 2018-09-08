DELETE FROM dev_ASIN_Category WHERE 1=1;
DELETE FROM dev_ASIN WHERE 1=1;

SET IDENTITY_INSERT dbo.dev_ASIN ON;
    
INSERT INTO dbo.dev_ASIN (ASIN_id, ASIN)
    SELECT 
        ASIN_id, 
        ASIN 
    FROM prod_ASIN;

INSERT INTO dev_ASIN_Category
    SELECT ASIN_id, ASIN, Category1, Category2, Category3
    FROM prod_ASIN_Category;

SET IDENTITY_INSERT dbo.dev_ASIN OFF;

