# https://dev.mysql.com/doc/refman/8.0/en/example-maximum-column-group-row.html
/*
SELECT s1.article, dealer, s1.price
FROM shop s1
JOIN (
  SELECT article, MAX(price) AS price
  FROM shop
  GROUP BY article) AS s2
  ON s1.article = s2.article AND s1.price = s2.price;
  */
  
  
SELECT *
FROM gpu_price tbl1
JOIN (
	SELECT ASIN, MAX(EffTimestampUTC) as EffTimestampUTC
    FROM gpu_price
    WHERE ASIN = 'B01MG0733A' 
    GROUP BY ASIN) AS tbl2
    ON tbl1.ASIN = tbl2.ASIN AND tbl1.EffTimestampUTC = tbl2.EffTimestampUTC;
  
  