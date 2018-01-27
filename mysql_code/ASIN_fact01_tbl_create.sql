CREATE TABLE `sfxpricecomp`.`asin_fact01` (
  `fact_pk` INT NOT NULL AUTO_INCREMENT,
  `ASIN` VARCHAR(12) NOT NULL COMMENT 'Not primary key because there will be multiple entries for each ASIN whenever the price changes',
  `Product_Price` INT(10) UNSIGNED NOT NULL,
  `Price_Eff_Start` INT(10) UNSIGNED NOT NULL,
  `Price_Eff_End` INT(10) UNSIGNED NOT NULL,
  `Is_Current` VARCHAR(1) NOT NULL COMMENT 'will be a \"Y\" for yes and a \"N\" for no - this really should be a boolean value, but I feel like a single character VARCHAR',
  PRIMARY KEY (`fact_pk`))
COMMENT = 'first attempt at a fact table that will utilize slowly changing dimensions for the price of the item';
