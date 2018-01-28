CREATE TABLE `sfxpricecomp`.`asin_list01` (
  `ASIN` VARCHAR(12) NOT NULL,
  `Epoch_Time` INT(10) NOT NULL,
  `Track_This` VARCHAR(1) NOT NULL COMMENT '\'Y\' for yes and \'N\' for no',
  PRIMARY KEY (`ASIN`));