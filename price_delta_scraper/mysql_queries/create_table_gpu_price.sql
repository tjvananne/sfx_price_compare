CREATE TABLE `gpu_price` (
`ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
`EffTimestampUTC` int(10) unsigned NOT NULL,
`ASIN` varchar(25) NOT NULL,

`Brand` varchar(50) DEFAULT NULL,
`ListPrice` int(11),
`ListPriceCd` varchar(5) DEFAULT NULL,
`LowestNewPrice` int(11),
`LowestNewPriceCd` varchar(5),
`LowestUsedPrice` int(11),
`LowestUsedPriceCd` varchar(5),
`LowestRefurbPrice` int(11),
`LowestRefurbPriceCd` varchar(5),
`Manufacturer` varchar(60) DEFAULT NULL,
`Model` varchar(60) DEFAULT NULL,
`MPN` varchar(60) DEFAULT NULL,
`NumberOfItems` int(11) DEFAULT NULL,
`PackageQuantity` int(11) DEFAULT NULL,
`PartNumber` varchar(60) DEFAULT NULL,
`ProductTypeName` varchar(60) DEFAULT NULL,
`Title` varchar(250) NULL,
`UPC` varchar(45) DEFAULT NULL,
`Warranty` varchar(60) DEFAULT NULL,
PRIMARY KEY (`ID`),
UNIQUE KEY `ID_UNIQUE` (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci





