# sfx_price_scraper
price scraper. using amazon product advertising API. storing in type-2 slowly changing dimensions database in AWS.


## version info:

* MS SQL Driver [here] (https://packages.microsoft.com/ubuntu/18.04/prod/pool/main/m/msodbcsql17/)
    - otherwise pick one from [here] (https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-2017)

* OS: Ubuntu 18.04.1 LTS
* R: 3.5.0
    - odbc: 1.1.6  (have to install unixodbc-dev first - and the MS SQL driver)
    - DBI: 1.0.0
    - dplyr: 0.7.6
    - tidyr: 0.8.1
    - ggplot2: 3.0.0
    - XML: 3.98.1.15  (have to install libxml2-dev first)
    - lubridate: 1.7.4
    - base64enc: 0.1.3 (for signing amazon requests)
    - digest: 0.6.15   (for signing amazon requests)
    - RCurl: 1.95.4.11 (have to install libcurl4-openssl-dev first)
* SQL Server Linux Driver: "ODBC Driver 17 for SQL Server"



