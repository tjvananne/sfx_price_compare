# this fixes the sha_256_module not found bug

CREATE USER 'taylor'@'localhost' IDENTIFIED WITH mysql_native_password BY 'securepw';
grant all on *.* to 'taylor'@'localhost';
