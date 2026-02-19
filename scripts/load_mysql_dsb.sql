-- Load DSB SF10 data into MySQL from pipe-delimited .dat files
-- Data files are mounted at /data/ inside the container
-- Trailing | handled by LINES TERMINATED BY '|\n' or via extra dummy column approach

SET GLOBAL local_infile = 1;

-- Small dimension tables first, then large fact tables

LOAD DATA INFILE '/data/dbgen_version.dat' INTO TABLE dbgen_version FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/customer_address.dat' INTO TABLE customer_address FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/customer_demographics.dat' INTO TABLE customer_demographics FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/date_dim.dat' INTO TABLE date_dim FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/warehouse.dat' INTO TABLE warehouse FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/ship_mode.dat' INTO TABLE ship_mode FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/time_dim.dat' INTO TABLE time_dim FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/reason.dat' INTO TABLE reason FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/income_band.dat' INTO TABLE income_band FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/item.dat' INTO TABLE item FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/store.dat' INTO TABLE store FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/call_center.dat' INTO TABLE call_center FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/customer.dat' INTO TABLE customer FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/web_site.dat' INTO TABLE web_site FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/household_demographics.dat' INTO TABLE household_demographics FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/web_page.dat' INTO TABLE web_page FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/promotion.dat' INTO TABLE promotion FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/catalog_page.dat' INTO TABLE catalog_page FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/inventory.dat' INTO TABLE inventory FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

-- Large fact tables
LOAD DATA INFILE '/data/store_sales.dat' INTO TABLE store_sales FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/store_returns.dat' INTO TABLE store_returns FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/catalog_sales.dat' INTO TABLE catalog_sales FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/catalog_returns.dat' INTO TABLE catalog_returns FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/web_sales.dat' INTO TABLE web_sales FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
LOAD DATA INFILE '/data/web_returns.dat' INTO TABLE web_returns FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
