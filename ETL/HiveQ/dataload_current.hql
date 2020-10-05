create database if not exists ${hivevar:db};
use ${hivevar:db};

create table ${hivevar:db}.store_dim_c(store_id int, storename string, storelocation string, fisweek int)
row format delimited fields terminated by '|';

create table ${hivevar:db}.product_dim_c(product_id int, productname string, productlocation string,productprice FLOAT, fisweek int)
row format delimited fields terminated by '|';

create table ${hivevar:db}.store_dim_h(store_id int, storename string, storelocation string)
PARTITIONED BY (fisweek int)
row format delimited fields terminated by '|';

create table ${hivevar:db}.product_dim_h(product_id int, productname string, productlocation string,productprice FLOAT)
PARTITIONED BY (fisweek int)
row format delimited fields terminated by '|';


create table ${hivevar:db}.transaction_item_mft(transaction_id int,  transaction_date string, productname string, product_id int,store_id int,unit_price int)
PARTITIONED BY (fisweek int)
row format delimited fields terminated by '|';
