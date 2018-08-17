SET mongo.input.split_size = 4096;
/*
DROP TABLE IF EXISTS wp_ods.rules_engine_item;
CREATE TABLE wp_ods.rules_engine_item STORED AS PARQUET AS
SELECT * FROM wp_ext.rules_engine_item;

DROP TABLE IF EXISTS wp_ods.merged_data;
CREATE TABLE wp_ods.merged_data STORED AS PARQUET AS
SELECT * FROM wp_ext.merged_data;

DROP TABLE IF EXISTS wp_ods.rules_cache;
CREATE TABLE wp_ods.rules_cache STORED AS PARQUET AS
SELECT * FROM wp_ext.rules_cache;

DROP TABLE IF EXISTS wp_ods.application;
CREATE TABLE wp_ods.application STORED AS PARQUET AS
SELECT * FROM wp_ext.application;

DROP TABLE IF EXISTS wp_ods.loan_application;
CREATE TABLE wp_ods.loan_application STORED AS PARQUET AS
SELECT * FROM wp_ext.loan_application;
*/
INSERT OVERWRITE TABLE wp_ods.rules_engine_item
SELECT      * FROM wp_inc.rules_engine_item
UNION ALL
SELECT      t1.*
FROM        wp_ods.rules_engine_item t1
LEFT JOIN   wp_inc.rules_engine_item t2 ON t1.oid = t2.oid
WHERE       t2.oid IS NULL;

INSERT OVERWRITE TABLE wp_ods.rules_cache
SELECT      * FROM wp_inc.rules_cache
UNION ALL
SELECT      t1.*
FROM        wp_ods.rules_cache t1
LEFT JOIN   wp_inc.rules_cache t2 ON t1.oid = t2.oid
WHERE       t2.oid IS NULL;

INSERT OVERWRITE TABLE wp_ods.merged_data
SELECT      * FROM wp_inc.merged_data
UNION ALL
SELECT      t1.*
FROM        wp_ods.merged_data t1
LEFT JOIN   wp_inc.merged_data t2 ON t1.oid = t2.oid
WHERE       t2.oid IS NULL;

INSERT OVERWRITE TABLE wp_ods.application
SELECT      * FROM wp_inc.application
UNION ALL
SELECT      t1.*
FROM        wp_ods.application t1
LEFT JOIN   wp_inc.application t2 ON t1.oid = t2.oid
WHERE       t2.oid IS NULL;

INSERT OVERWRITE TABLE wp_ods.loan_application
SELECT      * FROM wp_inc.loan_application
UNION ALL
SELECT      t1.*
FROM        wp_ods.loan_application t1
LEFT JOIN   wp_inc.loan_application t2 ON t1.oid = t2.oid
WHERE       t2.oid IS NULL;

DROP TABLE IF EXISTS wp_ods.alipay_login;
CREATE TABLE wp_ods.alipay_login STORED AS PARQUET AS
SELECT * FROM wp_ext.alipay_login;

DROP TABLE IF EXISTS wp_ods.crawler_mobile_auth;
CREATE TABLE wp_ods.crawler_mobile_auth STORED AS PARQUET AS
SELECT * FROM wp_ext.crawler_mobile_auth;

DROP TABLE IF EXISTS wp_ods.credit_card_auth;
CREATE TABLE wp_ods.credit_card_auth STORED AS PARQUET AS
SELECT * FROM wp_ext.credit_card_auth;

DROP TABLE IF EXISTS wp_ods.house_fund51_auth;
CREATE TABLE wp_ods.house_fund51_auth STORED AS PARQUET AS
SELECT * FROM wp_ext.house_fund51_auth;

DROP TABLE IF EXISTS wp_ods.jd_login;
CREATE TABLE wp_ods.jd_login STORED AS PARQUET AS
SELECT * FROM wp_ext.jd_login;

DROP TABLE IF EXISTS wp_ods.people_bank_credit;
CREATE TABLE wp_ods.people_bank_credit STORED AS PARQUET AS
SELECT * FROM wp_ext.people_bank_credit;

DROP TABLE IF EXISTS wp_ods.shebao_auth;
CREATE TABLE wp_ods.shebao_auth STORED AS PARQUET AS
SELECT * FROM wp_ext.shebao_auth;

DROP TABLE IF EXISTS wp_ods.sina_auth;
CREATE TABLE wp_ods.sina_auth STORED AS PARQUET AS
SELECT * FROM wp_ext.sina_auth;

DROP TABLE IF EXISTS wp_ods.taobao_login;
CREATE TABLE wp_ods.taobao_login STORED AS PARQUET AS
SELECT * FROM wp_ext.taobao_login;

DROP TABLE IF EXISTS wp_ods.zmxy_auth;
CREATE TABLE wp_ods.zmxy_auth STORED AS PARQUET AS
SELECT * FROM wp_ext.zmxy_auth;

