SET mongo.input.split_size = 4096;

DROP TABLE IF EXISTS wp_inc.rules_engine_item;
CREATE TABLE wp_inc.rules_engine_item STORED AS PARQUET AS
SELECT * FROM wp_ext.rules_engine_item
WHERE log_time > UNIX_TIMESTAMP(CURRENT_DATE) - 3600 * 48;

DROP TABLE IF EXISTS wp_inc.merged_data;
CREATE TABLE wp_inc.merged_data STORED AS PARQUET AS
SELECT * FROM wp_ext.merged_data
WHERE log_time > UNIX_TIMESTAMP(CURRENT_DATE) - 3600 * 48;

DROP TABLE IF EXISTS wp_inc.rules_cache;
CREATE TABLE wp_inc.rules_cache STORED AS PARQUET AS
SELECT * FROM wp_ext.rules_cache
WHERE time > UNIX_TIMESTAMP(CURRENT_DATE) - 3600 * 48;

DROP TABLE IF EXISTS wp_inc.loan_application;
CREATE TABLE wp_inc.loan_application STORED AS PARQUET AS
SELECT * FROM wp_ext.loan_application
WHERE log_time > UNIX_TIMESTAMP(CURRENT_DATE) - 3600 * 48;

DROP TABLE IF EXISTS wp_inc.application;
CREATE TABLE wp_inc.application STORED AS PARQUET AS
SELECT * FROM wp_ext.application
WHERE updated_at > TO_UTC_TIMESTAMP(DATE_SUB(CURRENT_DATE, 2), 'UTC');
