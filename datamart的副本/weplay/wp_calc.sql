-- INVALIDATE METADATA;

DROP TABLE IF EXISTS wp_calc.addresses;
CREATE TABLE wp_calc.addresses STORED AS PARQUET AS
SELECT * FROM (
SELECT      *,
            ROW_NUMBER() OVER (PARTITION BY addressable_type, addressable_id ORDER BY created_at DESC) rank_number
FROM        wp_std.addresses
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.addresses;

DROP TABLE IF EXISTS wp_calc.user_bank_card;
CREATE TABLE wp_calc.user_bank_card STORED AS PARQUET AS
SELECT * FROM (
SELECT      user_id,
            FIRST_VALUE(id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) id,
            FIRST_VALUE(account_number) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) account_number,
            FIRST_VALUE(province) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) province,
            FIRST_VALUE(city) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) city,
            FIRST_VALUE(state) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) state,
            FIRST_VALUE(bank_id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) bank_id,
            FIRST_VALUE(pay_code) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) pay_code,
            FIRST_VALUE(renew) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) renew,
            FIRST_VALUE(mobile) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) mobile,
            FIRST_VALUE(created_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) created_at,
            FIRST_VALUE(updated_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) updated_at,
            COUNT(id) OVER (PARTITION BY user_id) tot_cnt,
            LEAD(id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_id,
            LEAD(account_number) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_account_number,
            LEAD(state) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_state,
            LEAD(bank_id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_bank_id,
            LEAD(pay_code) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_pay_code,
            LEAD(renew) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_renew,
            LEAD(mobile) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_mobile,
            LEAD(created_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_created_at,
            LEAD(updated_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_updated_at,
            FIRST_VALUE(created_at + INTERVAL 8 HOUR) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_at,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) rank_number
FROM        wp_std.bank_cards
WHERE       state != 'fail'
AND         deleted_at IS NULL
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_bank_card;

DROP TABLE IF EXISTS wp_calc.user_company;
CREATE TABLE wp_calc.user_company STORED AS PARQUET AS
SELECT * FROM (
SELECT      user_id,
            FIRST_VALUE(id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) id,
            FIRST_VALUE(name) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) name,
            FIRST_VALUE(department) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) department,
            FIRST_VALUE(company_position) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) company_position,
            FIRST_VALUE(departure_time) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) departure_time,
            FIRST_VALUE(position_id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) position_id,
            FIRST_VALUE(telephone) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) telephone,
            FIRST_VALUE(salary_day) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) salary_day,
            FIRST_VALUE(created_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) created_at,
            FIRST_VALUE(updated_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) updated_at,
            COUNT(id) OVER (PARTITION BY user_id) tot_cnt,
            LEAD(id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_id,
            LEAD(name) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_name,
            LEAD(created_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_created_at,
            LEAD(updated_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_updated_at,
            FIRST_VALUE(created_at + INTERVAL 8 HOUR) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_at,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) rank_number
FROM        wp_std.companies
)t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_company;

DROP TABLE IF EXISTS wp_calc.user_document;
CREATE TABLE wp_calc.user_document STORED AS PARQUET AS
SELECT      documentable_id,
            MIN(CASE WHEN doc_type = 'id_front_proof' THEN created_at ELSE NULL END) first_up_id_front_proof_at,
            MIN(CASE WHEN doc_type = 'id_back_proof' THEN created_at ELSE NULL END) first_up_id_back_proof_at,
            MIN(CASE WHEN doc_type = 'id_handheld_proof' THEN created_at ELSE NULL END) first_up_id_handheld_proof_at,
            MIN(CASE WHEN doc_type = 'employment_proof' THEN created_at ELSE NULL END) first_up_id_employment_proof_at,
            MAX(CASE WHEN doc_type = 'id_front_proof' THEN created_at ELSE NULL END) latest_up_id_front_proof_at,
            MAX(CASE WHEN doc_type = 'id_back_proof' THEN created_at ELSE NULL END) latest_up_id_back_proof_at,
            MAX(CASE WHEN doc_type = 'id_handheld_proof' THEN created_at ELSE NULL END) latest_up_id_handheld_proof_at,
            MAX(CASE WHEN doc_type = 'employment_proof' THEN created_at ELSE NULL END) latest_up_id_employment_proof_at,
            SUM(CASE WHEN doc_type = 'id_front_proof' THEN 1 ELSE 0 END) up_cnt_id_front_proof_at,
            SUM(CASE WHEN doc_type = 'id_back_proof' THEN 1 ELSE 0 END) up_cnt_id_back_proof_at,
            SUM(CASE WHEN doc_type = 'id_handheld_proof' THEN 1 ELSE 0 END) up_cnt_id_handheld_proof_at,
            SUM(CASE WHEN doc_type = 'employment_proof' THEN 1 ELSE 0 END) up_cnt_id_employment_proof_at,
            COUNT(*) up_tot_cnt,
            GROUP_CONCAT(DISTINCT doc_type) up_doc_type_list,
            MIN(created_at) first_up_at,
            MAX(created_at) latest_up_at
FROM        wp_std.documents
WHERE       documentable_type = 'User'
GROUP BY    documentable_id
;
COMPUTE STATS wp_calc.user_document;

DROP TABLE IF EXISTS wp_calc.user_education;
CREATE TABLE wp_calc.user_education STORED AS PARQUET AS
SELECT * FROM (
SELECT      user_id,
            FIRST_VALUE(id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) id,
            FIRST_VALUE(school) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) school,
            FIRST_VALUE(degree_id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) degree_id,
            FIRST_VALUE(enrolled_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) enrolled_at,
            FIRST_VALUE(graduated_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) graduated_at,
            FIRST_VALUE(created_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) created_at,
            FIRST_VALUE(updated_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) updated_at,
            COUNT(id) OVER (PARTITION BY user_id) tot_cnt,
            LEAD(id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_id,
            LEAD(school) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_school,
            LEAD(degree_id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_degree_id,
            LEAD(created_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) prev_created_at,
            FIRST_VALUE(created_at) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_at,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) rank_number
FROM        wp_std.educations
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_education;

DROP TABLE IF EXISTS wp_calc.user_liaison;
CREATE TABLE wp_calc.user_liaison STORED AS PARQUET AS
SELECT * FROM (
SELECT      user_id,
            COUNT(id) OVER (PARTITION BY user_id) tot_cnt,
            FIRST_VALUE(id) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_id,
            FIRST_VALUE(name) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_name,
            FIRST_VALUE(relationship) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_relationship,
            FIRST_VALUE(mobile) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_mobile,
            FIRST_VALUE(company) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_company,
            FIRST_VALUE(position) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_position,
            FIRST_VALUE(source_id) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_source_id,
            FIRST_VALUE(created_at) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_created_at,
            FIRST_VALUE(updated_at) OVER (PARTITION BY user_id ORDER BY created_at, id) first_up_updated_at,
            FIRST_VALUE(id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) latest_up_id,
            FIRST_VALUE(name) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) latest_up_name,
            FIRST_VALUE(relationship) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) latest_up_relationship,
            FIRST_VALUE(mobile) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) latest_up_mobile,
            FIRST_VALUE(company) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) latest_up_company,
            FIRST_VALUE(position) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) latest_up_position,
            FIRST_VALUE(source_id) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) latest_up_source_id,
            FIRST_VALUE(created_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) latest_up_created_at,
            FIRST_VALUE(updated_at) OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) latest_up_updated_at,
            FIRST_VALUE(id) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_1_id,
            FIRST_VALUE(name) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_1_name,
            FIRST_VALUE(relationship) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_1_relationship,
            FIRST_VALUE(mobile) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_1_mobile,
            FIRST_VALUE(company) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_1_company,
            FIRST_VALUE(position) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_1_position,
            FIRST_VALUE(source_id) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_1_source_id,
            FIRST_VALUE(created_at) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_1_created_at,
            FIRST_VALUE(updated_at) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_1_updated_at,
            LEAD(id) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_2_id,
            LEAD(name) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_2_name,
            LEAD(relationship) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_2_relationship,
            LEAD(mobile) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_2_mobile,
            LEAD(company) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_2_company,
            LEAD(position) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_2_position,
            LEAD(source_id) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_2_source_id,
            LEAD(created_at) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_2_created_at,
            LEAD(updated_at) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_2_updated_at,
            LEAD(id, 2) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_3_id,
            LEAD(name, 2) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_3_name,
            LEAD(relationship, 2) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_3_relationship,
            LEAD(mobile, 2) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_3_mobile,
            LEAD(company, 2) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_3_company,
            LEAD(position, 2) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_3_position,
            LEAD(source_id, 2) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_3_source_id,
            LEAD(created_at, 2) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_3_created_at,
            LEAD(updated_at, 12) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_3_updated_at,
            LEAD(id, 3) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_4_id,
            LEAD(name, 3) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_4_name,
            LEAD(relationship, 3) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_4_relationship,
            LEAD(mobile, 3) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_4_mobile,
            LEAD(company, 3) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_4_company,
            LEAD(position, 3) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_4_position,
            LEAD(source_id, 3) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_4_source_id,
            LEAD(created_at, 3) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_4_created_at,
            LEAD(updated_at, 3) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_4_updated_at,
            LEAD(id, 4) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_5_id,
            LEAD(name, 4) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_5_name,
            LEAD(relationship, 4) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_5_relationship,
            LEAD(mobile, 4) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_5_mobile,
            LEAD(company, 4) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_5_company,
            LEAD(position, 4) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_5_position,
            LEAD(source_id, 4) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_5_source_id,
            LEAD(created_at, 4) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_5_created_at,
            LEAD(updated_at, 4) OVER (PARTITION BY user_id ORDER BY relationship_rank) liaison_5_updated_at,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) rank_number
FROM        wp_std.liaisons
WHERE       deleted_at IS NULL
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_liaison;

DROP TABLE IF EXISTS wp_calc.user_attribute;
CREATE TABLE wp_calc.user_attribute STORED AS PARQUET AS
SELECT      user_id,
            MAX(CASE WHEN att_type_id = 06 THEN CAST(value AS DOUBLE) ELSE NULL END) income_month,
            MAX(CASE WHEN att_type_id = 09 THEN CAST(value AS DOUBLE) ELSE NULL END) asset_auto_type,
            MAX(CASE WHEN att_type_id = 10 THEN CAST(value AS DOUBLE) ELSE NULL END) work_period,
            MAX(CASE WHEN att_type_id = 11 THEN CAST(value AS DOUBLE) ELSE NULL END) max_monthly_repayment,
            MAX(CASE WHEN att_type_id = 12 THEN CAST(value AS DOUBLE) ELSE NULL END) operating_year,
            MAX(CASE WHEN att_type_id = 13 THEN CAST(value AS DOUBLE) ELSE NULL END) user_social_security,
            MAX(CASE WHEN att_type_id = 14 THEN CAST(value AS DOUBLE) ELSE NULL END) monthly_average_income,
            MAX(CASE WHEN att_type_id = 15 THEN CAST(value AS DOUBLE) ELSE NULL END) credit_status,
            MAX(CASE WHEN att_type_id = 19 THEN value ELSE NULL END) income_month_scope,
            MAX(CASE WHEN att_type_id = 20 THEN CAST(value AS DOUBLE) ELSE NULL END) estimate_amount,
            MAX(CASE WHEN att_type_id = 24 THEN CAST(value AS DOUBLE) ELSE NULL END) xunlei_amount,
            MAX(CASE WHEN att_type_id = 06 THEN created_at ELSE NULL END) income_month_created_at,
            MAX(CASE WHEN att_type_id = 09 THEN created_at ELSE NULL END) asset_auto_type_created_at,
            MAX(CASE WHEN att_type_id = 10 THEN created_at ELSE NULL END) work_period_created_at,
            MAX(CASE WHEN att_type_id = 11 THEN created_at ELSE NULL END) max_monthly_repayment_created_at,
            MAX(CASE WHEN att_type_id = 12 THEN created_at ELSE NULL END) operating_year_created_at,
            MAX(CASE WHEN att_type_id = 13 THEN created_at ELSE NULL END) user_social_security_created_at,
            MAX(CASE WHEN att_type_id = 14 THEN created_at ELSE NULL END) monthly_average_income_created_at,
            MAX(CASE WHEN att_type_id = 15 THEN created_at ELSE NULL END) credit_status_created_at,
            MAX(CASE WHEN att_type_id = 19 THEN created_at ELSE NULL END) income_month_scope_created_at,
            MAX(CASE WHEN att_type_id = 20 THEN created_at ELSE NULL END) estimate_amount_created_at,
            MAX(CASE WHEN att_type_id = 24 THEN created_at ELSE NULL END) xunlei_amount_created_at,
            MAX(CASE WHEN att_type_id = 06 THEN updated_at ELSE NULL END) income_month_updated_at,
            MAX(CASE WHEN att_type_id = 09 THEN updated_at ELSE NULL END) asset_auto_type_updated_at,
            MAX(CASE WHEN att_type_id = 10 THEN updated_at ELSE NULL END) work_period_updated_at,
            MAX(CASE WHEN att_type_id = 11 THEN updated_at ELSE NULL END) max_monthly_repayment_updated_at,
            MAX(CASE WHEN att_type_id = 12 THEN updated_at ELSE NULL END) operating_year_updated_at,
            MAX(CASE WHEN att_type_id = 13 THEN updated_at ELSE NULL END) user_social_security_updated_at,
            MAX(CASE WHEN att_type_id = 14 THEN updated_at ELSE NULL END) monthly_average_income_updated_at,
            MAX(CASE WHEN att_type_id = 15 THEN updated_at ELSE NULL END) credit_status_updated_at,
            MAX(CASE WHEN att_type_id = 19 THEN updated_at ELSE NULL END) income_month_scope_updated_at,
            MAX(CASE WHEN att_type_id = 20 THEN updated_at ELSE NULL END) estimate_amount_updated_at,
            MAX(CASE WHEN att_type_id = 24 THEN updated_at ELSE NULL END) xunlei_amount_updated_at
FROM        wp_std.user_attributes
GROUP BY    user_id
;
COMPUTE STATS wp_calc.user_attribute;

DROP TABLE IF EXISTS wp_calc.rules_engine_item;
CREATE TABLE wp_calc.rules_engine_item STORED AS PARQUET AS
SELECT      *
FROM        wp_std.rules_engine_item
WHERE       rank_number = 1
;
COMPUTE STATS wp_calc.rules_engine_item;

DROP TABLE IF EXISTS wp_calc.merged_data;
CREATE TABLE wp_calc.merged_data STORED AS PARQUET AS
SELECT      * 
FROM        wp_std.merged_data
WHERE       rank_number = 1
;
COMPUTE STATS wp_calc.merged_data;

DROP TABLE IF EXISTS wp_calc.rules_cache;
CREATE TABLE wp_calc.rules_cache STORED AS PARQUET AS
SELECT      *
FROM        wp_std.rules_cache
WHERE       rank_number = 1
;
COMPUTE STATS wp_calc.rules_cache;

DROP TABLE IF EXISTS wp_calc.rules_cache_hit_rules;
CREATE TABLE wp_calc.rules_cache_hit_rules STORED AS PARQUET AS
SELECT      *
FROM        wp_std.rules_cache_hit_rules
;
COMPUTE STATS wp_calc.rules_cache_hit_rules;

DROP TABLE IF EXISTS wp_calc.rules_cache_extra_info_fm_info_risk_service_hit_rules;
CREATE TABLE wp_calc.rules_cache_extra_info_fm_info_risk_service_hit_rules STORED AS PARQUET AS
SELECT      *
FROM        wp_std.rules_cache_extra_info_fm_info_risk_service_hit_rules
;
COMPUTE STATS wp_calc.rules_cache_extra_info_fm_info_risk_service_hit_rules;

DROP TABLE IF EXISTS wp_calc.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules;
CREATE TABLE wp_calc.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules STORED AS PARQUET AS
SELECT      *
FROM        wp_std.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules
;
COMPUTE STATS wp_calc.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules;

DROP TABLE IF EXISTS wp_calc.user_up_credit_card;
CREATE TABLE wp_calc.user_up_credit_card STORED AS PARQUET AS
SELECT * FROM (
SELECT      account,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time) first_up_credit_card_at,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time DESC) latest_up_credit_card_at,
            ROW_NUMBER() OVER (PARTITION BY account ORDER BY log_time DESC) rank_number
FROM        wp_std.credit_card_auth
WHERE       state = 'auth_success'
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_up_credit_card;

DROP TABLE IF EXISTS wp_calc.user_up_alipay;
CREATE TABLE wp_calc.user_up_alipay STORED AS PARQUET AS
SELECT * FROM (
SELECT      account,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time) first_up_alipay_at,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time DESC) latest_up_alipay_at,
            ROW_NUMBER() OVER (PARTITION BY account ORDER BY log_time DESC) rank_number
FROM        wp_std.alipay_login
WHERE       status = 0
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_up_alipay;

DROP TABLE IF EXISTS wp_calc.user_up_isp;
CREATE TABLE wp_calc.user_up_isp STORED AS PARQUET AS
SELECT * FROM (
SELECT      account,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time) first_up_isp_at,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time DESC) latest_up_isp_at,
            ROW_NUMBER() OVER (PARTITION BY account ORDER BY log_time DESC) rank_number
FROM        wp_std.crawler_mobile_auth
WHERE       status_id > 0
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_up_isp;

DROP TABLE IF EXISTS wp_calc.user_up_gjj;
CREATE TABLE wp_calc.user_up_gjj STORED AS PARQUET AS
SELECT * FROM (
SELECT      account,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time) first_up_gjj_at,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time DESC) latest_up_gjj_at,
            ROW_NUMBER() OVER (PARTITION BY account ORDER BY log_time DESC) rank_number
FROM        wp_std.house_fund51_auth
WHERE       gjj_sid > 0
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_up_gjj;

DROP TABLE IF EXISTS wp_calc.user_up_jd;
CREATE TABLE wp_calc.user_up_jd STORED AS PARQUET AS
SELECT * FROM (
SELECT      account,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time) first_up_jd_at,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time DESC) latest_up_jd_at,
            ROW_NUMBER() OVER (PARTITION BY account ORDER BY log_time DESC) rank_number
FROM        wp_std.jd_login
WHERE       status = 0
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_up_jd;

DROP TABLE IF EXISTS wp_calc.user_up_shebao;
CREATE TABLE wp_calc.user_up_shebao STORED AS PARQUET AS
SELECT * FROM (
SELECT      account,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time) first_up_shebao_at,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time DESC) latest_up_shebao_at,
            ROW_NUMBER() OVER (PARTITION BY account ORDER BY log_time DESC) rank_number
FROM        wp_std.shebao_auth
WHERE       shebao_sid > 0
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_up_shebao;

DROP TABLE IF EXISTS wp_calc.user_up_weibo;
CREATE TABLE wp_calc.user_up_weibo STORED AS PARQUET AS
SELECT * FROM (
SELECT      account,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time) first_up_weibo_at,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time DESC) latest_up_weibo_at,
            ROW_NUMBER() OVER (PARTITION BY account ORDER BY log_time DESC) rank_number
FROM        wp_std.sina_auth
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_up_weibo;

DROP TABLE IF EXISTS wp_calc.user_up_taobao;
CREATE TABLE wp_calc.user_up_taobao STORED AS PARQUET AS
SELECT * FROM (
SELECT      account,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time) first_up_taobao_at,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time DESC) latest_up_taobao_at,
            ROW_NUMBER() OVER (PARTITION BY account ORDER BY log_time DESC) rank_number
FROM        wp_std.taobao_login
WHERE       status = 0
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_up_taobao;

DROP TABLE IF EXISTS wp_calc.user_up_zmxy;
CREATE TABLE wp_calc.user_up_zmxy STORED AS PARQUET AS
SELECT * FROM (
SELECT      account,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time) first_up_zmxy_at,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time DESC) latest_up_zmxy_at,
            ROW_NUMBER() OVER (PARTITION BY account ORDER BY log_time DESC) rank_number
FROM        wp_std.zmxy_auth
WHERE       authorized = true
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_up_zmxy;

DROP TABLE IF EXISTS wp_calc.user_up_people_bank;
CREATE TABLE wp_calc.user_up_people_bank STORED AS PARQUET AS
SELECT * FROM (
SELECT      account,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time) first_up_people_bank_at,
            FIRST_VALUE(log_time) OVER (PARTITION BY account ORDER BY log_time DESC) latest_up_people_bank_at,
            ROW_NUMBER() OVER (PARTITION BY account ORDER BY log_time DESC) rank_number
FROM        wp_std.people_bank_credit
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_up_people_bank;

DROP TABLE IF EXISTS wp_calc.loan_applications;
CREATE TABLE wp_calc.loan_applications STORED AS PARQUET AS
SELECT      'loan_applications' source,
            t1.id,
            t1.application_id,
            t1.borrower_id,
            t1.tenor,
            t1.amount,
            t1.state,
            t1.created_at,
            t1.updated_at,
            t1.handling_fee,
            t1.interest_rate,
            t1.management_fee_rate,
            t1.withdrawal_fee_rate,
            t1.applied_at,
            t1.approved_at,
            t1.reason_code1,
            t1.reason_code2,
            t1.reason_code3,
            t1.reject_code,
            t1.aip_by_id,
            t1.picked_up_by_id,
            t1.aip_picked_up_by_id,
            t1.user_evaluation_rank,
            t1.applied_amount,
            t1.applied_tenor,
            t1.push_backed_at,
            NULL push_back_reason_codes,
            t1.confirmed_at,
            t1.origin,
            t1.welab_product_id,
            NULL product_code,
            t1.biz_type,
            t1.approval_type,
            t1.partner_code,
            t1.experiment_amount,
            t1.experiment_info,
            t1.capital_pay_code,
            t1.sys_type,
            t2.step,
            t2.level,
            t2.pricing_level,
            t2.reason_code,
            t2.state approval_state,
            t2.approval_type approval_type_mongo,
            t2.approval_note,
            t2.approved_at approved_at_mongo,
            t2.source_id,
            t2.call_count,
            t2.product_code product_code_mongo,
            t2.product_name,
            t2.apply_type,
            t3.platform,
            t3.product,
            t3.device_id,
            t3.log_time,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_mobile ELSE t3.account END proposer_mobile,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_name ELSE t3.detail_profile_name END proposer_name,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_cnid ELSE t3.detail_profile_cnid END proposer_cnid,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_register_time ELSE t3.detail_base_info_user_register_time END proposer_register_time,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_company_income ELSE t3.detail_base_info_income_month END proposer_company_income,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_marriage ELSE t3.detail_profile_marriage END proposer_marriage,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_industry ELSE t3.detail_industry_industry_code END proposer_industry,
            t2.data_proposer_credit_line proposer_credit_line,
            t2.data_proposer_available_credit_line proposer_available_credit_line,
            t2.data_proposer_qq proposer_qq,
            t2.data_proposer_age proposer_age,
            t2.data_proposer_resident_description proposer_resident_description,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_address_province ELSE t3.detail_resident_address_province END proposer_address_province,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_address_city ELSE t3.detail_resident_address_city END proposer_address_city,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_address_district ELSE t3.detail_resident_address_district END proposer_address_district,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_proposer_address_location ELSE t3.detail_resident_address_location END proposer_address_location,
            t2.data_proposer_address_description proposer_address_description,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_device_info_app_version ELSE t3.detail_application_device_info_app_version END device_info_app_version,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_device_info_gps ELSE t3.detail_application_device_info_gps END device_info_gps,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_device_info_ip ELSE t3.detail_application_device_info_ip END device_info_ip,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_device_info_mac ELSE t3.detail_application_device_info_mac END device_info_mac,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_device_info_model ELSE t3.detail_application_device_info_model END device_info_model,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_device_info_os_version ELSE t3.detail_application_device_info_os_version END device_info_os_version,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_device_info_source_id ELSE t3.detail_application_device_info_source_id END device_info_source_id,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_device_info_uuid ELSE t3.detail_application_device_info_uuid END device_info_uuid,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_device_info_wdid ELSE t3.detail_application_device_info_wdid END device_info_wdid,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_name ELSE t3.detail_latest_company_name END company_name,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_telephone ELSE t3.detail_latest_company_telephone END company_telephone,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_entry_time ELSE t3.detail_latest_company_entry_time END company_entry_time,
            t2.data_company_department company_department,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_position_id ELSE t3.detail_profile_position_id END company_position_id,
            t2.data_company_position company_position,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_created_at ELSE t3.detail_latest_company_created_at END company_created_at,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_updated_at ELSE t3.detail_latest_company_updated_at END company_updated_at,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_address_province ELSE t3.detail_company_address_province END company_address_province,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_address_city ELSE t3.detail_company_address_city END company_address_city,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_address_district ELSE t3.detail_company_address_district END company_address_district,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_address_street ELSE t3.detail_company_address_street END company_address_street,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_address_description ELSE t3.detail_company_address_description END company_address_description,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_address_telephone ELSE t3.detail_company_address_telephone END company_address_telephone,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_address_created_at ELSE t3.detail_company_address_created_at END company_address_created_at,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_company_address_updated_at ELSE t3.detail_company_address_updated_at END company_address_updated_at,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_educations_cnt ELSE t3.detail_educations_cnt END educations_cnt,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_educations_degree_list ELSE t3.detail_educations_degree_id_list END educations_degree_list,
            t2.data_educations_school_list,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_documents_cnt ELSE t3.detail_documents_cnt END documents_cnt,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_documents_doc_type_list ELSE t3.detail_documents_doc_type_list END documents_doc_type_list,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_liaisons_cnt ELSE t3.detail_liaisons_cnt END liaisons_cnt,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_liaisons_relationship_list ELSE t3.detail_liaisons_relationship_list END liaisons_relationship_list,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_liaisons_name_list ELSE t3.detail_liaisons_name_list END liaisons_name_list,
            CASE WHEN t1.sys_type = 2 AND t1.biz_type IS NULL THEN t2.data_liaisons_mobile_list ELSE t3.detail_liaisons_mobile_list END liaisons_mobile_list,
            t2.wedefend_approval_operation,
            t2.wedefend_approval_reason_code,
            t2.wedefend_approval_amount,
            t2.wedefend_approval_tenor,
            t2.wedefend_approval_created_at,
            t2.wedefend_approval_updated_at,
            t2.init_approval_operator,
            t2.init_approval_operation,
            t2.init_approval_reason_code,
            t2.init_approval_amount,
            t2.init_approval_tenor,
            t2.init_approval_remark,
            t2.init_approval_created_at,
            t2.init_approval_updated_at,
            t2.final_approval_operator,
            t2.final_approval_operation,
            t2.final_approval_reason_code,
            t2.final_approval_user_credit_amount,
            t2.final_approval_amount,
            t2.final_approval_tenor,
            t2.final_approval_remark,
            t2.final_approval_created_at,
            t2.final_approval_updated_at
FROM        wp_std.loan_applications    t1
LEFT JOIN   wp_std.application          t2 ON t1.application_id = t2.oid
LEFT JOIN   wp_std.loan_application     t3 ON t1.application_id = t3.id AND rank_number = 1
UNION ALL
SELECT      'credit_applications' source,
            t1.id,
            t1.application_id,
            t4.id borrower_id,
            NULL tenor,
            NULL amount,
            t1.state,
            t1.created_at,
            t1.updated_at,
            NULL handling_fee,
            NULL interest_rate,
            NULL management_fee_rate,
            NULL withdrawal_fee_rate,
            t1.applied_at,
            t1.approved_at,
            NULL reason_code1,
            NULL reason_code2,
            NULL reason_code3,
            NULL reject_code,
            NULL aip_by_id,
            NULL picked_up_by_id,
            NULL aip_picked_up_by_id,
            NULL user_evaluation_rank,
            NULL applied_amount,
            NULL applied_tenor,
            NULL push_backed_at,
            t1.push_back_reason_codes,
            NULL confirmed_at,
            t1.origin,
            0 welab_product_id,
            t1.product_code,
            'credit_application' biz_type,
            NULL approval_type,
            NULL partner_code,
            NULL experiment_amount,
            NULL experiment_info,
            NULL capital_pay_code,
            NULL sys_type,
            t2.step,
            t2.level,
            t2.pricing_level,
            t2.reason_code,
            t2.state approval_state,
            t2.approval_type approval_type_mongo,
            t2.approval_note,
            t2.approved_at approved_at_mongo,
            t2.source_id,
            t2.call_count,
            t2.product_code product_code_mongo,
            t2.product_name,
            t2.apply_type,
            t3.platform,
            t3.product,
            t3.device_id,
            t3.log_time,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_mobile ELSE t3.account END proposer_mobile,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_name ELSE t3.detail_profile_name END proposer_name,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_cnid ELSE t3.detail_profile_cnid END proposer_cnid,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_register_time ELSE t3.detail_base_info_user_register_time END proposer_register_time,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_company_income ELSE t3.detail_base_info_income_month END proposer_company_income,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_marriage ELSE t3.detail_profile_marriage END proposer_marriage,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_industry ELSE t3.detail_industry_industry_code END proposer_industry,
            t2.data_proposer_credit_line proposer_credit_line,
            t2.data_proposer_available_credit_line proposer_available_credit_line,
            t2.data_proposer_qq proposer_qq,
            t2.data_proposer_age proposer_age,
            t2.data_proposer_resident_description proposer_resident_description,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_address_province ELSE t3.detail_resident_address_province END address_province,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_address_city ELSE t3.detail_resident_address_city END address_city,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_address_district ELSE t3.detail_resident_address_district END address_district,
            CASE WHEN t1.sys_type = 2 THEN t2.data_proposer_address_location ELSE t3.detail_resident_address_location END address_location,
            t2.data_proposer_address_description address_description,
            CASE WHEN t1.sys_type = 2 THEN t2.data_device_info_app_version ELSE t3.detail_application_device_info_app_version END device_info_app_version,
            CASE WHEN t1.sys_type = 2 THEN t2.data_device_info_gps ELSE t3.detail_application_device_info_gps END device_info_gps,
            CASE WHEN t1.sys_type = 2 THEN t2.data_device_info_ip ELSE t3.detail_application_device_info_ip END device_info_ip,
            CASE WHEN t1.sys_type = 2 THEN t2.data_device_info_mac ELSE t3.detail_application_device_info_mac END device_info_mac,
            CASE WHEN t1.sys_type = 2 THEN t2.data_device_info_model ELSE t3.detail_application_device_info_model END device_info_model,
            CASE WHEN t1.sys_type = 2 THEN t2.data_device_info_os_version ELSE t3.detail_application_device_info_os_version END device_info_os_version,
            CASE WHEN t1.sys_type = 2 THEN t2.data_device_info_source_id ELSE t3.detail_application_device_info_source_id END device_info_source_id,
            CASE WHEN t1.sys_type = 2 THEN t2.data_device_info_uuid ELSE t3.detail_application_device_info_uuid END device_info_uuid,
            CASE WHEN t1.sys_type = 2 THEN t2.data_device_info_wdid ELSE t3.detail_application_device_info_wdid END device_info_wdid,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_name ELSE t3.detail_latest_company_name END company_name,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_telephone ELSE t3.detail_latest_company_telephone END company_telephone,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_entry_time ELSE t3.detail_latest_company_entry_time END company_entry_time,
            t2.data_company_department company_department,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_position_id ELSE t3.detail_latest_company_position_id END company_position_id,
            t2.data_company_position company_position,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_created_at ELSE t3.detail_latest_company_created_at END company_created_at,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_updated_at ELSE t3.detail_latest_company_updated_at END company_updated_at,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_address_province ELSE t3.detail_company_address_province END company_address_province,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_address_city ELSE t3.detail_company_address_city END company_address_city,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_address_district ELSE t3.detail_company_address_district END company_address_district,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_address_street ELSE t3.detail_company_address_street END company_address_street,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_address_description ELSE t3.detail_company_address_description END company_address_description,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_address_telephone ELSE t3.detail_company_address_telephone END company_address_telephone,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_address_created_at ELSE t3.detail_company_address_created_at END company_address_created_at,
            CASE WHEN t1.sys_type = 2 THEN t2.data_company_address_updated_at ELSE t3.detail_company_address_updated_at END company_address_updated_at,
            CASE WHEN t1.sys_type = 2 THEN t2.data_educations_cnt ELSE t3.detail_educations_cnt END educations_cnt,
            CASE WHEN t1.sys_type = 2 THEN t2.data_educations_degree_list ELSE t3.detail_educations_degree_id_list END educations_degree_list,
            t2.data_educations_school_list,
            CASE WHEN t1.sys_type = 2 THEN t2.data_documents_cnt ELSE t3.detail_documents_cnt END documents_cnt,
            CASE WHEN t1.sys_type = 2 THEN t2.data_documents_doc_type_list ELSE t3.detail_documents_doc_type_list END documents_doc_type_list,
            CASE WHEN t1.sys_type = 2 THEN t2.data_liaisons_cnt ELSE t3.detail_liaisons_cnt END liaisons_cnt,
            CASE WHEN t1.sys_type = 2 THEN t2.data_liaisons_relationship_list ELSE t3.detail_liaisons_relationship_list END liaisons_relationship_list,
            CASE WHEN t1.sys_type = 2 THEN t2.data_liaisons_name_list ELSE t3.detail_liaisons_name_list END liaisons_name_list,
            CASE WHEN t1.sys_type = 2 THEN t2.data_liaisons_mobile_list ELSE t3.detail_liaisons_mobile_list END liaisons_mobile_list,
            t2.wedefend_approval_operation,
            t2.wedefend_approval_reason_code,
            t2.wedefend_approval_amount,
            t2.wedefend_approval_tenor,
            t2.wedefend_approval_created_at,
            t2.wedefend_approval_updated_at,
            t2.init_approval_operator,
            t2.init_approval_operation,
            t2.init_approval_reason_code,
            t2.init_approval_amount,
            t2.init_approval_tenor,
            t2.init_approval_remark,
            t2.init_approval_created_at,
            t2.init_approval_updated_at,
            t2.final_approval_operator,
            t2.final_approval_operation,
            t2.final_approval_reason_code,
            t2.final_approval_user_credit_amount,
            t2.final_approval_amount,
            t2.final_approval_tenor,
            t2.final_approval_remark,
            t2.final_approval_created_at,
            t2.final_approval_updated_at
FROM        wp_std.credit_applications t1
LEFT JOIN   wp_std.application      t2 ON t1.application_id = t2.oid
LEFT JOIN   wp_std.loan_application t3 ON t1.application_id = t3.id AND rank_number = 1
LEFT JOIN   wp_std.users            t4 ON t1.uuid = t4.uuid
;
COMPUTE STATS wp_calc.loan_applications;

DROP TABLE IF EXISTS wp_calc.welab_products;
CREATE TABLE wp_calc.welab_products STORED AS PARQUET AS
SELECT      t1.id,
            t1.name,
            t1.code,
            t1.product_type,
            t1.min_amount,
            t1.max_amount,
            t1.tenor,
            t2.prod_code,
            t2.prod_name,
            t2.prod_cate,
            t2.level,
            t2.platform,
            t2.loan_type,
            t2.credit_type,
            t2.enable,
            t1.created_at,
            t1.updated_at
FROM        wp_std.welab_products   t1
LEFT JOIN   wp_calc.product_info    t2 ON t1.id = t2.id
;
COMPUTE STATS wp_calc.welab_products;

DROP TABLE IF EXISTS wp_calc.loan_application_prev_aprv_credit;
CREATE TABLE wp_calc.loan_application_prev_aprv_credit STORED AS PARQUET AS
SELECT      application_id,
            welab_product_id,
            origin,
            LAST_VALUE((CASE WHEN biz_type = 'credit_application' AND t2.is_aprv = 1 THEN application_id ELSE NULL END) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aprv_credit
FROM        wp_calc.loan_applications       t1
LEFT JOIN   wp_calc.loan_application_state  t2 ON t1.state = t2.code
;
COMPUTE STATS wp_calc.loan_application_prev_aprv_credit;

DROP TABLE IF EXISTS wp_calc.loan_map_application;
CREATE TABLE wp_calc.loan_map_application STORED AS PARQUET AS
SELECT      t1.application_id,
            t2.welab_product_id,
            t2.origin,
            CASE WHEN t3.credit_type = 'credit' THEN t2.prev_aprv_credit
                 ELSE t1.application_id
            END             map_application_id
FROM        wp_std.loans                                t1
LEFT JOIN   wp_calc.loan_application_prev_aprv_credit   t2 ON t1.application_id = t2.application_id
LEFT JOIN   wp_calc.welab_products                      t3 ON t2.welab_product_id = t3.id
;
COMPUTE STATS wp_calc.loan_map_application;

DROP TABLE IF EXISTS wp_calc.loan_mob_set_group;
CREATE TABLE wp_calc.loan_mob_set_group STORED AS PARQUET AS
SELECT      due_id              due_id,
            SUM(amount)         amount,
            MAX(created_at)     set_at
FROM        wp_std.due_settlements
GROUP BY    due_id
;
COMPUTE STATS wp_calc.loan_mob_set_group;

DROP TABLE IF EXISTS wp_calc.loan_mob_date;
CREATE TABLE wp_calc.loan_mob_date STORED AS PARQUET AS
SELECT      id,
            TO_DATE(NOW() - INTERVAL 1 DAY)                     ystd_date,
            IF(mob1_date  < TO_DATE(NOW()), mob1_date,  NULL)   mob1_date,
            IF(mob2_date  < TO_DATE(NOW()), mob2_date,  NULL)   mob2_date,
            IF(mob3_date  < TO_DATE(NOW()), mob3_date,  NULL)   mob3_date,
            IF(mob4_date  < TO_DATE(NOW()), mob4_date,  NULL)   mob4_date,
            IF(mob5_date  < TO_DATE(NOW()), mob5_date,  NULL)   mob5_date,
            IF(mob6_date  < TO_DATE(NOW()), mob6_date,  NULL)   mob6_date,
            IF(mob7_date  < TO_DATE(NOW()), mob7_date,  NULL)   mob7_date,
            IF(mob8_date  < TO_DATE(NOW()), mob8_date,  NULL)   mob8_date,
            IF(mob9_date  < TO_DATE(NOW()), mob9_date,  NULL)   mob9_date,
            IF(mob10_date < TO_DATE(NOW()), mob10_date, NULL)   mob10_date,
            IF(mob11_date < TO_DATE(NOW()), mob11_date, NULL)   mob11_date,
            IF(mob12_date < TO_DATE(NOW()), mob12_date, NULL)   mob12_date,
            IF(wob14_date < TO_DATE(NOW()), wob14_date, NULL)   wob14_date,
            IF(fpd4_date  < TO_DATE(NOW()), fpd4_date,  NULL)   fpd4_date,
            IF(fpd30_date < TO_DATE(NOW()), fpd30_date, NULL)   fpd30_date,
            IF(spd4_date  < TO_DATE(NOW()), spd4_date,  NULL)   spd4_date,
            IF(tpd4_date  < TO_DATE(NOW()), tpd4_date,  NULL)   tpd4_date,
            IF(qpd4_date  < TO_DATE(NOW()), qpd4_date,  NULL)   qpd4_date,
            IF(i5pd4_date < TO_DATE(NOW()), i5pd4_date, NULL)   i5pd4_date,
            IF(i6pd4_date < TO_DATE(NOW()), i6pd4_date, NULL)   i6pd4_date,
            IF(i7pd4_date < TO_DATE(NOW()), i7pd4_date, NULL)   i7pd4_date,
            IF(i8pd4_date < TO_DATE(NOW()), i8pd4_date, NULL)   i8pd4_date,
            IF(i9pd4_date < TO_DATE(NOW()), i9pd4_date, NULL)   i9pd4_date,
            IF(i10pd4_date< TO_DATE(NOW()), i10pd4_date,NULL)   i10pd4_date,
            IF(i11pd4_date< TO_DATE(NOW()), i11pd4_date,NULL)   i11pd4_date,
            IF(i12pd4_date< TO_DATE(NOW()), i12pd4_date,NULL)   i12pd4_date
FROM        (
                SELECT      t1.id                                                   id,
                            LAST_DAY(t1.disbursed_at + INTERVAL  1 MONTH)           mob1_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL  2 MONTH)           mob2_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL  3 MONTH)           mob3_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL  4 MONTH)           mob4_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL  5 MONTH)           mob5_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL  6 MONTH)           mob6_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL  7 MONTH)           mob7_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL  8 MONTH)           mob8_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL  9 MONTH)           mob9_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL 10 MONTH)           mob10_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL 11 MONTH)           mob11_date,
                            LAST_DAY(t1.disbursed_at + INTERVAL 12 MONTH)           mob12_date,
                            TRUNC(t1.disbursed_at, 'DAY') + INTERVAL 15 WEEK - INTERVAL 1 DAY
                                                                                    wob14_date,
                            MIN(IF(index= 1, t2.due_date, NULL)) + INTERVAL 3 DAY   fpd4_date,
                            MIN(IF(index= 1, t2.due_date, NULL)) + INTERVAL 29 DAY  fpd30_date,
                            MIN(IF(index= 2, t2.due_date, NULL)) + INTERVAL 3 DAY   spd4_date,
                            MIN(IF(index= 3, t2.due_date, NULL)) + INTERVAL 3 DAY   tpd4_date,
                            MIN(IF(index= 4, t2.due_date, NULL)) + INTERVAL 3 DAY   qpd4_date,
                            MIN(IF(index= 5, t2.due_date, NULL)) + INTERVAL 3 DAY   i5pd4_date,
                            MIN(IF(index= 6, t2.due_date, NULL)) + INTERVAL 3 DAY   i6pd4_date,
                            MIN(IF(index= 7, t2.due_date, NULL)) + INTERVAL 3 DAY   i7pd4_date,
                            MIN(IF(index= 8, t2.due_date, NULL)) + INTERVAL 3 DAY   i8pd4_date,
                            MIN(IF(index= 9, t2.due_date, NULL)) + INTERVAL 3 DAY   i9pd4_date,
                            MIN(IF(index=10, t2.due_date, NULL)) + INTERVAL 3 DAY   i10pd4_date,
                            MIN(IF(index=11, t2.due_date, NULL)) + INTERVAL 3 DAY   i11pd4_date,
                            MIN(IF(index=12, t2.due_date, NULL)) + INTERVAL 3 DAY   i12pd4_date
                FROM        wp_std.loans    t1
                LEFT JOIN   wp_std.dues     t2 ON t1.id = t2.loan_id AND t2.due_type = 'principal'
                GROUP BY    1,2,3,4,5,6,7,8,9,10,11,12,13,14
            ) tt
;
COMPUTE STATS wp_calc.loan_mob_date;

DROP TABLE IF EXISTS wp_calc.loan_mob_index;
CREATE TABLE wp_calc.loan_mob_index STORED AS PARQUET AS
SELECT      t1.id,
            t1.ystd_date,
            t1.mob1_date,
            t1.mob2_date,
            t1.mob3_date,
            t1.mob4_date,
            t1.mob5_date,
            t1.mob6_date,
            t1.mob7_date,
            t1.mob8_date,
            t1.mob9_date,
            t1.mob10_date,
            t1.mob11_date,
            t1.mob12_date,
            t1.wob14_date,
            t1.fpd4_date,
            t1.fpd30_date,
            t1.spd4_date,
            t1.tpd4_date,
            t1.qpd4_date,
            t1.i5pd4_date,
            t1.i6pd4_date,
            t1.i7pd4_date,
            t1.i8pd4_date,
            t1.i9pd4_date,
            t1.i10pd4_date,
            t1.i11pd4_date,
            t1.i12pd4_date,
            MAX(IF(t2.due_date <= t1.ystd_date,   t2.index, NULL))      ystd_index,
            MAX(IF(t2.due_date <= t1.mob1_date,   t2.index, NULL))      mob1_index,
            MAX(IF(t2.due_date <= t1.mob2_date,   t2.index, NULL))      mob2_index,
            MAX(IF(t2.due_date <= t1.mob3_date,   t2.index, NULL))      mob3_index,
            MAX(IF(t2.due_date <= t1.mob4_date,   t2.index, NULL))      mob4_index,
            MAX(IF(t2.due_date <= t1.mob5_date,   t2.index, NULL))      mob5_index,
            MAX(IF(t2.due_date <= t1.mob6_date,   t2.index, NULL))      mob6_index,
            MAX(IF(t2.due_date <= t1.mob7_date,   t2.index, NULL))      mob7_index,
            MAX(IF(t2.due_date <= t1.mob8_date,   t2.index, NULL))      mob8_index,
            MAX(IF(t2.due_date <= t1.mob9_date,   t2.index, NULL))      mob9_index,
            MAX(IF(t2.due_date <= t1.mob10_date,  t2.index, NULL))      mob10_index,
            MAX(IF(t2.due_date <= t1.mob11_date,  t2.index, NULL))      mob11_index,
            MAX(IF(t2.due_date <= t1.mob12_date,  t2.index, NULL))      mob12_index,
            MAX(IF(t2.due_date <= t1.wob14_date,  t2.index, NULL))      wob14_index,
            MAX(IF(t2.due_date <= t1.fpd4_date,   t2.index, NULL))      fpd4_index,
            MAX(IF(t2.due_date <= t1.fpd30_date,  t2.index, NULL))      fpd30_index,
            MAX(IF(t2.due_date <= t1.spd4_date,   t2.index, NULL))      spd4_index,
            MAX(IF(t2.due_date <= t1.tpd4_date,   t2.index, NULL))      tpd4_index,
            MAX(IF(t2.due_date <= t1.qpd4_date,   t2.index, NULL))      qpd4_index,
            MAX(IF(t2.due_date <= t1.i5pd4_date,  t2.index, NULL))      i5pd4_index,
            MAX(IF(t2.due_date <= t1.i6pd4_date,  t2.index, NULL))      i6pd4_index,
            MAX(IF(t2.due_date <= t1.i7pd4_date,  t2.index, NULL))      i7pd4_index,
            MAX(IF(t2.due_date <= t1.i8pd4_date,  t2.index, NULL))      i8pd4_index,
            MAX(IF(t2.due_date <= t1.i9pd4_date,  t2.index, NULL))      i9pd4_index,
            MAX(IF(t2.due_date <= t1.i10pd4_date, t2.index, NULL))      i10pd4_index,
            MAX(IF(t2.due_date <= t1.i11pd4_date, t2.index, NULL))      i11pd4_index,
            MAX(IF(t2.due_date <= t1.i12pd4_date, t2.index, NULL))      i12pd4_index
FROM        wp_calc.loan_mob_date   t1
LEFT JOIN   wp_std.dues             t2 ON t1.id = t2.loan_id AND t2.due_type = 'principal'
GROUP BY    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
;
COMPUTE STATS wp_calc.loan_mob_index;


DROP TABLE IF EXISTS wp_calc.loan_mob_detail;
CREATE TABLE wp_calc.loan_mob_detail STORED AS PARQUET AS
SELECT      t1.id,
            t1.ystd_date,
            t1.mob1_date,
            t1.mob2_date,
            t1.mob3_date,
            t1.mob4_date,
            t1.mob5_date,
            t1.mob6_date,
            t1.mob7_date,
            t1.mob8_date,
            t1.mob9_date,
            t1.mob10_date,
            t1.mob11_date,
            t1.mob12_date,
            t1.wob14_date,
            t1.fpd4_date,
            t1.fpd30_date,
            t1.spd4_date,
            t1.tpd4_date,
            t1.qpd4_date,
            t1.i5pd4_date,
            t1.i6pd4_date,
            t1.i7pd4_date,
            t1.i8pd4_date,
            t1.i9pd4_date,
            t1.i10pd4_date,
            t1.i11pd4_date,
            t1.i12pd4_date,
            t1.ystd_index,
            t1.mob1_index,
            t1.mob2_index,
            t1.mob3_index,
            t1.mob4_index,
            t1.mob5_index,
            t1.mob6_index,
            t1.mob7_index,
            t1.mob8_index,
            t1.mob9_index,
            t1.mob10_index,
            t1.mob11_index,
            t1.mob12_index,
            t1.wob14_index,
            t1.fpd4_index,
            t1.fpd30_index,
            t1.spd4_index,
            t1.tpd4_index,
            t1.qpd4_index,
            t1.i5pd4_index,
            t1.i6pd4_index,
            t1.i7pd4_index,
            t1.i8pd4_index,
            t1.i9pd4_index,
            t1.i10pd4_index,
            t1.i11pd4_index,
            t1.i12pd4_index,
            MAX(CASE WHEN t2.due_type = 'principal' THEN t2.due_date ELSE NULL END)
                                            sche_close,
            SUM(t2.amount)                  tot_amt,
            SUM(CASE WHEN t2.due_type = 'principal' THEN t2.amount ELSE NULL END)
                                            tot_prin,
            SUM(CASE WHEN t2.due_type = 'interest' THEN t2.amount ELSE NULL END)
                                            tot_intr,
            SUM(CASE WHEN t2.due_type = 'management_fee' THEN t2.amount ELSE NULL END)
                                            tot_mgmt,
            SUM(CASE WHEN t2.due_type LIKE 'overdue%' THEN t2.amount ELSE NULL END)
                                            tot_over,
            SUM(CASE WHEN t2.due_type NOT IN ('principal', 'interest', 'management_fee') AND t2.due_type NOT LIKE 'overdue%' THEN t2.amount ELSE NULL END)
                                            tot_other,
            SUM(CASE WHEN t2.index <= t1.ystd_index THEN t2.amount ELSE NULL END)
                                            ystd_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.ystd_index THEN t2.amount ELSE NULL END)
                                            ystd_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.ystd_index THEN t2.amount ELSE NULL END)
                                            ystd_due_intr,
            SUM(CASE WHEN t2.due_type = 'management_fee' AND t2.index <= t1.ystd_index THEN t2.amount ELSE NULL END)
                                            ystd_due_mgmt,
            SUM(CASE WHEN t2.due_type LIKE 'overdue%' AND t2.index <= t1.ystd_index THEN t2.amount ELSE NULL END)
                                            ystd_due_over,
            SUM(CASE WHEN t2.due_type NOT IN ('principal', 'interest', 'management_fee') AND t2.due_type NOT LIKE 'overdue%' AND t2.index <= t1.ystd_index THEN t2.amount ELSE NULL END)
                                            ystd_due_other,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.ystd_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.ystd_index AND TO_DATE(t3.set_at) > t1.ystd_date THEN t2.due_date ELSE NULL END)
                                            ystd_due_date,
            MAX(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.ystd_index AND COALESCE(t3.amount, 0) < t2.amount THEN DATEDIFF(t1.ystd_date, t2.due_date) + 1
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.ystd_index AND TO_DATE(t3.set_at) > t2.due_date THEN DATEDIFF(t3.set_at, t2.due_date) ELSE NULL END)
                                            ystd_dpd_his,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.ystd_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.ystd_index AND TO_DATE(t3.set_at) > t1.ystd_date THEN t2.index ELSE NULL END)
                                            ystd_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.ystd_date) <= t1.ystd_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            ystd_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.ystd_date) <= t1.ystd_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            ystd_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.ystd_date) <= t1.ystd_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            ystd_set_intr,
            SUM(CASE WHEN t2.due_type = 'management_fee' AND COALESCE(TO_DATE(t3.set_at), t1.ystd_date) <= t1.ystd_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            ystd_set_mgmt,
            SUM(CASE WHEN t2.due_type LIKE 'overdue%' AND COALESCE(TO_DATE(t3.set_at), t1.ystd_date) <= t1.ystd_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            ystd_set_over,
            SUM(CASE WHEN t2.due_type NOT IN ('principal', 'interest', 'management_fee') AND t2.due_type NOT LIKE 'overdue%' AND COALESCE(TO_DATE(t3.set_at), t1.ystd_date) <= t1.ystd_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            ystd_set_other,
            SUM(CASE WHEN t2.index <= t1.mob1_index THEN t2.amount ELSE NULL END)
                                            mob1_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob1_index THEN t2.amount ELSE NULL END)
                                            mob1_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob1_index THEN t2.amount ELSE NULL END)
                                            mob1_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob1_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob1_index AND TO_DATE(t3.set_at) > t1.mob1_date THEN t2.due_date ELSE NULL END)
                                            mob1_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob1_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob1_index AND TO_DATE(t3.set_at) > t1.mob1_date THEN t2.index ELSE NULL END)
                                            mob1_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob1_date) <= t1.mob1_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob1_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob1_date) <= t1.mob1_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob1_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob1_date) <= t1.mob1_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob1_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob2_index THEN t2.amount ELSE NULL END)
                                            mob2_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob2_index THEN t2.amount ELSE NULL END)
                                            mob2_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob2_index THEN t2.amount ELSE NULL END)
                                            mob2_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob2_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob2_index AND TO_DATE(t3.set_at) > t1.mob2_date THEN t2.due_date ELSE NULL END)
                                            mob2_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob2_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob2_index AND TO_DATE(t3.set_at) > t1.mob2_date THEN t2.index ELSE NULL END)
                                            mob2_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob2_date) <= t1.mob2_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob2_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob2_date) <= t1.mob2_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob2_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob2_date) <= t1.mob2_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob2_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob3_index THEN t2.amount ELSE NULL END)
                                            mob3_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob3_index THEN t2.amount ELSE NULL END)
                                            mob3_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob3_index THEN t2.amount ELSE NULL END)
                                            mob3_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob3_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob3_index AND TO_DATE(t3.set_at) > t1.mob3_date THEN t2.due_date ELSE NULL END)
                                            mob3_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob3_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob3_index AND TO_DATE(t3.set_at) > t1.mob3_date THEN t2.index ELSE NULL END)
                                            mob3_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob3_date) <= t1.mob3_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob3_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob3_date) <= t1.mob3_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob3_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob3_date) <= t1.mob3_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob3_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob4_index THEN t2.amount ELSE NULL END)
                                            mob4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob4_index THEN t2.amount ELSE NULL END)
                                            mob4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob4_index THEN t2.amount ELSE NULL END)
                                            mob4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob4_index AND TO_DATE(t3.set_at) > t1.mob4_date THEN t2.due_date ELSE NULL END)
                                            mob4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob4_index AND TO_DATE(t3.set_at) > t1.mob4_date THEN t2.index ELSE NULL END)
                                            mob4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob4_date) <= t1.mob4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob4_date) <= t1.mob4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob4_date) <= t1.mob4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob4_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob5_index THEN t2.amount ELSE NULL END)
                                            mob5_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob5_index THEN t2.amount ELSE NULL END)
                                            mob5_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob5_index THEN t2.amount ELSE NULL END)
                                            mob5_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob5_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob5_index AND TO_DATE(t3.set_at) > t1.mob5_date THEN t2.due_date ELSE NULL END)
                                            mob5_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob5_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob5_index AND TO_DATE(t3.set_at) > t1.mob5_date THEN t2.index ELSE NULL END)
                                            mob5_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob5_date) <= t1.mob5_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob5_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob5_date) <= t1.mob5_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob5_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob5_date) <= t1.mob5_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob5_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob6_index THEN t2.amount ELSE NULL END)
                                            mob6_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob6_index THEN t2.amount ELSE NULL END)
                                            mob6_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob6_index THEN t2.amount ELSE NULL END)
                                            mob6_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob6_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob6_index AND TO_DATE(t3.set_at) > t1.mob6_date THEN t2.due_date ELSE NULL END)
                                            mob6_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob6_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob6_index AND TO_DATE(t3.set_at) > t1.mob6_date THEN t2.index ELSE NULL END)
                                            mob6_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob6_date) <= t1.mob6_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob6_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob6_date) <= t1.mob6_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob6_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob6_date) <= t1.mob6_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob6_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob7_index THEN t2.amount ELSE NULL END)
                                            mob7_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob7_index THEN t2.amount ELSE NULL END)
                                            mob7_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob7_index THEN t2.amount ELSE NULL END)
                                            mob7_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob7_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob7_index AND TO_DATE(t3.set_at) > t1.mob7_date THEN t2.due_date ELSE NULL END)
                                            mob7_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob7_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob7_index AND TO_DATE(t3.set_at) > t1.mob7_date THEN t2.index ELSE NULL END)
                                            mob7_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob7_date) <= t1.mob7_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob7_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob7_date) <= t1.mob7_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob7_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob7_date) <= t1.mob7_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob7_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob8_index THEN t2.amount ELSE NULL END)
                                            mob8_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob8_index THEN t2.amount ELSE NULL END)
                                            mob8_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob8_index THEN t2.amount ELSE NULL END)
                                            mob8_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob8_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob8_index AND TO_DATE(t3.set_at) > t1.mob8_date THEN t2.due_date ELSE NULL END)
                                            mob8_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob8_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob8_index AND TO_DATE(t3.set_at) > t1.mob8_date THEN t2.index ELSE NULL END)
                                            mob8_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob8_date) <= t1.mob8_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob8_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob8_date) <= t1.mob8_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob8_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob8_date) <= t1.mob8_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob8_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob9_index THEN t2.amount ELSE NULL END)
                                            mob9_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob9_index THEN t2.amount ELSE NULL END)
                                            mob9_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob9_index THEN t2.amount ELSE NULL END)
                                            mob9_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob9_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob9_index AND TO_DATE(t3.set_at) > t1.mob9_date THEN t2.due_date ELSE NULL END)
                                            mob9_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob9_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob9_index AND TO_DATE(t3.set_at) > t1.mob9_date THEN t2.index ELSE NULL END)
                                            mob9_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob9_date) <= t1.mob9_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob9_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob9_date) <= t1.mob9_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob9_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob9_date) <= t1.mob9_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob9_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob10_index THEN t2.amount ELSE NULL END)
                                            mob10_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob10_index THEN t2.amount ELSE NULL END)
                                            mob10_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob10_index THEN t2.amount ELSE NULL END)
                                            mob10_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob10_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob10_index AND TO_DATE(t3.set_at) > t1.mob10_date THEN t2.due_date ELSE NULL END)
                                            mob10_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob10_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob10_index AND TO_DATE(t3.set_at) > t1.mob10_date THEN t2.index ELSE NULL END)
                                            mob10_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob10_date) <= t1.mob10_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob10_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob10_date) <= t1.mob10_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob10_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob10_date) <= t1.mob10_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob10_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob11_index THEN t2.amount ELSE NULL END)
                                            mob11_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob11_index THEN t2.amount ELSE NULL END)
                                            mob11_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob11_index THEN t2.amount ELSE NULL END)
                                            mob11_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob11_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob11_index AND TO_DATE(t3.set_at) > t1.mob11_date THEN t2.due_date ELSE NULL END)
                                            mob11_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob11_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob11_index AND TO_DATE(t3.set_at) > t1.mob11_date THEN t2.index ELSE NULL END)
                                            mob11_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob11_date) <= t1.mob11_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob11_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob11_date) <= t1.mob11_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob11_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob11_date) <= t1.mob11_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob11_set_intr,
            SUM(CASE WHEN t2.index <= t1.mob12_index THEN t2.amount ELSE NULL END)
                                            mob12_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob12_index THEN t2.amount ELSE NULL END)
                                            mob12_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.mob12_index THEN t2.amount ELSE NULL END)
                                            mob12_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob12_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob12_index AND TO_DATE(t3.set_at) > t1.mob12_date THEN t2.due_date ELSE NULL END)
                                            mob12_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.mob12_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.mob12_index AND TO_DATE(t3.set_at) > t1.mob12_date THEN t2.index ELSE NULL END)
                                            mob12_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.mob12_date) <= t1.mob12_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob12_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.mob12_date) <= t1.mob12_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob12_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.mob12_date) <= t1.mob12_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            mob12_set_intr,
            SUM(CASE WHEN t2.index <= t1.wob14_index THEN t2.amount ELSE NULL END)
                                            wob14_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.wob14_index THEN t2.amount ELSE NULL END)
                                            wob14_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.wob14_index THEN t2.amount ELSE NULL END)
                                            wob14_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.wob14_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.wob14_index AND TO_DATE(t3.set_at) > t1.wob14_date THEN t2.due_date ELSE NULL END)
                                            wob14_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.wob14_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.wob14_index AND TO_DATE(t3.set_at) > t1.wob14_date THEN t2.index ELSE NULL END)
                                            wob14_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.wob14_date) <= t1.wob14_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            wob14_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.wob14_date) <= t1.wob14_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            wob14_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.wob14_date) <= t1.wob14_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            wob14_set_intr,
            SUM(CASE WHEN t2.index <= t1.fpd4_index THEN t2.amount ELSE NULL END)
                                            fpd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.fpd4_index THEN t2.amount ELSE NULL END)
                                            fpd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.fpd4_index THEN t2.amount ELSE NULL END)
                                            fpd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.fpd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.fpd4_index AND TO_DATE(t3.set_at) > t1.fpd4_date THEN t2.due_date ELSE NULL END)
                                            fpd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.fpd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.fpd4_index AND TO_DATE(t3.set_at) > t1.fpd4_date THEN t2.index ELSE NULL END)
                                            fpd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.fpd4_date) <= t1.fpd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            fpd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.fpd4_date) <= t1.fpd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            fpd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.fpd4_date) <= t1.fpd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            fpd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.fpd30_index THEN t2.amount ELSE NULL END)
                                            fpd30_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.fpd30_index THEN t2.amount ELSE NULL END)
                                            fpd30_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.fpd30_index THEN t2.amount ELSE NULL END)
                                            fpd30_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.fpd30_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.fpd30_index AND TO_DATE(t3.set_at) > t1.fpd30_date THEN t2.due_date ELSE NULL END)
                                            fpd30_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.fpd30_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.fpd30_index AND TO_DATE(t3.set_at) > t1.fpd30_date THEN t2.index ELSE NULL END)
                                            fpd30_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.fpd30_date) <= t1.fpd30_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            fpd30_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.fpd30_date) <= t1.fpd30_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            fpd30_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.fpd30_date) <= t1.fpd30_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            fpd30_set_intr,
            SUM(CASE WHEN t2.index <= t1.spd4_index THEN t2.amount ELSE NULL END)
                                            spd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.spd4_index THEN t2.amount ELSE NULL END)
                                            spd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.spd4_index THEN t2.amount ELSE NULL END)
                                            spd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.spd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.spd4_index AND TO_DATE(t3.set_at) > t1.spd4_date THEN t2.due_date ELSE NULL END)
                                            spd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.spd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.spd4_index AND TO_DATE(t3.set_at) > t1.spd4_date THEN t2.index ELSE NULL END)
                                            spd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.spd4_date) <= t1.spd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            spd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.spd4_date) <= t1.spd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            spd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.spd4_date) <= t1.spd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            spd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.tpd4_index THEN t2.amount ELSE NULL END)
                                            tpd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.tpd4_index THEN t2.amount ELSE NULL END)
                                            tpd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.tpd4_index THEN t2.amount ELSE NULL END)
                                            tpd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.tpd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.tpd4_index AND TO_DATE(t3.set_at) > t1.tpd4_date THEN t2.due_date ELSE NULL END)
                                            tpd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.tpd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.tpd4_index AND TO_DATE(t3.set_at) > t1.tpd4_date THEN t2.index ELSE NULL END)
                                            tpd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.tpd4_date) <= t1.tpd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            tpd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.tpd4_date) <= t1.tpd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            tpd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.tpd4_date) <= t1.tpd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            tpd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.qpd4_index THEN t2.amount ELSE NULL END)
                                            qpd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.qpd4_index THEN t2.amount ELSE NULL END)
                                            qpd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.qpd4_index THEN t2.amount ELSE NULL END)
                                            qpd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.qpd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.qpd4_index AND TO_DATE(t3.set_at) > t1.qpd4_date THEN t2.due_date ELSE NULL END)
                                            qpd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.qpd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.qpd4_index AND TO_DATE(t3.set_at) > t1.qpd4_date THEN t2.index ELSE NULL END)
                                            qpd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.qpd4_date) <= t1.qpd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            qpd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.qpd4_date) <= t1.qpd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            qpd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.qpd4_date) <= t1.qpd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            qpd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.i5pd4_index THEN t2.amount ELSE NULL END)
                                            i5pd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i5pd4_index THEN t2.amount ELSE NULL END)
                                            i5pd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.i5pd4_index THEN t2.amount ELSE NULL END)
                                            i5pd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i5pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i5pd4_index AND TO_DATE(t3.set_at) > t1.i5pd4_date THEN t2.due_date ELSE NULL END)
                                            i5pd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i5pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i5pd4_index AND TO_DATE(t3.set_at) > t1.i5pd4_date THEN t2.index ELSE NULL END)
                                            i5pd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.i5pd4_date) <= t1.i5pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i5pd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.i5pd4_date) <= t1.i5pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i5pd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.i5pd4_date) <= t1.i5pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i5pd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.i6pd4_index THEN t2.amount ELSE NULL END)
                                            i6pd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i6pd4_index THEN t2.amount ELSE NULL END)
                                            i6pd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.i6pd4_index THEN t2.amount ELSE NULL END)
                                            i6pd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i6pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i6pd4_index AND TO_DATE(t3.set_at) > t1.i6pd4_date THEN t2.due_date ELSE NULL END)
                                            i6pd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i6pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i6pd4_index AND TO_DATE(t3.set_at) > t1.i6pd4_date THEN t2.index ELSE NULL END)
                                            i6pd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.i6pd4_date) <= t1.i6pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i6pd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.i6pd4_date) <= t1.i6pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i6pd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.i6pd4_date) <= t1.i6pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i6pd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.i7pd4_index THEN t2.amount ELSE NULL END)
                                            i7pd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i7pd4_index THEN t2.amount ELSE NULL END)
                                            i7pd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.i7pd4_index THEN t2.amount ELSE NULL END)
                                            i7pd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i7pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i7pd4_index AND TO_DATE(t3.set_at) > t1.i7pd4_date THEN t2.due_date ELSE NULL END)
                                            i7pd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i7pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i7pd4_index AND TO_DATE(t3.set_at) > t1.i7pd4_date THEN t2.index ELSE NULL END)
                                            i7pd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.i7pd4_date) <= t1.i7pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i7pd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.i7pd4_date) <= t1.i7pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i7pd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.i7pd4_date) <= t1.i7pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i7pd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.i8pd4_index THEN t2.amount ELSE NULL END)
                                            i8pd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i8pd4_index THEN t2.amount ELSE NULL END)
                                            i8pd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.i8pd4_index THEN t2.amount ELSE NULL END)
                                            i8pd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i8pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i8pd4_index AND TO_DATE(t3.set_at) > t1.i8pd4_date THEN t2.due_date ELSE NULL END)
                                            i8pd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i8pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i8pd4_index AND TO_DATE(t3.set_at) > t1.i8pd4_date THEN t2.index ELSE NULL END)
                                            i8pd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.i8pd4_date) <= t1.i8pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i8pd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.i8pd4_date) <= t1.i8pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i8pd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.i8pd4_date) <= t1.i8pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i8pd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.i9pd4_index THEN t2.amount ELSE NULL END)
                                            i9pd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i9pd4_index THEN t2.amount ELSE NULL END)
                                            i9pd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.i9pd4_index THEN t2.amount ELSE NULL END)
                                            i9pd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i9pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i9pd4_index AND TO_DATE(t3.set_at) > t1.i9pd4_date THEN t2.due_date ELSE NULL END)
                                            i9pd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i9pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i9pd4_index AND TO_DATE(t3.set_at) > t1.i9pd4_date THEN t2.index ELSE NULL END)
                                            i9pd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.i9pd4_date) <= t1.i9pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i9pd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.i9pd4_date) <= t1.i9pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i9pd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.i9pd4_date) <= t1.i9pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i9pd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.i10pd4_index THEN t2.amount ELSE NULL END)
                                            i10pd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i10pd4_index THEN t2.amount ELSE NULL END)
                                            i10pd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.i10pd4_index THEN t2.amount ELSE NULL END)
                                            i10pd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i10pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i10pd4_index AND TO_DATE(t3.set_at) > t1.i10pd4_date THEN t2.due_date ELSE NULL END)
                                            i10pd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i10pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i10pd4_index AND TO_DATE(t3.set_at) > t1.i10pd4_date THEN t2.index ELSE NULL END)
                                            i10pd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.i10pd4_date) <= t1.i10pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i10pd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.i10pd4_date) <= t1.i10pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i10pd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.i10pd4_date) <= t1.i10pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i10pd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.i11pd4_index THEN t2.amount ELSE NULL END)
                                            i11pd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i11pd4_index THEN t2.amount ELSE NULL END)
                                            i11pd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.i11pd4_index THEN t2.amount ELSE NULL END)
                                            i11pd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i11pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i11pd4_index AND TO_DATE(t3.set_at) > t1.i11pd4_date THEN t2.due_date ELSE NULL END)
                                            i11pd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i11pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i11pd4_index AND TO_DATE(t3.set_at) > t1.i11pd4_date THEN t2.index ELSE NULL END)
                                            i11pd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.i11pd4_date) <= t1.i11pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i11pd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.i11pd4_date) <= t1.i11pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i11pd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.i11pd4_date) <= t1.i11pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i11pd4_set_intr,
            SUM(CASE WHEN t2.index <= t1.i12pd4_index THEN t2.amount ELSE NULL END)
                                            i12pd4_due_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i12pd4_index THEN t2.amount ELSE NULL END)
                                            i12pd4_due_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND t2.index <= t1.i12pd4_index THEN t2.amount ELSE NULL END)
                                            i12pd4_due_intr,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i12pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.due_date
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i12pd4_index AND TO_DATE(t3.set_at) > t1.i12pd4_date THEN t2.due_date ELSE NULL END)
                                            i12pd4_due_date,
            MIN(CASE WHEN t2.due_type = 'principal' AND t2.index <= t1.i12pd4_index AND COALESCE(t3.amount, 0) < t2.amount THEN t2.index
                     WHEN t2.due_type = 'principal' AND t2.index <= t1.i12pd4_index AND TO_DATE(t3.set_at) > t1.i12pd4_date THEN t2.index ELSE NULL END)
                                            i12pd4_set_index,
            SUM(CASE WHEN COALESCE(TO_DATE(t3.set_at), t1.i12pd4_date) <= t1.i12pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i12pd4_set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' AND COALESCE(TO_DATE(t3.set_at), t1.i12pd4_date) <= t1.i12pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i12pd4_set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' AND COALESCE(TO_DATE(t3.set_at), t1.i12pd4_date) <= t1.i12pd4_date THEN COALESCE(t3.amount, 0) ELSE NULL END)
                                            i12pd4_set_intr
FROM        wp_calc.loan_mob_index      t1
LEFT JOIN   wp_std.dues                 t2 ON t1.id = t2.loan_id
LEFT JOIN   wp_calc.loan_mob_set_group  t3 ON t2.id = t3.due_id
GROUP BY    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55
;
COMPUTE STATS wp_calc.loan_mob_detail;

DROP TABLE IF EXISTS wp_calc.loan_mob;
CREATE TABLE wp_calc.loan_mob STORED AS PARQUET AS
SELECT      t1.*,
            t1.tot_prin - t1.ystd_set_prin                          ystd_gap_prin,
            t1.tot_intr - t1.ystd_set_intr                          ystd_gap_intr,
            DATEDIFF(t1.ystd_date, t1.ystd_due_date) + 1            ystd_over_day,
            t1.ystd_dpd_his                                         ystd_over_day_his,
            t1.ystd_index - t1.ystd_set_index + 1                   ystd_over_index,
            greatest(t1.ystd_due_amt - t1.ystd_set_amt, 0)          ystd_over_amt,
            greatest(t1.ystd_due_prin - t1.ystd_set_prin, 0)        ystd_over_prin,
            greatest(t1.ystd_due_intr - t1.ystd_set_intr, 0)        ystd_over_intr,
            greatest(t1.ystd_due_mgmt - t1.ystd_set_mgmt, 0)        ystd_over_mgmt,
            greatest(t1.ystd_due_over - t1.ystd_set_over, 0)        ystd_over_over,
            greatest(t1.ystd_due_other - t1.ystd_set_other, 0)      ystd_over_other,
            t1.tot_prin - t1.mob1_set_prin                          mob1_gap_prin,
            DATEDIFF(t1.mob1_date, t1.mob1_due_date) + 1            mob1_over_day,
            t1.mob1_index - t1.mob1_set_index + 1                   mob1_over_index,
            greatest(t1.mob1_due_amt - t1.mob1_set_amt, 0)          mob1_over_amt,
            greatest(t1.mob1_due_prin - t1.mob1_set_prin, 0)        mob1_over_prin,
            greatest(t1.mob1_due_intr - t1.mob1_set_intr, 0)        mob1_over_intr,
            t1.tot_prin - t1.mob2_set_prin                          mob2_gap_prin,
            DATEDIFF(t1.mob2_date, t1.mob2_due_date) + 1            mob2_over_day,
            t1.mob2_index - t1.mob2_set_index + 1                   mob2_over_index,
            greatest(t1.mob2_due_amt - t1.mob2_set_amt, 0)          mob2_over_amt,
            greatest(t1.mob2_due_prin - t1.mob2_set_prin, 0)        mob2_over_prin,
            greatest(t1.mob2_due_intr - t1.mob2_set_intr, 0)        mob2_over_intr,
            t1.tot_prin - t1.mob3_set_prin                          mob3_gap_prin,
            DATEDIFF(t1.mob3_date, t1.mob3_due_date) + 1            mob3_over_day,
            t1.mob3_index - t1.mob3_set_index + 1                   mob3_over_index,
            greatest(t1.mob3_due_amt - t1.mob3_set_amt, 0)          mob3_over_amt,
            greatest(t1.mob3_due_prin - t1.mob3_set_prin, 0)        mob3_over_prin,
            greatest(t1.mob3_due_intr - t1.mob3_set_intr, 0)        mob3_over_intr,
            t1.tot_prin - t1.mob4_set_prin                          mob4_gap_prin,
            DATEDIFF(t1.mob4_date, t1.mob4_due_date) + 1            mob4_over_day,
            t1.mob4_index - t1.mob4_set_index + 1                   mob4_over_index,
            greatest(t1.mob4_due_amt - t1.mob4_set_amt, 0)          mob4_over_amt,
            greatest(t1.mob4_due_prin - t1.mob4_set_prin, 0)        mob4_over_prin,
            greatest(t1.mob4_due_intr - t1.mob4_set_intr, 0)        mob4_over_intr,
            t1.tot_prin - t1.mob5_set_prin                          mob5_gap_prin,
            DATEDIFF(t1.mob5_date, t1.mob5_due_date) + 1            mob5_over_day,
            t1.mob5_index - t1.mob5_set_index + 1                   mob5_over_index,
            greatest(t1.mob5_due_amt - t1.mob5_set_amt, 0)          mob5_over_amt,
            greatest(t1.mob5_due_prin - t1.mob5_set_prin, 0)        mob5_over_prin,
            greatest(t1.mob5_due_intr - t1.mob5_set_intr, 0)        mob5_over_intr,
            t1.tot_prin - t1.mob6_set_prin                          mob6_gap_prin,
            DATEDIFF(t1.mob6_date, t1.mob6_due_date) + 1            mob6_over_day,
            t1.mob6_index - t1.mob6_set_index + 1                   mob6_over_index,
            greatest(t1.mob6_due_amt - t1.mob6_set_amt, 0)          mob6_over_amt,
            greatest(t1.mob6_due_prin - t1.mob6_set_prin, 0)        mob6_over_prin,
            greatest(t1.mob6_due_intr - t1.mob6_set_intr, 0)        mob6_over_intr,
            t1.tot_prin - t1.mob7_set_prin                          mob7_gap_prin,
            DATEDIFF(t1.mob7_date, t1.mob7_due_date) + 1            mob7_over_day,
            t1.mob7_index - t1.mob7_set_index + 1                   mob7_over_index,
            greatest(t1.mob7_due_amt - t1.mob7_set_amt, 0)          mob7_over_amt,
            greatest(t1.mob7_due_prin - t1.mob7_set_prin, 0)        mob7_over_prin,
            greatest(t1.mob7_due_intr - t1.mob7_set_intr, 0)        mob7_over_intr,
            t1.tot_prin - t1.mob8_set_prin                          mob8_gap_prin,
            DATEDIFF(t1.mob8_date, t1.mob8_due_date) + 1            mob8_over_day,
            t1.mob8_index - t1.mob8_set_index + 1                   mob8_over_index,
            greatest(t1.mob8_due_amt - t1.mob8_set_amt, 0)          mob8_over_amt,
            greatest(t1.mob8_due_prin - t1.mob8_set_prin, 0)        mob8_over_prin,
            greatest(t1.mob8_due_intr - t1.mob8_set_intr, 0)        mob8_over_intr,
            t1.tot_prin - t1.mob9_set_prin                          mob9_gap_prin,
            DATEDIFF(t1.mob9_date, t1.mob9_due_date) + 1            mob9_over_day,
            t1.mob9_index - t1.mob9_set_index + 1                   mob9_over_index,
            greatest(t1.mob9_due_amt - t1.mob9_set_amt, 0)          mob9_over_amt,
            greatest(t1.mob9_due_prin - t1.mob9_set_prin, 0)        mob9_over_prin,
            greatest(t1.mob9_due_intr - t1.mob9_set_intr, 0)        mob9_over_intr,
            t1.tot_prin - t1.mob10_set_prin                         mob10_gap_prin,
            DATEDIFF(t1.mob10_date, t1.mob10_due_date) + 1          mob10_over_day,
            t1.mob10_index - t1.mob10_set_index + 1                 mob10_over_index,
            greatest(t1.mob10_due_amt - t1.mob10_set_amt, 0)        mob10_over_amt,
            greatest(t1.mob10_due_prin - t1.mob10_set_prin, 0)      mob10_over_prin,
            greatest(t1.mob10_due_intr - t1.mob10_set_intr, 0)      mob10_over_intr,
            t1.tot_prin - t1.mob11_set_prin                         mob11_gap_prin,
            DATEDIFF(t1.mob11_date, t1.mob11_due_date) + 1          mob11_over_day,
            t1.mob11_index - t1.mob11_set_index + 1                 mob11_over_index,
            greatest(t1.mob11_due_amt - t1.mob11_set_amt, 0)        mob11_over_amt,
            greatest(t1.mob11_due_prin - t1.mob11_set_prin, 0)      mob11_over_prin,
            greatest(t1.mob11_due_intr - t1.mob11_set_intr, 0)      mob11_over_intr,
            t1.tot_prin - t1.mob12_set_prin                         mob12_gap_prin,
            DATEDIFF(t1.mob12_date, t1.mob12_due_date) + 1          mob12_over_day,
            t1.mob12_index - t1.mob12_set_index + 1                 mob12_over_index,
            greatest(t1.mob12_due_amt - t1.mob12_set_amt, 0)        mob12_over_amt,
            greatest(t1.mob12_due_prin - t1.mob12_set_prin, 0)      mob12_over_prin,
            greatest(t1.mob12_due_intr - t1.mob12_set_intr, 0)      mob12_over_intr,
            t1.tot_prin - t1.wob14_set_prin                         wob14_gap_prin,
            DATEDIFF(t1.wob14_date, t1.wob14_due_date) + 1          wob14_over_day,
            t1.wob14_index - t1.wob14_set_index + 1                 wob14_over_index,
            greatest(t1.wob14_due_amt - t1.wob14_set_amt, 0)        wob14_over_amt,
            greatest(t1.wob14_due_prin - t1.wob14_set_prin, 0)      wob14_over_prin,
            greatest(t1.wob14_due_intr - t1.wob14_set_intr, 0)      wob14_over_intr,
            t1.tot_prin - t1.fpd4_set_prin                          fpd4_gap_prin,
            DATEDIFF(t1.fpd4_date, t1.fpd4_due_date) + 1            fpd4_over_day,
            t1.fpd4_index - t1.fpd4_set_index + 1                   fpd4_over_index,
            greatest(t1.fpd4_due_amt - t1.fpd4_set_amt, 0)          fpd4_over_amt,
            greatest(t1.fpd4_due_prin - t1.fpd4_set_prin, 0)        fpd4_over_prin,
            greatest(t1.fpd4_due_intr - t1.fpd4_set_intr, 0)        fpd4_over_intr,
            t1.tot_prin - t1.fpd30_set_prin                         fpd30_gap_prin,
            DATEDIFF(t1.fpd30_date, t1.fpd30_due_date) + 1          fpd30_over_day,
            t1.fpd30_index - t1.fpd30_set_index + 1                 fpd30_over_index,
            greatest(t1.fpd30_due_amt - t1.fpd30_set_amt, 0)        fpd30_over_amt,
            greatest(t1.fpd30_due_prin - t1.fpd30_set_prin, 0)      fpd30_over_prin,
            greatest(t1.fpd30_due_intr - t1.fpd30_set_intr, 0)      fpd30_over_intr,
            t1.tot_prin - t1.spd4_set_prin                          spd4_gap_prin,
            DATEDIFF(t1.spd4_date, t1.spd4_due_date) + 1            spd4_over_day,
            t1.spd4_index - t1.spd4_set_index + 1                   spd4_over_index,
            greatest(t1.spd4_due_amt - t1.spd4_set_amt, 0)          spd4_over_amt,
            greatest(t1.spd4_due_prin - t1.spd4_set_prin, 0)        spd4_over_prin,
            greatest(t1.spd4_due_intr - t1.spd4_set_intr, 0)        spd4_over_intr,
            t1.tot_prin - t1.tpd4_set_prin                          tpd4_gap_prin,
            DATEDIFF(t1.tpd4_date, t1.tpd4_due_date) + 1            tpd4_over_day,
            t1.tpd4_index - t1.tpd4_set_index + 1                   tpd4_over_index,
            greatest(t1.tpd4_due_amt - t1.tpd4_set_amt, 0)          tpd4_over_amt,
            greatest(t1.tpd4_due_prin - t1.tpd4_set_prin, 0)        tpd4_over_prin,
            greatest(t1.tpd4_due_intr - t1.tpd4_set_intr, 0)        tpd4_over_intr,
            t1.tot_prin - t1.qpd4_set_prin                          qpd4_gap_prin,
            DATEDIFF(t1.qpd4_date, t1.qpd4_due_date) + 1            qpd4_over_day,
            t1.qpd4_index - t1.qpd4_set_index + 1                   qpd4_over_index,
            greatest(t1.qpd4_due_amt - t1.qpd4_set_amt, 0)          qpd4_over_amt,
            greatest(t1.qpd4_due_prin - t1.qpd4_set_prin, 0)        qpd4_over_prin,
            greatest(t1.qpd4_due_intr - t1.qpd4_set_intr, 0)        qpd4_over_intr,
            t1.tot_prin - t1.i5pd4_set_prin                         i5pd4_gap_prin,
            DATEDIFF(t1.i5pd4_date, t1.i5pd4_due_date) + 1          i5pd4_over_day,
            t1.i5pd4_index - t1.i5pd4_set_index + 1                 i5pd4_over_index,
            greatest(t1.i5pd4_due_amt - t1.i5pd4_set_amt, 0)        i5pd4_over_amt,
            greatest(t1.i5pd4_due_prin - t1.i5pd4_set_prin, 0)      i5pd4_over_prin,
            greatest(t1.i5pd4_due_intr - t1.i5pd4_set_intr, 0)      i5pd4_over_intr,
            t1.tot_prin - t1.i6pd4_set_prin                         i6pd4_gap_prin,
            DATEDIFF(t1.i6pd4_date, t1.i6pd4_due_date) + 1          i6pd4_over_day,
            t1.i6pd4_index - t1.i6pd4_set_index + 1                 i6pd4_over_index,
            greatest(t1.i6pd4_due_amt - t1.i6pd4_set_amt, 0)        i6pd4_over_amt,
            greatest(t1.i6pd4_due_prin - t1.i6pd4_set_prin, 0)      i6pd4_over_prin,
            greatest(t1.i6pd4_due_intr - t1.i6pd4_set_intr, 0)      i6pd4_over_intr,
            t1.tot_prin - t1.i7pd4_set_prin                         i7pd4_gap_prin,
            DATEDIFF(t1.i7pd4_date, t1.i7pd4_due_date) + 1          i7pd4_over_day,
            t1.i7pd4_index - t1.i7pd4_set_index + 1                 i7pd4_over_index,
            greatest(t1.i7pd4_due_amt - t1.i7pd4_set_amt, 0)        i7pd4_over_amt,
            greatest(t1.i7pd4_due_prin - t1.i7pd4_set_prin, 0)      i7pd4_over_prin,
            greatest(t1.i7pd4_due_intr - t1.i7pd4_set_intr, 0)      i7pd4_over_intr,
            t1.tot_prin - t1.i8pd4_set_prin                         i8pd4_gap_prin,
            DATEDIFF(t1.i8pd4_date, t1.i8pd4_due_date) + 1          i8pd4_over_day,
            t1.i8pd4_index - t1.i8pd4_set_index + 1                 i8pd4_over_index,
            greatest(t1.i8pd4_due_amt - t1.i8pd4_set_amt, 0)        i8pd4_over_amt,
            greatest(t1.i8pd4_due_prin - t1.i8pd4_set_prin, 0)      i8pd4_over_prin,
            greatest(t1.i8pd4_due_intr - t1.i8pd4_set_intr, 0)      i8pd4_over_intr,
            t1.tot_prin - t1.i9pd4_set_prin                         i9pd4_gap_prin,
            DATEDIFF(t1.i9pd4_date, t1.i9pd4_due_date) + 1          i9pd4_over_day,
            t1.i9pd4_index - t1.i9pd4_set_index + 1                 i9pd4_over_index,
            greatest(t1.i9pd4_due_amt - t1.i9pd4_set_amt, 0)        i9pd4_over_amt,
            greatest(t1.i9pd4_due_prin - t1.i9pd4_set_prin, 0)      i9pd4_over_prin,
            greatest(t1.i9pd4_due_intr - t1.i9pd4_set_intr, 0)      i9pd4_over_intr,
            t1.tot_prin - t1.i10pd4_set_prin                        i10pd4_gap_prin,
            DATEDIFF(t1.i10pd4_date, t1.i10pd4_due_date) + 1        i10pd4_over_day,
            t1.i10pd4_index - t1.i10pd4_set_index + 1               i10pd4_over_index,
            greatest(t1.i10pd4_due_amt - t1.i10pd4_set_amt, 0)      i10pd4_over_amt,
            greatest(t1.i10pd4_due_prin - t1.i10pd4_set_prin, 0)    i10pd4_over_prin,
            greatest(t1.i10pd4_due_intr - t1.i10pd4_set_intr, 0)    i10pd4_over_intr,
            t1.tot_prin - t1.i11pd4_set_prin                        i11pd4_gap_prin,
            DATEDIFF(t1.i11pd4_date, t1.i11pd4_due_date) + 1        i11pd4_over_day,
            t1.i11pd4_index - t1.i11pd4_set_index + 1               i11pd4_over_index,
            greatest(t1.i11pd4_due_amt - t1.i11pd4_set_amt, 0)      i11pd4_over_amt,
            greatest(t1.i11pd4_due_prin - t1.i11pd4_set_prin, 0)    i11pd4_over_prin,
            greatest(t1.i11pd4_due_intr - t1.i11pd4_set_intr, 0)    i11pd4_over_intr,
            t1.tot_prin - t1.i12pd4_set_prin                        i12pd4_gap_prin,
            DATEDIFF(t1.i12pd4_date, t1.i12pd4_due_date) + 1        i12pd4_over_day,
            t1.i12pd4_index - t1.i12pd4_set_index + 1               i12pd4_over_index,
            greatest(t1.i12pd4_due_amt - t1.i12pd4_set_amt, 0)      i12pd4_over_amt,
            greatest(t1.i12pd4_due_prin - t1.i12pd4_set_prin, 0)    i12pd4_over_prin,
            greatest(t1.i12pd4_due_intr - t1.i12pd4_set_intr, 0)    i12pd4_over_intr
FROM        wp_calc.loan_mob_detail     t1
;
COMPUTE STATS wp_calc.loan_mob;

DROP TABLE IF EXISTS wp_calc.loans;
CREATE TABLE wp_calc.loans STORED AS PARQUET AS
SELECT      t1.id,
            t1.loan_application_id,
            t1.state,
            t1.disbursed_at,
            t1.created_at,
            t1.updated_at,
            t1.borrower_id,
            t1.application_id,
            t2.map_application_id,
            t1.overdue_penalty,
            t1.closed_at,
            t1.tenor,
            t1.amount,
            t1.overdue_charge_version,
            t1.pay_code,
            t1.writeoff,
            t1.partner_code,
            t2.welab_product_id,
            t2.origin                   withdraw_origin,
            t3.tot_amt                  tot_amt,
            t3.tot_prin                 tot_prin,
            t3.tot_intr                 tot_intr,
            t3.tot_mgmt                 tot_mgmt,
            t3.tot_over                 tot_over,
            t3.tot_other                tot_other,
            t3.ystd_due_amt             due_amt,
            t3.ystd_due_prin            due_prin,
            t3.ystd_due_intr            due_intr,
            t3.ystd_due_mgmt            due_mgmt,
            t3.ystd_due_over            due_over,
            t3.ystd_due_over            due_other,
            t3.ystd_set_amt             set_amt,
            t3.ystd_set_prin            set_prin,
            t3.ystd_set_intr            set_intr,
            t3.ystd_set_mgmt            set_mgmt,
            t3.ystd_set_over            set_over,
            t3.ystd_set_other           set_other,
            t3.ystd_due_date            min_due_date,
            t3.sche_close               sche_close_date,
            t3.ystd_set_index           set_index,
            t3.ystd_over_day > 0        is_over,
            t3.ystd_over_day            over_day,
            t3.ystd_over_day_his > 0    is_over_his,
            t3.ystd_over_day_his        over_day_his,
            t3.fpd4_over_day            fpd_day,
            t3.mob1_over_day,
            t3.mob2_over_day,
            t3.mob3_over_day,
            t3.mob4_over_day,
            t3.mob5_over_day,
            t3.mob6_over_day,
            t3.mob7_over_day,
            t3.mob8_over_day,
            t3.mob9_over_day,
            t3.mob10_over_day,
            t3.mob11_over_day,
            t3.mob12_over_day,
            COUNT(*) OVER (PARTITION BY t2.map_application_id ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        disb_his_aply_disb_cnt,
            COUNT(*) OVER (PARTITION BY t1.borrower_id ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        disb_his_disb_cnt,
            LAST_VALUE(t1.application_id) OVER (PARTITION BY t1.borrower_id ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        disb_prev_disb_application_id,
            LAST_VALUE(t1.state) OVER (PARTITION BY t1.borrower_id ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        disb_prev_disb_state,
            LAST_VALUE(t2.welab_product_id) OVER (PARTITION BY t1.borrower_id ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        disb_prev_disb_welab_product_id,
            LAST_VALUE(t2.origin) OVER (PARTITION BY t1.borrower_id ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        disb_prev_disb_disb_origin,
            LAST_VALUE(t1.disbursed_at) OVER (PARTITION BY t1.borrower_id ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        disb_prev_disb_disbursed_at,
            COUNT(*) OVER (PARTITION BY t4.cnid ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_disb_his_disb_cnt,
            LAST_VALUE(t1.application_id) OVER (PARTITION BY t4.cnid ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_disb_prev_disb_application_id,
            LAST_VALUE(t1.state) OVER (PARTITION BY t4.cnid ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_disb_prev_disb_state,
            LAST_VALUE(t2.welab_product_id) OVER (PARTITION BY t4.cnid ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_disb_prev_disb_welab_product_id,
            LAST_VALUE(t2.origin) OVER (PARTITION BY t4.cnid ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_disb_prev_disb_disb_origin,
            LAST_VALUE(t1.disbursed_at) OVER (PARTITION BY t4.cnid ORDER BY t1.disbursed_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_disb_prev_disb_disbursed_at
FROM        wp_std.loans                    t1
LEFT JOIN   wp_calc.loan_map_application    t2 ON t1.application_id = t2.application_id
LEFT JOIN   wp_calc.loan_mob                t3 ON t1.id = t3.id
LEFT JOIN   wp_std.profiles                 t4 ON t1.borrower_id = t4.borrower_id
;
COMPUTE STATS wp_calc.loans;

DROP TABLE IF EXISTS wp_calc.loan_application_disburse;
CREATE TABLE wp_calc.loan_application_disburse STORED AS PARQUET AS
SELECT      t1.map_application_id application_id,
            COUNT(*) disb_cnt,
            GROUP_CONCAT(t2.application_id) loan_list,
            SUM(t3.is_aprv) close_cnt,
            SUM(t2.amount) sum_disb_amt,
            MIN(t2.disbursed_at) min_disb_at,
            MAX(t2.disbursed_at) max_disb_at,
            SUM(t2.tot_prin) sum_tot_prin,
            SUM(t2.due_prin) sum_due_prin,
            SUM(t2.set_prin) sum_set_prin,
            SUM(t2.due_amt) sum_due_amt,
            SUM(t2.set_amt) sum_set_amt,
            MIN(t2.min_due_date) min_min_due_date,
            MAX(t2.sche_close_date) max_sche_close_date,
            SUM(t2.is_over) sum_is_over,
            MAX(t2.over_day) max_over_day,
            SUM(t2.is_over_his) sum_is_over_his,
            MAX(t2.fpd_day) max_fpd_day
FROM        wp_calc.loan_map_application    t1
LEFT JOIN   wp_calc.loans                   t2 ON t1.application_id = t2.application_id
LEFT JOIN   wp_calc.loan_application_state  t3 ON t2.state = t3.code
GROUP BY    1
;
COMPUTE STATS wp_calc.loan_application_disburse;

DROP TABLE IF EXISTS wp_calc.user_loan_application;
CREATE TABLE wp_calc.user_loan_application AS
SELECT * FROM (
SELECT      borrower_id,
            COUNT(*) OVER (PARTITION BY borrower_id)
                                            aply_cnt,
            SUM(t2.is_aprv) OVER (PARTITION BY borrower_id)
                                            aprv_cnt,
            SUM(t2.is_rjct) OVER (PARTITION BY borrower_id)
                                            rjct_cnt,
            FIRST_VALUE(application_id) OVER (PARTITION BY borrower_id ORDER BY applied_at)
                                            first_aply_application_id,
            FIRST_VALUE(applied_at) OVER (PARTITION BY borrower_id ORDER BY applied_at)
                                            first_aply_applied_at,
            FIRST_VALUE(state) OVER (PARTITION BY borrower_id ORDER BY applied_at)
                                            first_aply_state,
            FIRST_VALUE(welab_product_id) OVER (PARTITION BY borrower_id ORDER BY applied_at)
                                            first_aply_welab_product_id,
            FIRST_VALUE(origin) OVER (PARTITION BY borrower_id ORDER BY applied_at)
                                            first_aply_origin,
            FIRST_VALUE(IF(is_aprv = 1, application_id, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_aprv_application_id,
            FIRST_VALUE(IF(is_aprv = 1, applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_aprv_applied_at,
            FIRST_VALUE(IF(is_aprv = 1, approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_aprv_approved_at,
            FIRST_VALUE(IF(is_aprv = 1, state, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_aprv_state,
            FIRST_VALUE(IF(is_aprv = 1, welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_aprv_welab_product_id,
            FIRST_VALUE(IF(is_aprv = 1, origin, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_aprv_origin,
            FIRST_VALUE(IF(is_rjct = 1, application_id, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_rjct_application_id,
            FIRST_VALUE(IF(is_rjct = 1, applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_rjct_applied_at,
            FIRST_VALUE(IF(is_rjct = 1, approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_rjct_approved_at,
            FIRST_VALUE(IF(is_rjct = 1, state, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_rjct_state,
            FIRST_VALUE(IF(is_rjct = 1, welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_rjct_welab_product_id,
            FIRST_VALUE(IF(is_rjct = 1, origin, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at)
                                            first_rjct_origin,
            FIRST_VALUE(application_id) OVER (PARTITION BY borrower_id ORDER BY applied_at DESC)
                                            latest_aply_application_id,
            FIRST_VALUE(applied_at) OVER (PARTITION BY borrower_id ORDER BY applied_at DESC)
                                            latest_aply_applied_at,
            FIRST_VALUE(state) OVER (PARTITION BY borrower_id ORDER BY applied_at DESC)
                                            latest_aply_state,
            FIRST_VALUE(welab_product_id) OVER (PARTITION BY borrower_id ORDER BY applied_at DESC)
                                            latest_aply_welab_product_id,
            FIRST_VALUE(origin) OVER (PARTITION BY borrower_id ORDER BY applied_at DESC)
                                            latest_aply_origin,
            FIRST_VALUE(IF(is_aprv = 1, application_id, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_aprv_application_id,
            FIRST_VALUE(IF(is_aprv = 1, applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_aprv_applied_at,
            FIRST_VALUE(IF(is_aprv = 1, approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_aprv_approved_at,
            FIRST_VALUE(IF(is_aprv = 1, state, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_aprv_state,
            FIRST_VALUE(IF(is_aprv = 1, welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_aprv_welab_product_id,
            FIRST_VALUE(IF(is_aprv = 1, origin, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_aprv_origin,
            FIRST_VALUE(IF(is_rjct = 1, application_id, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_rjct_application_id,
            FIRST_VALUE(IF(is_rjct = 1, applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_rjct_applied_at,
            FIRST_VALUE(IF(is_rjct = 1, approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_rjct_approved_at,
            FIRST_VALUE(IF(is_rjct = 1, state, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_rjct_state,
            FIRST_VALUE(IF(is_rjct = 1, welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_rjct_welab_product_id,
            FIRST_VALUE(IF(is_rjct = 1, origin, NULL) IGNORE NULLS) OVER (PARTITION BY borrower_id ORDER BY approved_at DESC)
                                            latest_rjct_origin,
            ROW_NUMBER() OVER (PARTITION BY borrower_id ORDER BY applied_at) rank_number
FROM        wp_calc.loan_applications       t1
LEFT JOIN   wp_calc.loan_application_state  t2 ON t1.state = t2.code
WHERE       t1.biz_type NOT IN ('credit_withdraw')
) tt
WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_loan_application;

DROP TABLE IF EXISTS wp_calc.user_loan;
CREATE TABLE wp_calc.user_loan AS
SELECT * FROM (
SELECT      borrower_id,
            COUNT(*) OVER (PARTITION BY borrower_id)
                                            disb_cnt,
            SUM(amount) OVER (PARTITION BY borrower_id)
                                            disb_amt,
            SUM(tot_amt) OVER (PARTITION BY borrower_id)
                                            sum_tot_amt,
            SUM(tot_prin) OVER (PARTITION BY borrower_id)
                                            sum_tot_prin,
            SUM(tot_intr) OVER (PARTITION BY borrower_id)
                                            sum_tot_intr,
            SUM(tot_mgmt) OVER (PARTITION BY borrower_id)
                                            sum_tot_mgmt,
            SUM(tot_over) OVER (PARTITION BY borrower_id)
                                            sum_tot_over,
            SUM(tot_other) OVER (PARTITION BY borrower_id)
                                            sum_tot_other,
            SUM(due_amt) OVER (PARTITION BY borrower_id)
                                            sum_due_amt,
            SUM(due_prin) OVER (PARTITION BY borrower_id)
                                            sum_due_prin,
            SUM(due_intr) OVER (PARTITION BY borrower_id)
                                            sum_due_intr,
            SUM(due_mgmt) OVER (PARTITION BY borrower_id)
                                            sum_due_mgmt,
            SUM(due_over) OVER (PARTITION BY borrower_id)
                                            sum_due_over,
            SUM(due_other) OVER (PARTITION BY borrower_id)
                                            sum_due_other,
            SUM(set_amt) OVER (PARTITION BY borrower_id)
                                            sum_set_amt,
            SUM(set_prin) OVER (PARTITION BY borrower_id)
                                            sum_set_prin,
            SUM(set_intr) OVER (PARTITION BY borrower_id)
                                            sum_set_intr,
            SUM(set_mgmt) OVER (PARTITION BY borrower_id)
                                            sum_set_mgmt,
            SUM(set_over) OVER (PARTITION BY borrower_id)
                                            sum_set_over,
            SUM(set_other) OVER (PARTITION BY borrower_id)
                                            sum_set_other,
            MIN(min_due_date) OVER (PARTITION BY borrower_id)
                                            min_min_due_date,
            MAX(sche_close_date) OVER (PARTITION BY borrower_id)
                                            max_sche_close_date,
            SUM(is_close) OVER (PARTITION BY borrower_id)
                                            sum_is_close,
            SUM(is_early) OVER (PARTITION BY borrower_id)
                                            sum_is_early,
            SUM(is_over) OVER (PARTITION BY borrower_id)
                                            sum_is_over,
            MAX(over_day) OVER (PARTITION BY borrower_id)
                                            max_over_day,
            SUM(is_over_his) OVER (PARTITION BY borrower_id)
                                            sum_is_over_his,
            MAX(over_day_his) OVER (PARTITION BY borrower_id)
                                            max_over_day_his,
            FIRST_VALUE(application_id) OVER (PARTITION BY borrower_id ORDER BY disbursed_at)
                                            first_disb_application_id,
            FIRST_VALUE(state) OVER (PARTITION BY borrower_id ORDER BY disbursed_at)
                                            first_disb_state,
            FIRST_VALUE(disbursed_at) OVER (PARTITION BY borrower_id ORDER BY disbursed_at)
                                            first_disb_disbursed_at, 
            FIRST_VALUE(amount) OVER (PARTITION BY borrower_id ORDER BY disbursed_at)
                                            first_disb_amount,
            FIRST_VALUE(welab_product_id) OVER (PARTITION BY borrower_id ORDER BY disbursed_at)
                                            first_disb_welab_product_id, 
            FIRST_VALUE(withdraw_origin) OVER (PARTITION BY borrower_id ORDER BY disbursed_at)
                                            first_disb_withdraw_origin,
            FIRST_VALUE(over_day) OVER (PARTITION BY borrower_id ORDER BY disbursed_at)
                                            first_disb_over_day,
            FIRST_VALUE(application_id) OVER (PARTITION BY borrower_id ORDER BY disbursed_at DESC)
                                            latest_disb_application_id,
            FIRST_VALUE(state) OVER (PARTITION BY borrower_id ORDER BY disbursed_at DESC)
                                            latest_disb_state,
            FIRST_VALUE(disbursed_at) OVER (PARTITION BY borrower_id ORDER BY disbursed_at DESC)
                                            latest_disb_disbursed_at,
            FIRST_VALUE(amount) OVER (PARTITION BY borrower_id ORDER BY disbursed_at DESC)
                                            latest_disb_amount,
            FIRST_VALUE(welab_product_id) OVER (PARTITION BY borrower_id ORDER BY disbursed_at DESC)
                                            latest_disb_welab_product_id,
            FIRST_VALUE(withdraw_origin) OVER (PARTITION BY borrower_id ORDER BY disbursed_at DESC)
                                            latest_disb_withdraw_origin,
            FIRST_VALUE(over_day) OVER (PARTITION BY borrower_id ORDER BY disbursed_at DESC)
                                            latest_disb_over_day,
            FIRST_VALUE(sche_close_date) OVER (PARTITION BY borrower_id ORDER BY disbursed_at DESC)
                                            latest_disb_sche_close_date,
            ROW_NUMBER() OVER (PARTITION BY borrower_id ORDER BY disbursed_at) rank_number
FROM        wp_calc.loans                   t1
LEFT JOIN   wp_calc.loan_application_state  t2 ON t1.state = t2.code
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.user_loan;

DROP TABLE IF EXISTS wp_calc.users;
CREATE TABLE wp_calc.users STORED AS PARQUET AS
SELECT      t01.id                                                 id,
            t01.mobile                                             mobile,
            t01.sign_in_count                                      sign_in_count,
            t01.current_sign_in_at                                 current_sign_in_at,
            t01.last_sign_in_at                                    last_sign_in_at,
            t01.current_sign_in_ip                                 current_sign_in_ip,
            t01.last_sign_in_ip                                    last_sign_in_ip,
            t01.created_at                                         created_at,
            t01.updated_at                                         updated_at,
            t01.blocked                                            blocked,
            t01.user_agent                                         user_agent,
            t01.origin                                             origin,
            t01.agent                                              agent,
            t01.referee_id                                         referee_id,
            t01.role_type                                          role_type,
            t01.product_code                                       product_code,
            t01.uuid                                               uuid,
            CAST(t02.uuid IS NOT NULL AS INT)                      is_credit,
            t04.name                                               name,
            t04.cnid                                               cnid,
            t04.verified                                           verified,
            t04.position_id                                        position_id,
            t04.marriage                                           marriage,
            t04.qq                                                 qq,
            t04.created_at                                         profile_created_at,
            t04.updated_at                                         profile_updated_at,
            t02.created_at                                         be_credit_at,
            t03.category                                           credit_category,
            t03.credit_line                                        credit_line,
            t03.freeze_state                                       credit_freeze_state,
            t03.state                                              credit_state,
            t03.origin                                             credit_origin,
            t05.province                                           profile_address_province,
            t05.city                                               profile_address_city,
            t05.district                                           profile_address_district,
            t05.street                                             profile_address_street,
            t05.telephone                                          profile_address_telephone,
            t05.`location`                                         profile_address_location,
            t05.created_at                                         profile_address_created_at,
            t05.updated_at                                         profile_address_updated_at,
            t06.name                                               company_name,
            t06.department                                         company_department,
            t06.company_position                                   company_position,
            t06.departure_time                                     company_departure_time,
            t06.position_id                                        company_position_id,
            t06.telephone                                          company_telephone,
            t06.salary_day                                         company_salary_day,
            t06.created_at                                         company_created_at,
            t06.updated_at                                         company_updated_at,
            t06.tot_cnt                                            company_tot_cnt,
            t06.prev_id                                            company_prev_id,
            t06.prev_name                                          company_prev_name,
            t06.prev_created_at                                    company_prev_created_at,
            t06.prev_updated_at                                    company_prev_updated_at,
            t06.first_up_at                                        company_first_up_at,
            t07.province                                           company_address_province,
            t07.city                                               company_address_city,
            t07.district                                           company_address_district,
            t07.street                                             company_address_street,
            t07.telephone                                          company_address_telephone,
            t07.`location`                                         company_address_location,
            t07.created_at                                         company_address_created_at,
            t07.updated_at                                         company_address_updated_at,
            t08.school                                             education_school,
            t08.degree_id                                          education_degree_id,
            t08.enrolled_at                                        education_enrolled_at,
            t08.graduated_at                                       education_graduated_at,
            t08.created_at                                         education_created_at,
            t08.updated_at                                         education_updated_at,
            t08.tot_cnt                                            education_tot_cnt,
            t08.prev_id                                            education_prev_id,
            t08.prev_school                                        education_prev_school,
            t08.prev_degree_id                                     education_prev_degree_id,
            t08.prev_created_at                                    education_prev_created_at,
            t08.first_up_at                                        education_first_up_at,
            t09.province                                           education_address_province,
            t09.city                                               education_address_city,
            t09.district                                           education_address_district,
            t09.street                                             education_address_street,
            t09.telephone                                          education_address_telephone,
            t09.`location`                                         education_address_location,
            t09.created_at                                         education_address_created_at,
            t09.updated_at                                         education_address_updated_at,
            t10.first_up_id_front_proof_at                         document_first_up_id_front_proof_at,
            t10.first_up_id_back_proof_at                          document_first_up_id_back_proof_at,
            t10.first_up_id_handheld_proof_at                      document_first_up_id_handheld_proof_at,
            t10.first_up_id_employment_proof_at                    document_first_up_id_employment_proof_at,
            t10.latest_up_id_front_proof_at                        document_latest_up_id_front_proof_at,
            t10.latest_up_id_back_proof_at                         document_latest_up_id_back_proof_at,
            t10.latest_up_id_handheld_proof_at                     document_latest_up_id_handheld_proof_at,
            t10.latest_up_id_employment_proof_at                   document_latest_up_id_employment_proof_at,
            t10.up_cnt_id_front_proof_at                           document_up_cnt_id_front_proof_at,
            t10.up_cnt_id_back_proof_at                            document_up_cnt_id_back_proof_at,
            t10.up_cnt_id_handheld_proof_at                        document_up_cnt_id_handheld_proof_at,
            t10.up_cnt_id_employment_proof_at                      document_up_cnt_id_employment_proof_at,
            t10.up_tot_cnt                                         document_up_tot_cnt,
            t10.up_doc_type_list                                   document_up_doc_type_list,
            t10.first_up_at                                        document_first_up_at,
            t10.latest_up_at                                       document_latest_up_at,
            t11.account_number                                     bank_card_account_number,
            t11.province                                           bank_card_province,
            t11.city                                               bank_card_city,
            t11.state                                              bank_card_state,
            t11.bank_id                                            bank_card_bank_id,
            t11.pay_code                                           bank_card_pay_code,
            t11.renew                                              bank_card_renew,
            t11.mobile                                             bank_card_mobile,
            t11.created_at                                         bank_card_created_at,
            t11.updated_at                                         bank_card_updated_at,
            t11.tot_cnt                                            bank_card_tot_cnt,
            t11.prev_id                                            bank_card_prev_id,
            t11.prev_account_number                                bank_card_prev_account_number,
            t11.prev_state                                         bank_card_prev_state,
            t11.prev_bank_id                                       bank_card_prev_bank_id,
            t11.prev_pay_code                                      bank_card_prev_pay_code,
            t11.prev_renew                                         bank_card_prev_renew,
            t11.prev_mobile                                        bank_card_prev_mobile,
            t11.prev_created_at                                    bank_card_prev_created_at,
            t11.prev_updated_at                                    bank_card_prev_updated_at,
            t11.first_up_at                                        bank_card_first_up_at,
            t12.income_month                                       attr_income_month,
            t12.asset_auto_type                                    attr_asset_auto_type,
            t12.work_period                                        attr_work_period,
            t12.max_monthly_repayment                              attr_max_monthly_repayment,
            t12.operating_year                                     attr_operating_year,
            t12.user_social_security                               attr_user_social_security,
            t12.monthly_average_income                             attr_monthly_average_income,
            t12.credit_status                                      attr_credit_status,
            t12.income_month_scope                                 attr_income_month_scope,
            t12.estimate_amount                                    attr_estimate_amount,
            t12.xunlei_amount                                      attr_xunlei_amount,
            t12.income_month_created_at                            attr_income_month_created_at,
            t12.asset_auto_type_created_at                         attr_asset_auto_type_created_at,
            t12.work_period_created_at                             attr_work_period_created_at,
            t12.max_monthly_repayment_created_at                   attr_max_monthly_repayment_created_at,
            t12.operating_year_created_at                          attr_operating_year_created_at,
            t12.user_social_security_created_at                    attr_user_social_security_created_at,
            t12.monthly_average_income_created_at                  attr_monthly_average_income_created_at,
            t12.credit_status_created_at                           attr_credit_status_created_at,
            t12.income_month_scope_created_at                      attr_income_month_scope_created_at,
            t12.estimate_amount_created_at                         attr_estimate_amount_created_at,
            t12.xunlei_amount_created_at                           attr_xunlei_amount_created_at,
            t12.income_month_updated_at                            attr_income_month_updated_at,
            t12.asset_auto_type_updated_at                         attr_asset_auto_type_updated_at,
            t12.work_period_updated_at                             attr_work_period_updated_at,
            t12.max_monthly_repayment_updated_at                   attr_max_monthly_repayment_updated_at,
            t12.operating_year_updated_at                          attr_operating_year_updated_at,
            t12.user_social_security_updated_at                    attr_user_social_security_updated_at,
            t12.monthly_average_income_updated_at                  attr_monthly_average_income_updated_at,
            t12.credit_status_updated_at                           attr_credit_status_updated_at,
            t12.income_month_scope_updated_at                      attr_income_month_scope_updated_at,
            t12.estimate_amount_updated_at                         attr_estimate_amount_updated_at,
            t12.xunlei_amount_updated_at                           attr_xunlei_amount_updated_at,
            t13.first_up_alipay_at                                 first_up_alipay_at,
            t13.latest_up_alipay_at                                latest_up_alipay_at,
            t14.first_up_credit_card_at                            first_up_credit_card_at,
            t14.latest_up_credit_card_at                           latest_up_credit_card_at,
            t15.first_up_gjj_at                                    first_up_gjj_at,
            t15.latest_up_gjj_at                                   latest_up_gjj_at,
            t16.first_up_isp_at                                    first_up_isp_at,
            t16.latest_up_isp_at                                   latest_up_isp_at,
            t17.first_up_jd_at                                     first_up_jd_at,
            t17.latest_up_jd_at                                    latest_up_jd_at,
            t18.first_up_people_bank_at                            first_up_people_bank_at,
            t18.latest_up_people_bank_at                           latest_up_people_bank_at,
            t19.first_up_shebao_at                                 first_up_shebao_at,
            t19.latest_up_shebao_at                                latest_up_shebao_at,
            t20.first_up_taobao_at                                 first_up_taobao_at,
            t20.latest_up_taobao_at                                latest_up_taobao_at,
            t21.first_up_weibo_at                                  first_up_weibo_at,
            t21.latest_up_weibo_at                                 latest_up_weibo_at,
            t22.first_up_zmxy_at                                   first_up_zmxy_at,
            t22.latest_up_zmxy_at                                  latest_up_zmxy_at,
            t23.tot_cnt                                            liaison_tot_cnt,
            t23.first_up_id                                        liaison_first_up_id,
            t23.first_up_name                                      liaison_first_up_name,
            t23.first_up_relationship                              liaison_first_up_relationship,
            t23.first_up_mobile                                    liaison_first_up_mobile,
            t23.first_up_company                                   liaison_first_up_company,
            t23.first_up_position                                  liaison_first_up_position,
            t23.first_up_source_id                                 liaison_first_up_source_id,
            t23.first_up_created_at                                liaison_first_up_created_at,
            t23.first_up_updated_at                                liaison_first_up_updated_at,
            t23.latest_up_id                                       liaison_latest_up_id,
            t23.latest_up_name                                     liaison_latest_up_name,
            t23.latest_up_relationship                             liaison_latest_up_relationship,
            t23.latest_up_mobile                                   liaison_latest_up_mobile,
            t23.latest_up_company                                  liaison_latest_up_company,
            t23.latest_up_position                                 liaison_latest_up_position,
            t23.latest_up_source_id                                liaison_latest_up_source_id,
            t23.latest_up_created_at                               liaison_latest_up_created_at,
            t23.latest_up_updated_at                               liaison_latest_up_updated_at,
            t23.liaison_1_id                                       liaison_1_id,
            t23.liaison_1_name                                     liaison_1_name,
            t23.liaison_1_relationship                             liaison_1_relationship,
            t23.liaison_1_mobile                                   liaison_1_mobile,
            t23.liaison_1_company                                  liaison_1_company,
            t23.liaison_1_position                                 liaison_1_position,
            t23.liaison_1_source_id                                liaison_1_source_id,
            t23.liaison_1_created_at                               liaison_1_created_at,
            t23.liaison_1_updated_at                               liaison_1_updated_at,
            t23.liaison_2_id                                       liaison_2_id,
            t23.liaison_2_name                                     liaison_2_name,
            t23.liaison_2_relationship                             liaison_2_relationship,
            t23.liaison_2_mobile                                   liaison_2_mobile,
            t23.liaison_2_company                                  liaison_2_company,
            t23.liaison_2_position                                 liaison_2_position,
            t23.liaison_2_source_id                                liaison_2_source_id,
            t23.liaison_2_created_at                               liaison_2_created_at,
            t23.liaison_2_updated_at                               liaison_2_updated_at,
            t23.liaison_3_id                                       liaison_3_id,
            t23.liaison_3_name                                     liaison_3_name,
            t23.liaison_3_relationship                             liaison_3_relationship,
            t23.liaison_3_mobile                                   liaison_3_mobile,
            t23.liaison_3_company                                  liaison_3_company,
            t23.liaison_3_position                                 liaison_3_position,
            t23.liaison_3_source_id                                liaison_3_source_id,
            t23.liaison_3_created_at                               liaison_3_created_at,
            t23.liaison_3_updated_at                               liaison_3_updated_at,
            t23.liaison_4_id                                       liaison_4_id,
            t23.liaison_4_name                                     liaison_4_name,
            t23.liaison_4_relationship                             liaison_4_relationship,
            t23.liaison_4_mobile                                   liaison_4_mobile,
            t23.liaison_4_company                                  liaison_4_company,
            t23.liaison_4_position                                 liaison_4_position,
            t23.liaison_4_source_id                                liaison_4_source_id,
            t23.liaison_4_created_at                               liaison_4_created_at,
            t23.liaison_4_updated_at                               liaison_4_updated_at,
            t23.liaison_5_id                                       liaison_5_id,
            t23.liaison_5_name                                     liaison_5_name,
            t23.liaison_5_relationship                             liaison_5_relationship,
            t23.liaison_5_mobile                                   liaison_5_mobile,
            t23.liaison_5_company                                  liaison_5_company,
            t23.liaison_5_position                                 liaison_5_position,
            t23.liaison_5_source_id                                liaison_5_source_id,
            t23.liaison_5_created_at                               liaison_5_created_at,
            t23.liaison_5_updated_at                               liaison_5_updated_at,
            t24.province                                           liaison_1_address_province,
            t24.city                                               liaison_1_address_city,
            t24.district                                           liaison_1_address_district,
            t24.street                                             liaison_1_address_street,
            t24.telephone                                          liaison_1_address_telephone,
            t24.`location`                                         liaison_1_address_location,
            t24.created_at                                         liaison_1_address_created_at,
            t24.updated_at                                         liaison_1_address_updated_at,
            t25.province                                           liaison_2_address_province,
            t25.city                                               liaison_2_address_city,
            t25.district                                           liaison_2_address_district,
            t25.street                                             liaison_2_address_street,
            t25.telephone                                          liaison_2_address_telephone,
            t25.`location`                                         liaison_2_address_location,
            t25.created_at                                         liaison_2_address_created_at,
            t25.updated_at                                         liaison_2_address_updated_at,
            t26.province                                           liaison_3_address_province,
            t26.city                                               liaison_3_address_city,
            t26.district                                           liaison_3_address_district,
            t26.street                                             liaison_3_address_street,
            t26.telephone                                          liaison_3_address_telephone,
            t26.`location`                                         liaison_3_address_location,
            t26.created_at                                         liaison_3_address_created_at,
            t26.updated_at                                         liaison_3_address_updated_at,
            t27.province                                           liaison_4_address_province,
            t27.city                                               liaison_4_address_city,
            t27.district                                           liaison_4_address_district,
            t27.street                                             liaison_4_address_street,
            t27.telephone                                          liaison_4_address_telephone,
            t27.`location`                                         liaison_4_address_location,
            t27.created_at                                         liaison_4_address_created_at,
            t27.updated_at                                         liaison_4_address_updated_at,
            t28.province                                           liaison_5_address_province,
            t28.city                                               liaison_5_address_city,
            t28.district                                           liaison_5_address_district,
            t28.street                                             liaison_5_address_street,
            t28.telephone                                          liaison_5_address_telephone,
            t28.`location`                                         liaison_5_address_location,
            t28.created_at                                         liaison_5_address_created_at,
            t28.updated_at                                         liaison_5_address_updated_at,
            t29.aply_cnt                                           aply_cnt,
            t29.aprv_cnt                                           aprv_cnt,
            t29.rjct_cnt                                           rjct_cnt,
            t29.first_aply_application_id                          first_aply_application_id,
            t29.first_aply_applied_at                              first_aply_applied_at,
            t29.first_aply_state                                   first_aply_state,
            t29.first_aply_welab_product_id                        first_aply_welab_product_id,
            t29.first_aply_origin                                  first_aply_origin,
            t29.first_aprv_application_id                          first_aprv_application_id,
            t29.first_aprv_applied_at                              first_aprv_applied_at,
            t29.first_aprv_approved_at                             first_aprv_approved_at,
            t29.first_aprv_state                                   first_aprv_state,
            t29.first_aprv_welab_product_id                        first_aprv_welab_product_id,
            t29.first_aprv_origin                                  first_aprv_origin,
            t29.first_rjct_application_id                          first_rjct_application_id,
            t29.first_rjct_applied_at                              first_rjct_applied_at,
            t29.first_rjct_approved_at                             first_rjct_approved_at,
            t29.first_rjct_state                                   first_rjct_state,
            t29.first_rjct_welab_product_id                        first_rjct_welab_product_id,
            t29.first_rjct_origin                                  first_rjct_origin,
            t29.latest_aply_application_id                         latest_aply_application_id,
            t29.latest_aply_applied_at                             latest_aply_applied_at,
            t29.latest_aply_state                                  latest_aply_state,
            t29.latest_aply_welab_product_id                       latest_aply_welab_product_id,
            t29.latest_aply_origin                                 latest_aply_origin,
            t29.latest_aprv_application_id                         latest_aprv_application_id,
            t29.latest_aprv_applied_at                             latest_aprv_applied_at,
            t29.latest_aprv_approved_at                            latest_aprv_approved_at,
            t29.latest_aprv_state                                  latest_aprv_state,
            t29.latest_aprv_welab_product_id                       latest_aprv_welab_product_id,
            t29.latest_aprv_origin                                 latest_aprv_origin,
            t29.latest_rjct_application_id                         latest_rjct_application_id,
            t29.latest_rjct_applied_at                             latest_rjct_applied_at,
            t29.latest_rjct_approved_at                            latest_rjct_approved_at,
            t29.latest_rjct_state                                  latest_rjct_state,
            t29.latest_rjct_welab_product_id                       latest_rjct_welab_product_id,
            t29.latest_rjct_origin                                 latest_rjct_origin,
            t30.disb_cnt                                           disb_cnt,
            t30.disb_amt                                           disb_amt,
            t30.sum_tot_amt                                        sum_tot_amt,
            t30.sum_tot_prin                                       sum_tot_prin,
            t30.sum_tot_intr                                       sum_tot_intr,
            t30.sum_tot_mgmt                                       sum_tot_mgmt,
            t30.sum_tot_over                                       sum_tot_over,
            t30.sum_tot_other                                      sum_tot_other,
            t30.sum_due_amt                                        sum_due_amt,
            t30.sum_due_prin                                       sum_due_prin,
            t30.sum_due_intr                                       sum_due_intr,
            t30.sum_due_mgmt                                       sum_due_mgmt,
            t30.sum_due_over                                       sum_due_over,
            t30.sum_due_other                                      sum_due_other,
            t30.sum_set_amt                                        sum_set_amt,
            t30.sum_set_prin                                       sum_set_prin,
            t30.sum_set_intr                                       sum_set_intr,
            t30.sum_set_mgmt                                       sum_set_mgmt,
            t30.sum_set_over                                       sum_set_over,
            t30.sum_set_other                                      sum_set_other,
            t30.min_min_due_date                                   min_min_due_date,
            t30.max_sche_close_date                                max_sche_close_date,
            t30.sum_is_close                                       sum_is_close,
            t30.sum_is_early                                       sum_is_early,
            t30.sum_is_over                                        sum_is_over,
            t30.max_over_day                                       max_over_day,
            t30.sum_is_over_his                                    sum_is_over_his,
            t30.max_over_day_his                                   max_over_day_his,
            t30.first_disb_application_id                          first_disb_application_id,
            t30.first_disb_state                                   first_disb_state,
            t30.first_disb_disbursed_at                            first_disb_disbursed_at,
            t30.first_disb_amount                                  first_disb_amount,
            t30.first_disb_welab_product_id                        first_disb_welab_product_id,
            t30.first_disb_withdraw_origin                         first_disb_withdraw_origin,
            t30.first_disb_over_day                                first_disb_over_day,
            t30.latest_disb_application_id                         latest_disb_application_id,
            t30.latest_disb_state                                  latest_disb_state,
            t30.latest_disb_disbursed_at                           latest_disb_disbursed_at,
            t30.latest_disb_amount                                 latest_disb_amount,
            t30.latest_disb_welab_product_id                       latest_disb_welab_product_id,
            t30.latest_disb_withdraw_origin                        latest_disb_withdraw_origin,
            t30.latest_disb_over_day                               latest_disb_over_day,
            t30.latest_disb_sche_close_date                        latest_disb_sche_close_date
FROM        wp_std.users                    t01
LEFT JOIN   wp_std.quota_shunts             t02 ON t01.uuid = t02.uuid
LEFT JOIN   wp_std.user_quotas              t03 ON t01.uuid = t03.uuid
LEFT JOIN   wp_std.profiles                 t04 ON t01.id = t04.borrower_id
LEFT JOIN   wp_calc.addresses               t05 ON t04.id = t05.addressable_id AND t05.addressable_type = 'Profile'
LEFT JOIN   wp_calc.user_company            t06 ON t01.id = t06.user_id
LEFT JOIN   wp_calc.addresses               t07 ON t06.id = t07.addressable_id AND t07.addressable_type = 'Company'
LEFT JOIN   wp_calc.user_education          t08 ON t01.id = t08.user_id
LEFT JOIN   wp_calc.addresses               t09 ON t08.id = t09.addressable_id AND t09.addressable_type = 'Education'
LEFT JOIN   wp_calc.user_document           t10 ON t01.id = t10.documentable_id
LEFT JOIN   wp_calc.user_bank_card          t11 ON t01.id = t11.user_id
LEFT JOIN   wp_calc.user_attribute          t12 ON t01.id = t12.user_id
LEFT JOIN   wp_calc.user_up_alipay          t13 ON t01.mobile = t13.account
LEFT JOIN   wp_calc.user_up_credit_card     t14 ON t01.mobile = t14.account
LEFT JOIN   wp_calc.user_up_gjj             t15 ON t01.mobile = t15.account
LEFT JOIN   wp_calc.user_up_isp             t16 ON t01.mobile = t16.account
LEFT JOIN   wp_calc.user_up_jd              t17 ON t01.mobile = t17.account
LEFT JOIN   wp_calc.user_up_people_bank     t18 ON t01.mobile = t18.account
LEFT JOIN   wp_calc.user_up_shebao          t19 ON t01.mobile = t19.account
LEFT JOIN   wp_calc.user_up_taobao          t20 ON t01.mobile = t20.account
LEFT JOIN   wp_calc.user_up_weibo           t21 ON t01.mobile = t21.account
LEFT JOIN   wp_calc.user_up_zmxy            t22 ON t01.mobile = t22.account
LEFT JOIN   wp_calc.user_liaison            t23 ON t01.id = t23.user_id
LEFT JOIN   wp_calc.addresses               t24 ON t23.liaison_1_id = t24.addressable_id AND t24.addressable_type = 'Liaison'
LEFT JOIN   wp_calc.addresses               t25 ON t23.liaison_2_id = t25.addressable_id AND t25.addressable_type = 'Liaison'
LEFT JOIN   wp_calc.addresses               t26 ON t23.liaison_3_id = t26.addressable_id AND t26.addressable_type = 'Liaison'
LEFT JOIN   wp_calc.addresses               t27 ON t23.liaison_4_id = t27.addressable_id AND t27.addressable_type = 'Liaison'
LEFT JOIN   wp_calc.addresses               t28 ON t23.liaison_5_id = t28.addressable_id AND t28.addressable_type = 'Liaison'
LEFT JOIN   wp_calc.user_loan_application   t29 ON t01.id = t29.borrower_id
LEFT JOIN   wp_calc.user_loan               t30 ON t01.id = t30.borrower_id
;
COMPUTE STATS wp_calc.users;

-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS wp_calc.cnid_loan_application;
CREATE TABLE wp_calc.cnid_loan_application AS
SELECT * FROM (
SELECT      cnid,
            COUNT(*) OVER (PARTITION BY cnid)
                                            aply_cnt,
            SUM(t2.is_aprv) OVER (PARTITION BY cnid)
                                            aprv_cnt,
            SUM(t2.is_rjct) OVER (PARTITION BY cnid)
                                            rjct_cnt,
            FIRST_VALUE(application_id) OVER (PARTITION BY cnid ORDER BY applied_at)
                                            first_aply_application_id,
            FIRST_VALUE(applied_at) OVER (PARTITION BY cnid ORDER BY applied_at)
                                            first_aply_applied_at,
            FIRST_VALUE(state) OVER (PARTITION BY cnid ORDER BY applied_at)
                                            first_aply_state,
            FIRST_VALUE(welab_product_id) OVER (PARTITION BY cnid ORDER BY applied_at)
                                            first_aply_welab_product_id,
            FIRST_VALUE(origin) OVER (PARTITION BY cnid ORDER BY applied_at)
                                            first_aply_origin,
            FIRST_VALUE(IF(is_aprv = 1, application_id, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_aprv_application_id,
            FIRST_VALUE(IF(is_aprv = 1, applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_aprv_applied_at,
            FIRST_VALUE(IF(is_aprv = 1, approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_aprv_approved_at,
            FIRST_VALUE(IF(is_aprv = 1, state, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_aprv_state,
            FIRST_VALUE(IF(is_aprv = 1, welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_aprv_welab_product_id,
            FIRST_VALUE(IF(is_aprv = 1, origin, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_aprv_origin,
            FIRST_VALUE(IF(is_rjct = 1, application_id, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_rjct_application_id,
            FIRST_VALUE(IF(is_rjct = 1, applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_rjct_applied_at,
            FIRST_VALUE(IF(is_rjct = 1, approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_rjct_approved_at,
            FIRST_VALUE(IF(is_rjct = 1, state, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_rjct_state,
            FIRST_VALUE(IF(is_rjct = 1, welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_rjct_welab_product_id,
            FIRST_VALUE(IF(is_rjct = 1, origin, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at)
                                            first_rjct_origin,
            FIRST_VALUE(application_id) OVER (PARTITION BY cnid ORDER BY applied_at DESC)
                                            latest_aply_application_id,
            FIRST_VALUE(applied_at) OVER (PARTITION BY cnid ORDER BY applied_at DESC)
                                            latest_aply_applied_at,
            FIRST_VALUE(state) OVER (PARTITION BY cnid ORDER BY applied_at DESC)
                                            latest_aply_state,
            FIRST_VALUE(welab_product_id) OVER (PARTITION BY cnid ORDER BY applied_at DESC)
                                            latest_aply_welab_product_id,
            FIRST_VALUE(origin) OVER (PARTITION BY cnid ORDER BY applied_at DESC)
                                            latest_aply_origin,
            FIRST_VALUE(IF(is_aprv = 1, application_id, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_aprv_application_id,
            FIRST_VALUE(IF(is_aprv = 1, applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_aprv_applied_at,
            FIRST_VALUE(IF(is_aprv = 1, approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_aprv_approved_at,
            FIRST_VALUE(IF(is_aprv = 1, state, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_aprv_state,
            FIRST_VALUE(IF(is_aprv = 1, welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_aprv_welab_product_id,
            FIRST_VALUE(IF(is_aprv = 1, origin, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_aprv_origin,
            FIRST_VALUE(IF(is_rjct = 1, application_id, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_rjct_application_id,
            FIRST_VALUE(IF(is_rjct = 1, applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_rjct_applied_at,
            FIRST_VALUE(IF(is_rjct = 1, approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_rjct_approved_at,
            FIRST_VALUE(IF(is_rjct = 1, state, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_rjct_state,
            FIRST_VALUE(IF(is_rjct = 1, welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_rjct_welab_product_id,
            FIRST_VALUE(IF(is_rjct = 1, origin, NULL) IGNORE NULLS) OVER (PARTITION BY cnid ORDER BY approved_at DESC)
                                            latest_rjct_origin,
            ROW_NUMBER() OVER (PARTITION BY cnid ORDER BY applied_at) rank_number
FROM        wp_calc.loan_applications       t1
LEFT JOIN   wp_calc.loan_application_state  t2 ON t1.state = t2.code
LEFT JOIN   wp_std.profiles                 t3 ON t1.borrower_id = t3.borrower_id
WHERE       t1.biz_type IN ('normal', 'credit_application')
) tt
WHERE rank_number = 1
;
COMPUTE STATS wp_calc.cnid_loan_application;



DROP TABLE IF EXISTS wp_calc.cnid_loan;
CREATE TABLE wp_calc.cnid_loan AS
SELECT * FROM (
SELECT      cnid,
            COUNT(*) OVER (PARTITION BY cnid)
                                            disb_cnt,
            SUM(amount) OVER (PARTITION BY cnid)
                                            disb_amt,
            SUM(tot_amt) OVER (PARTITION BY cnid)
                                            sum_tot_amt,
            SUM(tot_prin) OVER (PARTITION BY cnid)
                                            sum_tot_prin,
            SUM(tot_intr) OVER (PARTITION BY cnid)
                                            sum_tot_intr,
            SUM(tot_mgmt) OVER (PARTITION BY cnid)
                                            sum_tot_mgmt,
            SUM(tot_over) OVER (PARTITION BY cnid)
                                            sum_tot_over,
            SUM(tot_other) OVER (PARTITION BY cnid)
                                            sum_tot_other,
            SUM(due_amt) OVER (PARTITION BY cnid)
                                            sum_due_amt,
            SUM(due_prin) OVER (PARTITION BY cnid)
                                            sum_due_prin,
            SUM(due_intr) OVER (PARTITION BY cnid)
                                            sum_due_intr,
            SUM(due_mgmt) OVER (PARTITION BY cnid)
                                            sum_due_mgmt,
            SUM(due_over) OVER (PARTITION BY cnid)
                                            sum_due_over,
            SUM(due_other) OVER (PARTITION BY cnid)
                                            sum_due_other,
            SUM(set_amt) OVER (PARTITION BY cnid)
                                            sum_set_amt,
            SUM(set_prin) OVER (PARTITION BY cnid)
                                            sum_set_prin,
            SUM(set_intr) OVER (PARTITION BY cnid)
                                            sum_set_intr,
            SUM(set_mgmt) OVER (PARTITION BY cnid)
                                            sum_set_mgmt,
            SUM(set_over) OVER (PARTITION BY cnid)
                                            sum_set_over,
            SUM(set_other) OVER (PARTITION BY cnid)
                                            sum_set_other,
            MIN(min_due_date) OVER (PARTITION BY cnid)
                                            min_min_due_date,
            MAX(sche_close_date) OVER (PARTITION BY cnid)
                                            max_sche_close_date,
            SUM(is_close) OVER (PARTITION BY cnid)
                                            sum_is_close,
            SUM(is_early) OVER (PARTITION BY cnid)
                                            sum_is_early,
            SUM(is_over) OVER (PARTITION BY cnid)
                                            sum_is_over,
            MAX(over_day) OVER (PARTITION BY cnid)
                                            max_over_day,
            SUM(is_over_his) OVER (PARTITION BY cnid)
                                            sum_is_over_his,
            MAX(over_day_his) OVER (PARTITION BY cnid)
                                            max_over_day_his,
            FIRST_VALUE(application_id) OVER (PARTITION BY cnid ORDER BY disbursed_at)
                                            first_disb_application_id,
            FIRST_VALUE(state) OVER (PARTITION BY cnid ORDER BY disbursed_at)
                                            first_disb_state,
            FIRST_VALUE(disbursed_at) OVER (PARTITION BY cnid ORDER BY disbursed_at)
                                            first_disb_disbursed_at, 
            FIRST_VALUE(amount) OVER (PARTITION BY cnid ORDER BY disbursed_at)
                                            first_disb_amount,
            FIRST_VALUE(welab_product_id) OVER (PARTITION BY cnid ORDER BY disbursed_at)
                                            first_disb_welab_product_id, 
            FIRST_VALUE(withdraw_origin) OVER (PARTITION BY cnid ORDER BY disbursed_at)
                                            first_disb_withdraw_origin,
            FIRST_VALUE(over_day) OVER (PARTITION BY cnid ORDER BY disbursed_at)
                                            first_disb_over_day,
            FIRST_VALUE(application_id) OVER (PARTITION BY cnid ORDER BY disbursed_at DESC)
                                            latest_disb_application_id,
            FIRST_VALUE(state) OVER (PARTITION BY cnid ORDER BY disbursed_at DESC)
                                            latest_disb_state,
            FIRST_VALUE(disbursed_at) OVER (PARTITION BY cnid ORDER BY disbursed_at DESC)
                                            latest_disb_disbursed_at,
            FIRST_VALUE(amount) OVER (PARTITION BY cnid ORDER BY disbursed_at DESC)
                                            latest_disb_amount,
            FIRST_VALUE(welab_product_id) OVER (PARTITION BY cnid ORDER BY disbursed_at DESC)
                                            latest_disb_welab_product_id,
            FIRST_VALUE(withdraw_origin) OVER (PARTITION BY cnid ORDER BY disbursed_at DESC)
                                            latest_disb_withdraw_origin,
            FIRST_VALUE(over_day) OVER (PARTITION BY cnid ORDER BY disbursed_at DESC)
                                            latest_disb_over_day,
            FIRST_VALUE(sche_close_date) OVER (PARTITION BY cnid ORDER BY disbursed_at DESC)
                                            latest_disb_sche_close_date,
            ROW_NUMBER() OVER (PARTITION BY cnid ORDER BY disbursed_at) rank_number
FROM        wp_calc.loans                   t1
LEFT JOIN   wp_calc.loan_application_state  t2 ON t1.state = t2.code
LEFT JOIN   wp_std.profiles                 t3 ON t1.borrower_id = t3.borrower_id
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.cnid_loan;


DROP TABLE IF EXISTS wp_calc.cnid_users;
CREATE TABLE wp_calc.cnid_users AS
SELECT * FROM (
SELECT      cnid,
            name,
            mobile,
            blocked,
            origin,
            agent,
            role_type,
            verified,
            position_id,
            marriage,
            qq,
            is_credit,
            credit_line,
            COUNT(*) OVER (PARTITION BY cnid)
                                            register_cnt,
            MIN(created_at) OVER (PARTITION BY cnid)
                                            min_created_at,
            MAX(created_at) OVER (PARTITION BY cnid)
                                            max_created_at,
            FIRST_VALUE(profile_created_at) OVER (PARTITION BY cnid ORDER BY profile_created_at DESC, id DESC)
                                            profile_created_at,
            FIRST_VALUE(profile_updated_at) OVER (PARTITION BY cnid ORDER BY profile_created_at DESC, id DESC)
                                            profile_updated_at,
            FIRST_VALUE(profile_address_province) OVER (PARTITION BY cnid ORDER BY profile_created_at DESC, id DESC)
                                            profile_address_province,
            FIRST_VALUE(profile_address_city) OVER (PARTITION BY cnid ORDER BY profile_created_at DESC, id DESC)
                                            profile_address_city,
            FIRST_VALUE(profile_address_district) OVER (PARTITION BY cnid ORDER BY profile_created_at DESC, id DESC)
                                            profile_address_district,
            FIRST_VALUE(profile_address_street) OVER (PARTITION BY cnid ORDER BY profile_created_at DESC, id DESC)
                                            profile_address_street,
            FIRST_VALUE(profile_address_telephone) OVER (PARTITION BY cnid ORDER BY profile_created_at DESC, id DESC)
                                            profile_address_telephone,
            FIRST_VALUE(profile_address_location) OVER (PARTITION BY cnid ORDER BY profile_created_at DESC, id DESC)
                                            profile_address_location,
            FIRST_VALUE(profile_address_created_at) OVER (PARTITION BY cnid ORDER BY profile_created_at DESC, id DESC)
                                            profile_address_created_at,
            FIRST_VALUE(profile_address_updated_at) OVER (PARTITION BY cnid ORDER BY profile_created_at DESC, id DESC)
                                            profile_address_updated_at,
            FIRST_VALUE(company_name) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_name,
            FIRST_VALUE(company_department) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_department,
            FIRST_VALUE(company_position) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_position,
            FIRST_VALUE(company_departure_time) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_departure_time,
            FIRST_VALUE(company_position_id) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_position_id,
            FIRST_VALUE(company_telephone) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_telephone,
            FIRST_VALUE(company_salary_day) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_salary_day,
            FIRST_VALUE(company_created_at) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_created_at,
            FIRST_VALUE(company_updated_at) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_updated_at,
            SUM(company_tot_cnt) OVER (PARTITION BY cnid)
                                            company_tot_cnt,
            MIN(company_first_up_at) OVER (PARTITION BY cnid)
                                            company_first_up_at,
            FIRST_VALUE(company_address_province) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_address_province,
            FIRST_VALUE(company_address_city) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_address_city,
            FIRST_VALUE(company_address_district) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_address_district,
            FIRST_VALUE(company_address_street) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_address_street,
            FIRST_VALUE(company_address_telephone) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_address_telephone,
            FIRST_VALUE(company_address_location) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_address_location,
            FIRST_VALUE(company_address_created_at) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_address_created_at,
            FIRST_VALUE(company_address_updated_at) OVER (PARTITION BY cnid ORDER BY company_created_at DESC, id DESC)
                                            company_address_updated_at,
            FIRST_VALUE(education_school) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_school,
            FIRST_VALUE(education_degree_id) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_degree_id,
            FIRST_VALUE(education_enrolled_at) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_enrolled_at,
            FIRST_VALUE(education_graduated_at) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_graduated_at,
            FIRST_VALUE(education_created_at) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_created_at,
            FIRST_VALUE(education_updated_at) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_updated_at,
            SUM(education_tot_cnt) OVER (PARTITION BY cnid)
                                            education_tot_cnt,
            MIN(education_first_up_at) OVER (PARTITION BY cnid)
                                            education_first_up_at,
            FIRST_VALUE(education_address_province) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_address_province,
            FIRST_VALUE(education_address_city) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_address_city,
            FIRST_VALUE(education_address_district) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_address_district,
            FIRST_VALUE(education_address_street) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_address_street,
            FIRST_VALUE(education_address_telephone) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_address_telephone,
            FIRST_VALUE(education_address_location) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_address_location,
            FIRST_VALUE(education_address_created_at) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_address_created_at,
            FIRST_VALUE(education_address_updated_at) OVER (PARTITION BY cnid ORDER BY education_created_at DESC, id DESC)
                                            education_address_updated_at,
            MIN(document_first_up_id_front_proof_at) OVER (PARTITION BY cnid)
                                            document_first_up_id_front_proof_at,
            MIN(document_first_up_id_back_proof_at) OVER (PARTITION BY cnid)
                                            document_first_up_id_back_proof_at,
            MIN(document_first_up_id_handheld_proof_at) OVER (PARTITION BY cnid)
                                            document_first_up_id_handheld_proof_at,
            MIN(document_first_up_id_employment_proof_at) OVER (PARTITION BY cnid)
                                            document_first_up_id_employment_proof_at,
            MAX(document_latest_up_id_front_proof_at) OVER (PARTITION BY cnid)
                                            document_latest_up_id_front_proof_at,
            MAX(document_latest_up_id_back_proof_at) OVER (PARTITION BY cnid)
                                            document_latest_up_id_back_proof_at,
            MAX(document_latest_up_id_handheld_proof_at) OVER (PARTITION BY cnid)
                                            document_latest_up_id_handheld_proof_at,
            MAX(document_latest_up_id_employment_proof_at) OVER (PARTITION BY cnid)
                                            document_latest_up_id_employment_proof_at,
            SUM(document_up_cnt_id_front_proof_at) OVER (PARTITION BY cnid)
                                            document_up_cnt_id_front_proof_at,
            SUM(document_up_cnt_id_back_proof_at) OVER (PARTITION BY cnid)
                                            document_up_cnt_id_back_proof_at,
            SUM(document_up_cnt_id_handheld_proof_at) OVER (PARTITION BY cnid)
                                            document_up_cnt_id_handheld_proof_at,
            SUM(document_up_cnt_id_employment_proof_at) OVER (PARTITION BY cnid)
                                            document_up_cnt_id_employment_proof_at,
            SUM(document_up_tot_cnt) OVER (PARTITION BY cnid)
                                            document_up_tot_cnt,
            MIN(document_first_up_at) OVER (PARTITION BY cnid)
                                            document_first_up_at,
            MAX(document_latest_up_at) OVER (PARTITION BY cnid)
                                            document_latest_up_at,
            FIRST_VALUE(bank_card_account_number) OVER (PARTITION BY cnid ORDER BY bank_card_created_at DESC, id DESC)
                                            bank_card_account_number,
            FIRST_VALUE(bank_card_province) OVER (PARTITION BY cnid ORDER BY bank_card_created_at DESC, id DESC)
                                            bank_card_province,
            FIRST_VALUE(bank_card_city) OVER (PARTITION BY cnid ORDER BY bank_card_created_at DESC, id DESC)
                                            bank_card_city,
            FIRST_VALUE(bank_card_state) OVER (PARTITION BY cnid ORDER BY bank_card_created_at DESC, id DESC)
                                            bank_card_state,
            FIRST_VALUE(bank_card_bank_id) OVER (PARTITION BY cnid ORDER BY bank_card_created_at DESC, id DESC)
                                            bank_card_bank_id,
            FIRST_VALUE(bank_card_pay_code) OVER (PARTITION BY cnid ORDER BY bank_card_created_at DESC, id DESC)
                                            bank_card_pay_code,
            FIRST_VALUE(bank_card_renew) OVER (PARTITION BY cnid ORDER BY bank_card_created_at DESC, id DESC)
                                            bank_card_renew,
            FIRST_VALUE(bank_card_mobile) OVER (PARTITION BY cnid ORDER BY bank_card_created_at DESC, id DESC)
                                            bank_card_mobile,
            FIRST_VALUE(bank_card_created_at) OVER (PARTITION BY cnid ORDER BY bank_card_created_at DESC, id DESC)
                                            bank_card_created_at,
            FIRST_VALUE(bank_card_updated_at) OVER (PARTITION BY cnid ORDER BY bank_card_created_at DESC, id DESC)
                                            bank_card_updated_at,
            SUM(bank_card_tot_cnt) OVER (PARTITION BY cnid)
                                            bank_card_tot_cnt,
            MIN(bank_card_first_up_at) OVER (PARTITION BY cnid)
                                            bank_card_first_up_at,
            /* FIRST_VALUE(attr_income_month) OVER (PARTITION BY cnid ORDER BY attr_income_month_created_at DESC, id DESC)
                                            attr_income_month,
            FIRST_VALUE(attr_asset_auto_type) OVER (PARTITION BY cnid ORDER BY attr_asset_auto_type_created_at DESC, id DESC)
                                            attr_asset_auto_type,
            FIRST_VALUE(attr_work_period) OVER (PARTITION BY cnid ORDER BY attr_work_period_created_at DESC, id DESC)
                                            attr_work_period,
            FIRST_VALUE(attr_max_monthly_repayment) OVER (PARTITION BY cnid ORDER BY attr_max_monthly_repayment_created_at DESC, id DESC)
                                            attr_max_monthly_repayment,
            FIRST_VALUE(attr_operating_year) OVER (PARTITION BY cnid ORDER BY attr_operating_year_created_at DESC, id DESC)
                                            attr_operating_year,
            FIRST_VALUE(attr_user_social_security) OVER (PARTITION BY cnid ORDER BY attr_user_social_security_created_at DESC, id DESC)
                                            attr_user_social_security,
            FIRST_VALUE(attr_monthly_average_income) OVER (PARTITION BY cnid ORDER BY attr_monthly_average_income_created_at DESC, id DESC)
                                            attr_monthly_average_income,
            FIRST_VALUE(attr_credit_status) OVER (PARTITION BY cnid ORDER BY attr_credit_status_created_at DESC, id DESC)
                                            attr_credit_status,
            FIRST_VALUE(attr_income_month_scope) OVER (PARTITION BY cnid ORDER BY attr_income_month_scope_created_at DESC, id DESC)
                                            attr_income_month_scope,
            FIRST_VALUE(attr_estimate_amount) OVER (PARTITION BY cnid ORDER BY attr_estimate_amount_created_at DESC, id DESC)
                                            attr_estimate_amount,
            FIRST_VALUE(attr_xunlei_amount) OVER (PARTITION BY cnid ORDER BY attr_xunlei_amount_created_at DESC, id DESC)
                                            attr_xunlei_amount, */
            MIN(first_up_alipay_at) OVER (PARTITION BY cnid)
                                            first_up_alipay_at,
            MAX(latest_up_alipay_at) OVER (PARTITION BY cnid)
                                            latest_up_alipay_at,
            MIN(first_up_credit_card_at) OVER (PARTITION BY cnid)
                                            first_up_credit_card_at,
            MAX(latest_up_credit_card_at) OVER (PARTITION BY cnid)
                                            latest_up_credit_card_at,
            MIN(first_up_gjj_at) OVER (PARTITION BY cnid)
                                            first_up_gjj_at,
            MAX(latest_up_gjj_at) OVER (PARTITION BY cnid)
                                            latest_up_gjj_at,
            MIN(first_up_isp_at) OVER (PARTITION BY cnid)
                                            first_up_isp_at,
            MAX(latest_up_isp_at) OVER (PARTITION BY cnid)
                                            latest_up_isp_at,
            MIN(first_up_jd_at) OVER (PARTITION BY cnid)
                                            first_up_jd_at,
            MAX(latest_up_jd_at) OVER (PARTITION BY cnid)
                                            latest_up_jd_at,
            MIN(first_up_people_bank_at) OVER (PARTITION BY cnid)
                                            first_up_people_bank_at,
            MAX(latest_up_people_bank_at) OVER (PARTITION BY cnid)
                                            latest_up_people_bank_at,
            MIN(first_up_shebao_at) OVER (PARTITION BY cnid)
                                            first_up_shebao_at,
            MAX(latest_up_shebao_at) OVER (PARTITION BY cnid)
                                            latest_up_shebao_at,
            MIN(first_up_taobao_at) OVER (PARTITION BY cnid)
                                            first_up_taobao_at,
            MAX(latest_up_taobao_at) OVER (PARTITION BY cnid)
                                            latest_up_taobao_at,
            MIN(first_up_weibo_at) OVER (PARTITION BY cnid)
                                            first_up_weibo_at,
            MAX(latest_up_weibo_at) OVER (PARTITION BY cnid)
                                            latest_up_weibo_at,
            MIN(first_up_zmxy_at) OVER (PARTITION BY cnid)
                                            first_up_zmxy_at,
            MAX(latest_up_zmxy_at) OVER (PARTITION BY cnid)
                                            latest_up_zmxy_at,
            SUM(liaison_tot_cnt) OVER (PARTITION BY cnid)
                                            liaison_tot_cnt,
            MIN(liaison_first_up_created_at) OVER (PARTITION BY cnid)
                                            liaison_first_up_created_at,
            MAX(liaison_latest_up_created_at) OVER (PARTITION BY cnid)
                                            liaison_latest_up_created_at,
            ROW_NUMBER() OVER (PARTITION BY cnid ORDER BY created_at DESC, id DESC) rank_number
FROM        wp_calc.users
) t1 WHERE rank_number = 1
;
COMPUTE STATS wp_calc.cnid_users;

