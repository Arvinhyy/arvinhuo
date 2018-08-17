-- INVALIDATE METADATA;

DROP TABLE IF EXISTS wp_ods.addresses;
CREATE TABLE wp_ods.addresses (
    id INT,
    province INT,
    city INT,
    district INT,
    street STRING,
     `location` STRING,
    telephone STRING,
    addressable_id INT,
    addressable_type STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.areas;
CREATE TABLE wp_ods.areas (
    id INT,
    province_code STRING,
    province_name STRING,
    city_code STRING,
    city_level DECIMAL(4,1),
    city_name STRING,
    district_code STRING,
    district_name STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.attribute_types;
CREATE TABLE wp_ods.attribute_types (
    id INT,
    name STRING,
    enable BOOLEAN,
    description STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.bank_cards;
CREATE TABLE wp_ods.bank_cards (
    id INT,
    account_number STRING,
    province INT,
    city INT,
    user_id INT,
    state STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    bank_id INT,
    deleted_at TIMESTAMP,
    pay_code STRING,
    renew BOOLEAN,
    mobile STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.banks;
CREATE TABLE wp_ods.banks (
    id INT,
    name STRING,
    logo STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    pay_code STRING,
    code STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.companies;
CREATE TABLE wp_ods.companies (
    id INT,
    user_id INT,
    name STRING,
    department STRING,
    company_position STRING,
    entry_time STRING,
    departure_time STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    position_id INT,
    telephone STRING,
    salary_day INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.credit_applications;
CREATE TABLE wp_ods.credit_applications (
    id INT,
    application_id STRING,
    uuid BIGINT,
    state STRING,
    product_code STRING,
    origin STRING,
    applied_at TIMESTAMP,
    approved_at TIMESTAMP,
    apply_type STRING,
    amount DECIMAL(10,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    push_back_reason_codes STRING,
    sys_type INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.documents;
CREATE TABLE wp_ods.documents (
    id INT,
    documentable_id INT,
    documentable_type STRING,
    doc_type STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.due_settlements;
CREATE TABLE wp_ods.due_settlements (
    id INT,
    account_transaction_id INT,
    due_id INT,
    amount DECIMAL(10,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.dues;
CREATE TABLE wp_ods.dues (
    id INT,
    loan_id INT,
    `index` INT,
    amount DECIMAL(10,2),
    due_date TIMESTAMP,
    due_type STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    settled_amount DECIMAL(10,2)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.educations;
CREATE TABLE wp_ods.educations (
    id INT,
    user_id INT,
    school STRING,
    degree_id INT,
    enrolled_at TIMESTAMP,
    graduated_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.liaisons;
CREATE TABLE wp_ods.liaisons (
    id INT,
    user_id INT,
    name STRING,
    relationship STRING,
    mobile STRING,
    company STRING,
    `position` STRING,
    source_id INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.loan_applications;
CREATE TABLE wp_ods.loan_applications (
    id INT,
    borrower_id INT,
    tenor STRING,
    amount DECIMAL(10,2),
    state STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    application_id STRING,
    handling_fee DECIMAL(10,6),
    interest_rate DECIMAL(10,8),
    management_fee_rate DECIMAL(10,8),
    withdrawal_fee_rate DECIMAL(10,8),
    applied_at TIMESTAMP,
    approved_at TIMESTAMP,
    aip_by_id INT,
    reason_code1 STRING,
    reason_code2 STRING,
    reason_code3 STRING,
    reject_code STRING,
    picked_up_by_id INT,
    bank_card_id INT,
    applied_amount INT,
    applied_tenor STRING,
    push_backed_at TIMESTAMP,
    confirmed_at TIMESTAMP,
    origin STRING,
    user_evaluation_rank STRING,
    welab_product_id INT,
    aip_picked_up_by_id INT,
    pay_code STRING,
    total_rate DECIMAL(10,4),
    biz_type STRING,
    approval_type STRING,
    partner_code STRING,
    pre_picked_up_by_id INT,
    experiment_amount DECIMAL(10,2),
    experiment_info STRING,
    capital_pay_code STRING,
    sys_type INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.loans;
CREATE TABLE wp_ods.loans (
    id INT,
    loan_application_id INT,
    state STRING,
    disbursed_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    borrower_id INT,
    application_id STRING,
    overdue_penalty DECIMAL(10,2),
    closed_at TIMESTAMP,
    tenor STRING,
    amount DECIMAL(10,2),
    overdue_charge_version INT,
    pay_code STRING,
    writeoff INT,
    partner_code STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.profiles;
CREATE TABLE wp_ods.profiles (
    id INT,
    name STRING,
    borrower_id INT,
    cnid STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    verified BOOLEAN,
    position_id INT,
    marriage INT,
    qq STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.schools;
CREATE TABLE wp_ods.schools (
    id INT,
    name STRING,
    category1 STRING,
    province STRING,
    category2 STRING,
    locale STRING,
    city STRING,
    category3 STRING,
    is_college BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.quota_shunts;
CREATE TABLE wp_ods.quota_shunts (
    id INT,
    uuid BIGINT,
    created_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.user_attributes;
CREATE TABLE wp_ods.user_attributes (
    id INT,
    user_id INT,
    att_type_id INT,
    value STRING,
    verified_value STRING,
    state STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.user_industries;
CREATE TABLE wp_ods.user_industries (
    id INT,
    user_id INT,
    category_code INT,
    industry_code INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.user_quotas;
CREATE TABLE wp_ods.user_quotas (
    id INT,
    uuid BIGINT,
    category STRING,
    credit_line DECIMAL(10,2),
    freeze_state INT,
    state INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    origin STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.users;
CREATE TABLE wp_ods.users (
    id INT,
    mobile STRING,
    sign_in_count INT,
    current_sign_in_at TIMESTAMP,
    last_sign_in_at TIMESTAMP,
    current_sign_in_ip STRING,
    last_sign_in_ip STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    blocked BOOLEAN,
    user_agent STRING,
    origin STRING,
    agent BOOLEAN,
    referee_id INT,
    role_type INT,
    product_code STRING,
    uuid BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;

DROP TABLE IF EXISTS wp_ods.welab_products;
CREATE TABLE wp_ods.welab_products (
    id INT,
    name STRING,
    code STRING,
    priority INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    product_type INT,
    min_amount INT,
    max_amount INT,
    tenor STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
;
