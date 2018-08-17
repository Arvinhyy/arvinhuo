-- INVALIDATE METADATA;

DROP TABLE IF EXISTS wp_calc.loan_aspect;
CREATE TABLE wp_calc.loan_aspect (
  loan_id INT,
  tot_amt DECIMAL(38,2),
  tot_prin DECIMAL(38,2),
  tot_intr DECIMAL(38,2),
  tot_mgmt DECIMAL(38,2),
  tot_over DECIMAL(38,2),
  tot_other DECIMAL(38,2),
  due_amt DECIMAL(38,2),
  due_prin DECIMAL(38,2),
  due_intr DECIMAL(38,2),
  due_mgmt DECIMAL(38,2),
  due_over DECIMAL(38,2),
  due_other DECIMAL(38,2),
  set_amt DECIMAL(38,2),
  set_prin DECIMAL(38,2),
  set_intr DECIMAL(38,2),
  set_mgmt DECIMAL(38,2),
  set_over DECIMAL(38,2),
  set_other DECIMAL(38,2),
  min_due_date STRING,
  set_index INT,
  is_over TINYINT,
  over_day INT,
  is_over_his TINYINT,
  over_day_his INT,
  fpd_day_eval INT,
  fpd_day INT
) PARTITIONED BY (dateasp STRING) STORED AS PARQUET;
