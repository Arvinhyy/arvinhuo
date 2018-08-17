-- 这是一个循环对模版

-- due_settlements汇总
DROP TABLE IF EXISTS wp_tmp.loan_aspect_set_group;
CREATE TABLE wp_tmp.loan_aspect_set_group STORED AS PARQUET AS
SELECT      due_id              due_id,
            SUM(CASE WHEN TO_DATE(created_at) <= '2017-12-31' THEN amount ELSE 0 END)
                                amount,
            MAX(CASE WHEN TO_DATE(created_at) <= '2017-12-31' THEN created_at ELSE NULL END)
                                set_at,
            SUM(amount)         amount_delay,
            MAX(created_at)     set_at_delay
FROM        wp_std.due_settlements
WHERE       TO_DATE(created_at) <= DATE_ADD('2017-12-31', 3)
GROUP BY    due_id
;
COMPUTE STATS wp_tmp.loan_aspect_set_group;

-- loan应还期数
DROP TABLE IF EXISTS wp_tmp.loan_aspect_loan_main;
CREATE TABLE wp_tmp.loan_aspect_loan_main STORED AS PARQUET AS
SELECT      t1.id,
            MAX(CASE WHEN TO_DATE(t2.due_date) <= '2017-12-31' THEN t2.index ELSE 0 END) due_index
FROM        wp_std.loans    t1
LEFT JOIN   wp_std.dues     t2 ON t1.id = t2.loan_id AND t2.due_type = 'principal'
WHERE       TO_DATE(t1.disbursed_at) <= '2017-12-31'
GROUP BY    t1.id
;
COMPUTE STATS wp_tmp.loan_aspect_loan_main;

-- 贷款分期明细
DROP TABLE IF EXISTS wp_tmp.loan_aspect_loan_index_group;
CREATE TABLE wp_tmp.loan_aspect_loan_index_group STORED AS PARQUET AS
SELECT      t1.id,
            t2.index,
            SUM(t2.amount)              tot_amt,
            SUM(CASE WHEN t2.due_type = 'principal' THEN t2.amount
                     ELSE 0
                END)                    tot_prin,
            SUM(CASE WHEN t2.due_type = 'interest' THEN t2.amount
                     ELSE 0
                END)                    tot_intr,
            SUM(CASE WHEN t2.due_type = 'management_fee' THEN t2.amount
                     ELSE 0
                END)                    tot_mgmt,
            SUM(CASE WHEN t2.due_type LIKE 'overdue%' THEN t2.amount
                     ELSE 0
                END)                    tot_over,
            SUM(CASE WHEN t2.due_type NOT IN ('principal', 'interest', 'management_fee') AND t2.due_type NOT LIKE 'overdue%' THEN t2.amount
                     ELSE 0
                END)                    tot_other,
            MIN(CASE WHEN t2.due_type = 'principal' THEN TO_DATE(t2.due_date)
                     ELSE NULL
                END)                    due_date,
            SUM(CASE WHEN t2.index <= t1.due_index THEN t2.amount
                     ELSE 0
                END)                    due_amt,
            SUM(CASE WHEN t2.index <= t1.due_index AND t2.due_type = 'principal' THEN t2.amount
                     ELSE 0
                END)                    due_prin,
            SUM(CASE WHEN t2.index <= t1.due_index AND t2.due_type = 'interest' THEN t2.amount
                     ELSE 0
                END)                    due_intr,
            SUM(CASE WHEN t2.index <= t1.due_index AND t2.due_type = 'management_fee' THEN t2.amount
                     ELSE 0
                END)                    due_mgmt,
            SUM(CASE WHEN t2.index <= t1.due_index AND t2.due_type LIKE 'overdue%' THEN t2.amount
                     ELSE 0
                END)                    due_over,
            SUM(CASE WHEN t2.index <= t1.due_index AND t2.due_type NOT IN ('principal', 'interest', 'management_fee')
                                                   AND t2.due_type NOT LIKE 'overdue%' THEN t2.amount
                     ELSE 0
                END)                    due_other,
            MAX(t3.set_at)              set_at,
            SUM(COALESCE(t3.amount, 0))
                                        set_amt,
            SUM(CASE WHEN t2.due_type = 'principal' THEN COALESCE(t3.amount, 0)
                     ELSE 0
                END)                    set_prin,
            SUM(CASE WHEN t2.due_type = 'interest' THEN COALESCE(t3.amount, 0)
                     ELSE 0
                END)                    set_intr,
            SUM(CASE WHEN t2.due_type = 'management_fee' THEN COALESCE(t3.amount, 0)
                     ELSE 0
                END)                    set_mgmt,
            SUM(CASE WHEN t2.due_type LIKE 'overdue%' THEN COALESCE(t3.amount, 0)
                     ELSE 0
                END)                    set_over,
            SUM(CASE WHEN t2.due_type NOT IN ('principal', 'interest', 'management_fee') AND t2.due_type NOT LIKE 'overdue%' THEN COALESCE(t3.amount, 0)
                     ELSE 0
                END)                    set_other,
            MAX(t3.set_at_delay)        set_at_delay,
            SUM(CASE WHEN t2.due_type = 'principal' THEN COALESCE(t3.amount_delay, 0)
                     ELSE 0
                END)                    set_prin_delay
FROM        wp_tmp.loan_aspect_loan_main    t1
LEFT JOIN   wp_std.dues                     t2 ON t1.id = t2.loan_id
LEFT JOIN   wp_tmp.loan_aspect_set_group    t3 ON t2.id = t3.due_id
GROUP BY    t1.id,
            t2.index
;
COMPUTE STATS wp_tmp.loan_aspect_loan_index_group;

DROP TABLE IF EXISTS wp_tmp.loan_aspect_loan_index_result;
CREATE TABLE wp_tmp.loan_aspect_loan_index_result STORED AS PARQUET AS
SELECT      id,
            index,
            tot_amt,
            tot_prin,
            tot_intr,
            tot_mgmt,
            tot_over,
            tot_other,
            due_date,
            due_amt,
            due_prin,
            due_intr,
            due_mgmt,
            due_over,
            due_other,
            set_at,
            set_amt,
            set_prin,
            set_intr,
            set_mgmt,
            set_over,
            set_other,
            set_at_delay,
            set_prin_delay,
            CASE WHEN tot_prin <= set_prin THEN 1
                 ELSE 0
            END                 is_close,
            CASE WHEN due_prin > set_prin THEN 1
                 ELSE 0
            END                 is_over,
            CASE WHEN due_prin > set_prin THEN DATEDIFF(DATE_ADD('2017-12-31', 1), due_date)
                 ELSE 0
            END                 over_day,
            CASE WHEN due_amt > 0 AND set_at IS NULL THEN 1
                 WHEN due_amt > 0 AND due_date < TO_DATE(set_at) THEN 1
                 ELSE 0
            END                 is_over_his,
            CASE WHEN due_prin > set_prin THEN DATEDIFF(DATE_ADD('2017-12-31', 1), due_date)
                 ELSE greatest(0, DATEDIFF(COALESCE(set_at, DATE_ADD('2017-12-31', 1)), due_date))
            END                 over_day_his,
            CASE WHEN due_prin > set_prin_delay THEN DATEDIFF(DATE_ADD('2017-12-31', 1+3), due_date)
                 ELSE GREATEST(0, DATEDIFF(COALESCE(set_at_delay, DATE_ADD('2017-12-31', 1+3)), due_date))
            END                 over_day_his_delay
FROM        wp_tmp.loan_aspect_loan_index_group
;
COMPUTE STATS wp_tmp.loan_aspect_loan_index_result;

INSERT OVERWRITE TABLE wp_calc.loan_aspect PARTITION (dateasp = '2017-12-31') 
SELECT      id                          loan_id,
            SUM(tot_amt)                tot_amt,
            SUM(tot_prin)               tot_prin,
            SUM(tot_intr)               tot_intr,
            SUM(tot_mgmt)               tot_mgmt,
            SUM(tot_over)               tot_over,
            SUM(tot_other)              tot_other,
            SUM(due_amt)                due_amt,
            SUM(due_prin)               due_prin,
            SUM(due_intr)               due_intr,
            SUM(due_mgmt)               due_mgmt,
            SUM(due_over)               due_over,
            SUM(due_other)              due_other,
            SUM(set_amt)                set_amt,
            SUM(set_prin)               set_prin,
            SUM(set_intr)               set_intr,
            SUM(set_mgmt)               set_mgmt,
            SUM(set_over)               set_over,
            SUM(set_other)              set_other,
            MIN(CASE WHEN is_close = 0 THEN due_date
                     ELSE NULL
                END)                    min_due_date,
            MIN(CASE WHEN is_close = 0 THEN INDEX
                     ELSE NULL
                END)                    set_index,
            MAX(is_over)                is_over,
            MAX(over_day)               over_day,
            MAX(is_over_his)            is_over_his,
            MAX(over_day_his)           over_day_his,
            MAX(CASE WHEN INDEX = 1 THEN over_day_his
                     ELSE 0
                END)                    fpd_day_eval,
            MAX(CASE WHEN INDEX = 1 THEN over_day_his_delay
                     ELSE 0
                END)                    fpd_day
FROM        wp_tmp.loan_aspect_loan_index_result
GROUP BY    id
;

COMPUTE INCREMENTAL STATS wp_calc.loan_aspect PARTITION  (dateasp = '2017-12-31');
