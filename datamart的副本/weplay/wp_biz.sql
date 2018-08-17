-- INVALIDATE METADATA;

DROP TABLE IF EXISTS wp_biz.loan_applications;
CREATE TABLE wp_biz.loan_applications STORED AS PARQUET AS
SELECT      t1.*,
            t2.disb_cnt,
            t2.loan_list,
            t2.close_cnt,
            t2.sum_disb_amt,
            t2.min_disb_at,
            t2.max_disb_at,
            t2.sum_tot_prin,
            t2.sum_due_prin,
            t2.sum_set_prin,
            t2.sum_due_amt,
            t2.sum_set_amt,
            t2.min_min_due_date,
            t2.max_sche_close_date,
            t2.sum_is_over,
            t2.max_over_day,
            t2.sum_is_over_his,
            t2.max_fpd_day,
            COUNT(*) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        his_aply_cnt,
            COALESCE(SUM(is_aprv) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                                        his_aprv_cnt,
            COALESCE(SUM(is_disb) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                                        his_disb_cnt,
            COALESCE(SUM(is_close) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                                        his_close_cnt,
            COALESCE(SUM(is_rjct) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                                        his_rjct_cnt,
            LAST_VALUE(t1.application_id) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aply_application_id,
            LAST_VALUE(t1.state) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aply_state,
            LAST_VALUE(t1.welab_product_id) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aply_welab_product_id,
            LAST_VALUE(t1.origin) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aply_origin,
            LAST_VALUE(t1.applied_at) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aply_applied_at,
            LAST_VALUE(t1.approved_at) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aply_approved_at,
            LAST_VALUE(IF(t4.is_aprv = 1, t1.application_id, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aprv_application_id,
            LAST_VALUE(IF(t4.is_aprv = 1, t1.state, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aprv_state,
            LAST_VALUE(IF(t4.is_aprv = 1, t1.welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aprv_welab_product_id,
            LAST_VALUE(IF(t4.is_aprv = 1, t1.applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aprv_applied_at,
            LAST_VALUE(IF(t4.is_aprv = 1, t1.approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_aprv_approved_at,
            LAST_VALUE(IF(t4.is_disb = 1, t1.application_id, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_disb_application_id,
            LAST_VALUE(IF(t4.is_disb = 1, t1.state, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_disb_state,
            LAST_VALUE(IF(t4.is_disb = 1, t1.welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_disb_welab_product_id,
            LAST_VALUE(IF(t4.is_disb = 1, t1.applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_disb_applied_at,
            LAST_VALUE(IF(t4.is_disb = 1, t3.disbursed_at, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_disb_disbursed_at,
            LAST_VALUE(IF(t4.is_close = 1, t1.application_id, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_close_application_id,
            LAST_VALUE(IF(t4.is_close = 1, t1.state, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_close_state,
            LAST_VALUE(IF(t4.is_close = 1, t1.welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_close_welab_product_id,
            LAST_VALUE(IF(t4.is_close = 1, t1.applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_close_applied_at,
            LAST_VALUE(IF(t4.is_close = 1, t3.disbursed_at, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_close_disbursed_at,
            LAST_VALUE(IF(t4.is_close = 1, t3.closed_at, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_close_closed_at,
            LAST_VALUE(IF(t4.is_rjct = 1, t1.application_id, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_rjct_application_id,
            LAST_VALUE(IF(t4.is_rjct = 1, t1.state, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_rjct_state,
            LAST_VALUE(IF(t4.is_rjct = 1, t1.welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_rjct_welab_product_id,
            LAST_VALUE(IF(t4.is_rjct = 1, t1.applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_rjct_applied_at,
            LAST_VALUE(IF(t4.is_rjct = 1, t1.approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY t1.borrower_id ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        prev_rjct_approved_at,
            COUNT(*) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_his_aply_cnt,
            COALESCE(SUM(is_aprv) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                                        cnid_his_aprv_cnt,
            COALESCE(SUM(is_disb) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                                        cnid_his_disb_cnt,
            COALESCE(SUM(is_close) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                                        cnid_his_close_cnt,
            COALESCE(SUM(is_rjct) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                                        cnid_his_rjct_cnt,
            LAST_VALUE(t1.application_id) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aply_application_id,
            LAST_VALUE(t1.state) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aply_state,
            LAST_VALUE(t1.welab_product_id) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aply_welab_product_id,
            LAST_VALUE(t1.origin) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aply_origin,
            LAST_VALUE(t1.applied_at) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aply_applied_at,
            LAST_VALUE(t1.approved_at) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aply_approved_at,
            LAST_VALUE(IF(t4.is_aprv = 1, t1.application_id, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aprv_application_id,
            LAST_VALUE(IF(t4.is_aprv = 1, t1.state, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aprv_state,
            LAST_VALUE(IF(t4.is_aprv = 1, t1.welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aprv_welab_product_id,
            LAST_VALUE(IF(t4.is_aprv = 1, t1.applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aprv_applied_at,
            LAST_VALUE(IF(t4.is_aprv = 1, t1.approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_aprv_approved_at,
            LAST_VALUE(IF(t4.is_disb = 1, t1.application_id, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_disb_application_id,
            LAST_VALUE(IF(t4.is_disb = 1, t1.state, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_disb_state,
            LAST_VALUE(IF(t4.is_disb = 1, t1.welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_disb_welab_product_id,
            LAST_VALUE(IF(t4.is_disb = 1, t1.applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_disb_applied_at,
            LAST_VALUE(IF(t4.is_disb = 1, t3.disbursed_at, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_disb_disbursed_at,
            LAST_VALUE(IF(t4.is_close = 1, t1.application_id, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_close_application_id,
            LAST_VALUE(IF(t4.is_close = 1, t1.state, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_close_state,
            LAST_VALUE(IF(t4.is_close = 1, t1.welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_close_welab_product_id,
            LAST_VALUE(IF(t4.is_close = 1, t1.applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_close_applied_at,
            LAST_VALUE(IF(t4.is_close = 1, t3.disbursed_at, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_close_disbursed_at,
            LAST_VALUE(IF(t4.is_close = 1, t3.closed_at, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_close_closed_at,
            LAST_VALUE(IF(t4.is_rjct = 1, t1.application_id, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_rjct_application_id,
            LAST_VALUE(IF(t4.is_rjct = 1, t1.state, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_rjct_state,
            LAST_VALUE(IF(t4.is_rjct = 1, t1.welab_product_id, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_rjct_welab_product_id,
            LAST_VALUE(IF(t4.is_rjct = 1, t1.applied_at, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_rjct_applied_at,
            LAST_VALUE(IF(t4.is_rjct = 1, t1.approved_at, NULL) IGNORE NULLS) OVER (PARTITION BY t5.cnid ORDER BY t1.applied_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
                                        cnid_prev_rjct_approved_at,
            t3.id loan_id,
            t3.disbursed_at,
            t3.overdue_penalty,
            t3.closed_at,
            t3.overdue_charge_version,
            t3.pay_code,
            t3.writeoff,
            t3.tot_amt,
            t3.tot_prin,
            t3.tot_intr,
            t3.tot_mgmt,
            t3.tot_over,
            t3.tot_other,
            t3.due_amt,
            t3.due_prin,
            t3.due_intr,
            t3.due_mgmt,
            t3.due_over,
            t3.due_other,
            t3.set_amt,
            t3.set_prin,
            t3.set_intr,
            t3.set_mgmt,
            t3.set_over,
            t3.set_other,
            t3.min_due_date,
            t3.sche_close_date,
            t3.set_index,
            t3.is_over,
            t3.over_day,
            t3.is_over_his,
            t3.over_day_his,
            t3.fpd_day,
            t3.mob1_over_day,
            t3.mob2_over_day,
            t3.mob3_over_day,
            t3.mob4_over_day,
            t3.mob5_over_day,
            t3.mob6_over_day,
            t3.mob9_over_day,
            t3.mob12_over_day,
            t3.disb_his_aply_disb_cnt,
            t3.disb_his_disb_cnt,
            t3.disb_prev_disb_application_id,
            t3.disb_prev_disb_state,
            t3.disb_prev_disb_welab_product_id,
            t3.disb_prev_disb_disb_origin,
            t3.disb_prev_disb_disbursed_at,
            t3.cnid_disb_his_disb_cnt,
            t3.cnid_disb_prev_disb_application_id,
            t3.cnid_disb_prev_disb_state,
            t3.cnid_disb_prev_disb_welab_product_id,
            t3.cnid_disb_prev_disb_disb_origin,
            t3.cnid_disb_prev_disb_disbursed_at
FROM        wp_calc.loan_applications           t1
LEFT JOIN   wp_calc.loan_application_disburse   t2 ON t1.application_id = t2.application_id
LEFT JOIN   wp_calc.loans                       t3 ON t1.application_id = t3.application_id
LEFT JOIN   wp_calc.loan_application_state      t4 ON COALESCE(t3.state, t1.state) = t4.code
LEFT JOIN   wp_calc.users                       t5 ON t1.borrower_id = t5.id
WHERE       t1.biz_type IN ('normal', 'credit_application')
;
COMPUTE STATS wp_biz.loan_applications;

DROP TABLE IF EXISTS wp_biz.loans;
CREATE TABLE wp_biz.loans STORED AS PARQUET AS
SELECT      t1.*,
            t2.source,
            t2.handling_fee,
            t2.interest_rate,
            t2.management_fee_rate,
            t2.withdrawal_fee_rate,
            t2.applied_at,
            t2.approved_at,
            t2.reason_code1,
            t2.reason_code2,
            t2.reason_code3,
            t2.reject_code,
            t2.aip_by_id,
            t2.picked_up_by_id,
            t2.aip_picked_up_by_id,
            t2.user_evaluation_rank,
            t2.applied_amount,
            t2.applied_tenor,
            t2.push_backed_at,
            t2.push_back_reason_codes,
            t2.confirmed_at,
            t2.origin,
            t2.product_code,
            t2.biz_type,
            t2.approval_type,
            t2.experiment_amount,
            t2.experiment_info,
            t2.capital_pay_code,
            t2.sys_type,
            t2.step,
            t2.level,
            t2.pricing_level,
            t2.reason_code,
            t2.approval_state,
            t2.approval_type_mongo,
            t2.approval_note,
            t2.approved_at_mongo,
            t2.source_id,
            t2.call_count,
            t2.product_code_mongo,
            t2.product_name,
            t2.apply_type,
            t2.platform,
            t2.product,
            t2.device_id,
            t2.log_time,
            t2.proposer_mobile,
            t2.proposer_name,
            t2.proposer_cnid,
            t2.proposer_register_time,
            t2.proposer_company_income,
            t2.proposer_marriage,
            t2.proposer_industry,
            t2.proposer_credit_line,
            t2.proposer_available_credit_line,
            t2.proposer_qq,
            t2.proposer_age,
            t2.proposer_resident_description,
            t2.proposer_address_province,
            t2.proposer_address_city,
            t2.proposer_address_district,
            t2.proposer_address_location,
            t2.proposer_address_description,
            t2.device_info_app_version,
            t2.device_info_gps,
            t2.device_info_ip,
            t2.device_info_mac,
            t2.device_info_model,
            t2.device_info_os_version,
            t2.device_info_source_id,
            t2.device_info_uuid,
            t2.device_info_wdid,
            t2.company_name,
            t2.company_telephone,
            t2.company_entry_time,
            t2.company_department,
            t2.company_position_id,
            t2.company_position,
            t2.company_created_at,
            t2.company_updated_at,
            t2.company_address_province,
            t2.company_address_city,
            t2.company_address_district,
            t2.company_address_street,
            t2.company_address_description,
            t2.company_address_telephone,
            t2.company_address_created_at,
            t2.company_address_updated_at,
            t2.educations_cnt,
            t2.educations_degree_list,
            t2.data_educations_school_list,
            t2.documents_cnt,
            t2.documents_doc_type_list,
            t2.liaisons_cnt,
            t2.liaisons_relationship_list,
            t2.liaisons_name_list,
            t2.liaisons_mobile_list,
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
            t2.final_approval_updated_at,
            t2.disb_cnt,
            t2.loan_list,
            t2.close_cnt,
            t2.sum_disb_amt,
            t2.min_disb_at,
            t2.max_disb_at,
            t2.sum_tot_prin,
            t2.sum_due_prin,
            t2.sum_set_prin,
            t2.sum_due_amt,
            t2.sum_set_amt,
            t2.min_min_due_date,
            t2.max_sche_close_date,
            t2.sum_is_over,
            t2.max_over_day,
            t2.sum_is_over_his,
            t2.max_fpd_day,
            t2.his_aply_cnt,
            t2.his_aprv_cnt,
            t2.his_disb_cnt,
            t2.his_close_cnt,
            t2.his_rjct_cnt,
            t2.prev_aply_application_id,
            t2.prev_aply_state,
            t2.prev_aply_welab_product_id,
            t2.prev_aply_origin,
            t2.prev_aply_applied_at,
            t2.prev_aply_approved_at,
            t2.prev_aprv_application_id,
            t2.prev_aprv_state,
            t2.prev_aprv_welab_product_id,
            t2.prev_aprv_applied_at,
            t2.prev_aprv_approved_at,
            t2.prev_disb_application_id,
            t2.prev_disb_state,
            t2.prev_disb_welab_product_id,
            t2.prev_disb_applied_at,
            t2.prev_disb_disbursed_at,
            t2.prev_close_application_id,
            t2.prev_close_state,
            t2.prev_close_welab_product_id,
            t2.prev_close_applied_at,
            t2.prev_close_disbursed_at,
            t2.prev_close_closed_at,
            t2.prev_rjct_application_id,
            t2.prev_rjct_state,
            t2.prev_rjct_welab_product_id,
            t2.prev_rjct_applied_at,
            t2.prev_rjct_approved_at,
            t2.cnid_his_aply_cnt,
            t2.cnid_his_aprv_cnt,
            t2.cnid_his_disb_cnt,
            t2.cnid_his_close_cnt,
            t2.cnid_his_rjct_cnt,
            t2.cnid_prev_aply_application_id,
            t2.cnid_prev_aply_state,
            t2.cnid_prev_aply_welab_product_id,
            t2.cnid_prev_aply_origin,
            t2.cnid_prev_aply_applied_at,
            t2.cnid_prev_aply_approved_at,
            t2.cnid_prev_aprv_application_id,
            t2.cnid_prev_aprv_state,
            t2.cnid_prev_aprv_welab_product_id,
            t2.cnid_prev_aprv_applied_at,
            t2.cnid_prev_aprv_approved_at,
            t2.cnid_prev_disb_application_id,
            t2.cnid_prev_disb_state,
            t2.cnid_prev_disb_welab_product_id,
            t2.cnid_prev_disb_applied_at,
            t2.cnid_prev_disb_disbursed_at,
            t2.cnid_prev_close_application_id,
            t2.cnid_prev_close_state,
            t2.cnid_prev_close_welab_product_id,
            t2.cnid_prev_close_applied_at,
            t2.cnid_prev_close_disbursed_at,
            t2.cnid_prev_close_closed_at,
            t2.cnid_prev_rjct_application_id,
            t2.cnid_prev_rjct_state,
            t2.cnid_prev_rjct_welab_product_id,
            t2.cnid_prev_rjct_applied_at,
            t2.cnid_prev_rjct_approved_at
FROM        wp_calc.loans               t1
LEFT JOIN   wp_biz.loan_applications    t2 ON t1.map_application_id = t2.application_id
;
COMPUTE STATS wp_biz.loans;


DROP TABLE IF EXISTS wp_biz.users;
CREATE TABLE wp_biz.users STORED AS PARQUET AS
SELECT * FROM wp_calc.users
;
COMPUTE STATS wp_biz.users;

DROP TABLE IF EXISTS wp_biz.cnid_users;
CREATE TABLE wp_biz.cnid_users STORED AS PARQUET AS
SELECT      t1.*,
            t2.aply_cnt,
            t2.aprv_cnt,
            t2.rjct_cnt,
            t2.first_aply_application_id,
            t2.first_aply_applied_at,
            t2.first_aply_state,
            t2.first_aply_welab_product_id,
            t2.first_aply_origin,
            t2.first_aprv_application_id,
            t2.first_aprv_applied_at,
            t2.first_aprv_approved_at,
            t2.first_aprv_state,
            t2.first_aprv_welab_product_id,
            t2.first_aprv_origin,
            t2.first_rjct_application_id,
            t2.first_rjct_applied_at,
            t2.first_rjct_approved_at,
            t2.first_rjct_state,
            t2.first_rjct_welab_product_id,
            t2.first_rjct_origin,
            t2.latest_aply_application_id,
            t2.latest_aply_applied_at,
            t2.latest_aply_state,
            t2.latest_aply_welab_product_id,
            t2.latest_aply_origin,
            t2.latest_aprv_application_id,
            t2.latest_aprv_applied_at,
            t2.latest_aprv_approved_at,
            t2.latest_aprv_state,
            t2.latest_aprv_welab_product_id,
            t2.latest_aprv_origin,
            t2.latest_rjct_application_id,
            t2.latest_rjct_applied_at,
            t2.latest_rjct_approved_at,
            t2.latest_rjct_state,
            t2.latest_rjct_welab_product_id,
            t2.latest_rjct_origin,
            t3.disb_cnt,
            t3.disb_amt,
            t3.sum_tot_amt,
            t3.sum_tot_prin,
            t3.sum_tot_intr,
            t3.sum_tot_mgmt,
            t3.sum_tot_over,
            t3.sum_tot_other,
            t3.sum_due_amt,
            t3.sum_due_prin,
            t3.sum_due_intr,
            t3.sum_due_mgmt,
            t3.sum_due_over,
            t3.sum_due_other,
            t3.sum_set_amt,
            t3.sum_set_prin,
            t3.sum_set_intr,
            t3.sum_set_mgmt,
            t3.sum_set_over,
            t3.sum_set_other,
            t3.min_min_due_date,
            t3.max_sche_close_date,
            t3.sum_is_close,
            t3.sum_is_early,
            t3.sum_is_over,
            t3.max_over_day,
            t3.sum_is_over_his,
            t3.max_over_day_his,
            t3.first_disb_application_id,
            t3.first_disb_state,
            t3.first_disb_disbursed_at,
            t3.first_disb_amount,
            t3.first_disb_welab_product_id,
            t3.first_disb_withdraw_origin,
            t3.first_disb_over_day,
            t3.latest_disb_application_id,
            t3.latest_disb_state,
            t3.latest_disb_disbursed_at,
            t3.latest_disb_amount,
            t3.latest_disb_welab_product_id,
            t3.latest_disb_withdraw_origin,
            t3.latest_disb_over_day,
            t3.latest_disb_sche_close_date
FROM        wp_calc.cnid_users              t1
LEFT JOIN   wp_calc.cnid_loan_application   t2 ON t1.cnid = t2.cnid
LEFT JOIN   wp_calc.cnid_loan               t3 ON t1.cnid = t3.cnid
;
COMPUTE STATS wp_biz.cnid_users;

DROP TABLE IF EXISTS wp_biz.welab_products;
CREATE TABLE wp_biz.welab_products STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.welab_products;
COMPUTE STATS wp_biz.welab_products;

DROP TABLE IF EXISTS wp_biz.rules_engine_item;
CREATE TABLE wp_biz.rules_engine_item STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.rules_engine_item;
COMPUTE STATS wp_biz.rules_engine_item;

DROP TABLE IF EXISTS wp_biz.merged_data;
CREATE TABLE wp_biz.merged_data STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.merged_data;
COMPUTE STATS wp_biz.merged_data;

DROP TABLE IF EXISTS wp_biz.rules_cache;
CREATE TABLE wp_biz.rules_cache STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.rules_cache;
COMPUTE STATS wp_biz.rules_cache;

DROP TABLE IF EXISTS wp_biz.rules_cache_hit_rules;
CREATE TABLE wp_biz.rules_cache_hit_rules STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.rules_cache_hit_rules;
COMPUTE STATS wp_biz.rules_cache_hit_rules;

DROP TABLE IF EXISTS wp_biz.rules_cache_extra_info_fm_info_risk_service_hit_rules;
CREATE TABLE wp_biz.rules_cache_extra_info_fm_info_risk_service_hit_rules STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.rules_cache_extra_info_fm_info_risk_service_hit_rules;
COMPUTE STATS wp_biz.rules_cache_extra_info_fm_info_risk_service_hit_rules;

DROP TABLE IF EXISTS wp_biz.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules;
CREATE TABLE wp_biz.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules;
COMPUTE STATS wp_biz.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules;

-- -------------------------------------------------------------------------
-- -------------------------------------------------------------------------

DROP TABLE IF EXISTS weplay.rules_engine_item;
CREATE TABLE weplay.rules_engine_item STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.rules_engine_item;
COMPUTE STATS weplay.rules_engine_item;

DROP TABLE IF EXISTS weplay.merged_data;
CREATE TABLE weplay.merged_data STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.merged_data;
COMPUTE STATS weplay.merged_data;

DROP TABLE IF EXISTS weplay.rules_cache;
CREATE TABLE weplay.rules_cache STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.rules_cache;
COMPUTE STATS weplay.rules_cache;

DROP TABLE IF EXISTS weplay.rules_cache_hit_rules;
CREATE TABLE weplay.rules_cache_hit_rules STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.rules_cache_hit_rules;
COMPUTE STATS weplay.rules_cache_hit_rules;

DROP TABLE IF EXISTS weplay.rules_cache_extra_info_fm_info_risk_service_hit_rules;
CREATE TABLE weplay.rules_cache_extra_info_fm_info_risk_service_hit_rules STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.rules_cache_extra_info_fm_info_risk_service_hit_rules;
COMPUTE STATS weplay.rules_cache_extra_info_fm_info_risk_service_hit_rules;

DROP TABLE IF EXISTS weplay.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules;
CREATE TABLE weplay.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules STORED AS PARQUET AS
SELECT      *
FROM        wp_calc.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules;
COMPUTE STATS weplay.rules_cache_extra_info_fm_info_risk_service_policy_set_hit_rules;


