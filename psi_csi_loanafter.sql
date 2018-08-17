-- Kaka Yao  DataMining
-- 评分卡监控

-- 按天明细监控安卓PSI指标

testtest
DROP TABLE IF EXISTS  wp_rpt.psi_supervisory_android ;

CREATE TABLE  wp_rpt.psi_supervisory_android 
AS
select 
t1.biz_date                                                                            as  biz_date ,
t1.platform_model                                                                      as  platform_model ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.sn                                                                                  as  s_sn           ,
t1.na                                                                                  as  s_na           ,
t1.`unknown`                                                                           as  s_unknown      ,
t1.sum_s                                                                               as  sum_s          ,
t2.wek_day                                                                             as  wek_day        ,
t2.month_day                                                                           as  month_day      ,
t2.week_day                                                                            as  week_day       ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,

(t1.ss1/all_sum-t3.s1_proportion) * ln((t1.ss1/all_sum)/t3.s1_proportion)              as  s1_psi         ,
(t1.ss2/all_sum-t3.s2_proportion) * ln((t1.ss2/all_sum)/t3.s2_proportion)              as  s2_psi         ,
(t1.ss3/all_sum-t3.s3_proportion) * ln((t1.ss3/all_sum)/t3.s3_proportion)              as  s3_psi         ,
(t1.ss4/all_sum-t3.s4_proportion) * ln((t1.ss4/all_sum)/t3.s4_proportion)              as  s4_psi         ,
(t1.ss5/all_sum-t3.s5_proportion) * ln((t1.ss5/all_sum)/t3.s5_proportion)              as  s5_psi         ,

(t1.ss1/all_sum-t3.s1_proportion) * ln((t1.ss1/all_sum)/t3.s1_proportion) 
+(t1.ss2/all_sum-t3.s2_proportion) * ln((t1.ss2/all_sum)/t3.s2_proportion)  
+(t1.ss3/all_sum-t3.s3_proportion) * ln((t1.ss3/all_sum)/t3.s3_proportion) 
+(t1.ss4/all_sum-t3.s4_proportion) * ln((t1.ss4/all_sum)/t3.s4_proportion) 
+(t1.ss5/all_sum-t3.s5_proportion) * ln((t1.ss5/all_sum)/t3.s5_proportion)             as  s_psi


from
(
select 
to_date(t1.application_date)                                                                                as  biz_date   	    ,
'android'                                                                                                   as  platform_model  ,
count(case when t1.score_card_class = 'S1' then t1.loan_id end)                                             as  s1              ,
count(case when t1.score_card_class = 'S2' then t1.loan_id end)                                             as  s2              ,
count(case when t1.score_card_class = 'S3' then t1.loan_id end)                                             as  s3              ,
count(case when t1.score_card_class = 'S4' then t1.loan_id end)                                             as  s4              ,
count(case when t1.score_card_class = 'S5' then t1.loan_id end)                                             as  s5              ,
count(case when t1.score_card_class = 'NA' then t1.loan_id end)                                             as  na              ,
count(case when t1.score_card_class = 'unknown' then t1.loan_id end)                                        as  unknown     	,
count(case when t1.score_card_class = 'SN' then t1.loan_id end)                                             as  sn              ,
count(case when t1.score_card_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)                      as  sum_s           ,
count(case when t1.score_card_class = 'S1'  then t1.loan_id end)/count(case when t1.score_card_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)       as  s1_proportion ,
count(case when t1.score_card_class = 'S2'  then t1.loan_id end)/count(case when t1.score_card_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)       as  s2_proportion ,
count(case when t1.score_card_class = 'S3'  then t1.loan_id end)/count(case when t1.score_card_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)       as  s3_proportion ,
count(case when t1.score_card_class = 'S4'  then t1.loan_id end)/count(case when t1.score_card_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)       as  s4_proportion ,
count(case when t1.score_card_class = 'S5'  then t1.loan_id end)/count(case when t1.score_card_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)       as  s5_proportion ,
COUNT(CASE WHEN t1.score_card_class IN ('S1','S2','S3','S4','S5') THEN t1.loan_id END)  +
      CAST(COUNT(CASE WHEN t1.score_card_class = 'S1' THEN t1.loan_id END) = 0 as INT)  +    
      CAST(COUNT(CASE WHEN t1.score_card_class = 'S2' THEN t1.loan_id END) = 0 as INT)  +
      CAST(COUNT(CASE WHEN t1.score_card_class = 'S3' THEN t1.loan_id END) = 0 as INT)  +
      CAST(COUNT(CASE WHEN t1.score_card_class = 'S4' THEN t1.loan_id END) = 0 as INT)  +
	  CAST(COUNT(CASE WHEN t1.score_card_class = 'S5' THEN t1.loan_id END) = 0 as INT)                                                            		      as  all_sum       ,
IF(COUNT(CASE WHEN t1.score_card_class = 'S1' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.score_card_class = 'S1' THEN t1.loan_id END))   				  as  ss1           ,
IF(COUNT(CASE WHEN t1.score_card_class = 'S2' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.score_card_class = 'S2' THEN t1.loan_id END))   				  as  ss2           ,
IF(COUNT(CASE WHEN t1.score_card_class = 'S3' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.score_card_class = 'S3' THEN t1.loan_id END))   				  as  ss3           ,
IF(COUNT(CASE WHEN t1.score_card_class = 'S4' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.score_card_class = 'S4' THEN t1.loan_id END))   				  as  ss4           ,
IF(COUNT(CASE WHEN t1.score_card_class = 'S5' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.score_card_class = 'S5' THEN t1.loan_id END))   				  as  ss5 

from  weplay.rules_engine_item  t1 
left join weplay.merged_data t2 ON t1.loan_id = t2.loan_id
where to_date(t1.application_date) >= to_date(CURRENT_TIMESTAMP() - interval 60 day) 
and  to_date(t1.application_date) < to_date(CURRENT_TIMESTAMP())  
and  t2.score_card_version = '3.3'
and  t1.score_card_class in ('S1','S2','S3','S4','S5','SN','unknown','NA')
group by to_date(t1.application_date)
)  t1
left join wp_rpt.weekdate_model  t2   on  to_date(t1.biz_date) = to_date(t2.date_day)
left join (select * from  wp_rpt.benchmark_value where platform = 'android-3') t3  on 1=1
where  t1.biz_date >= '2018-02-07'
;    


-- 按周明细监控安卓PSI指标

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_android_week ;

CREATE  TABLE wp_rpt.psi_supervisory_android_week
AS
select 
t1.week_date                                                                           as  week_date        ,
t1.platform_model                                                                      as  platform_model   ,
t1.s1                                                                                  as  s1               ,
t1.s2                                                                                  as  s2               ,
t1.s3                                                                                  as  s3               ,
t1.s4                                                                                  as  s4               ,
t1.s5                                                                                  as  s5               ,
t1.sn                                                                                  as  s_sn             ,
t1.na                                                                                  as  s_na             ,
t1.unknown                                                                             as  s_unknown        ,
t1.sum_s                                                                               as  sum_s            ,
t1.s1_proportion                                                                       as  s1_proportion    ,
t1.s2_proportion                                                                       as  s2_proportion    ,
t1.s3_proportion                                                                       as  s3_proportion    ,
t1.s4_proportion                                                                       as  s4_proportion    ,
t1.s5_proportion                                                                       as  s5_proportion    ,
(t1.ss1/t1.all_sum-t3.s1_proportion)*ln((t1.ss1/t1.all_sum)/t3.s1_proportion)          as  s1_psi           ,
(t1.ss2/t1.all_sum-t3.s2_proportion)*ln((t1.ss2/t1.all_sum)/t3.s2_proportion)          as  s2_psi           ,
(t1.ss3/t1.all_sum-t3.s3_proportion)*ln((t1.ss3/t1.all_sum)/t3.s3_proportion)          as  s3_psi       	,
(t1.ss4/t1.all_sum-t3.s4_proportion)*ln((t1.ss4/t1.all_sum)/t3.s4_proportion)          as  s4_psi       	,
(t1.ss5/t1.all_sum-t3.s5_proportion)*ln((t1.ss5/t1.all_sum)/t3.s5_proportion)          as  s5_psi       	,

(t1.ss1/t1.all_sum-t3.s1_proportion)*ln((t1.ss1/t1.all_sum)/t3.s1_proportion)
+(t1.ss2/t1.all_sum-t3.s2_proportion)*ln((t1.ss2/t1.all_sum)/t3.s2_proportion)
+(t1.ss3/t1.all_sum-t3.s3_proportion)*ln((t1.ss3/t1.all_sum)/t3.s3_proportion) 
+(t1.ss4/t1.all_sum-t3.s4_proportion)*ln((t1.ss4/t1.all_sum)/t3.s4_proportion)
+(t1.ss5/t1.all_sum-t3.s5_proportion)*ln((t1.ss5/t1.all_sum)/t3.s5_proportion)             as  s_psi

from
(
select 
t1.week_day                                                           as  week_date,
t1.platform_model                                                     as  platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                          as  na             ,
sum(t1.s_unknown)                                                     as  unknown        ,
sum(t1.s_sn)                                                          as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  ,

sum(t1.s1) + sum(t1.s2) + sum(t1.s3) + sum(t1.s4) + sum(t1.s5) +
    CAST(sum(t1.s1) = 0 as INT)  +    
    CAST(sum(t1.s2) = 0 as INT)  +
    CAST(sum(t1.s3) = 0 as INT)  +
    CAST(sum(t1.s4) = 0 as INT)  +
	CAST(sum(t1.s5) = 0 as INT)                                       as  all_sum        ,
IF(sum(t1.s1) = 0,1,sum(t1.s1))   				  					  as  ss1            ,
IF(sum(t1.s2) = 0,1,sum(t1.s2))   				  					  as  ss2            ,
IF(sum(t1.s3) = 0,1,sum(t1.s3))   				  					  as  ss3            ,
IF(sum(t1.s4) = 0,1,sum(t1.s4))   				  					  as  ss4            ,
IF(sum(t1.s5) = 0,1,sum(t1.s5))   				  					  as  ss5            

from wp_rpt.psi_supervisory_android t1
group by t1.week_day , t1.platform_model
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'android-3') t3  on 1=1
;


-- 按月明细监控安卓PSI指标

DROP TABLE IF EXISTS  wp_rpt.psi_supervisory_androi_month;

CREATE  TABLE wp_rpt.psi_supervisory_androi_month
AS
select 
t1.month_date                                                                          as  month_date        ,
t1.platform_model                                                                      as  platform_model    ,
t1.s1                                                                                  as  s1             	 ,
t1.s2                                                                                  as  s2             	 ,
t1.s3                                                                                  as  s3             	 ,
t1.s4                                                                                  as  s4             	 ,
t1.s5                                                                                  as  s5              	 ,
t1.sn                                                                                  as  s_sn              ,
t1.na                                                                                  as  s_na              ,
t1.`unknown`                                                                           as  s_unknown         ,
t1.sum_s                                                                               as  sum_s          	 ,
t1.s1_proportion                                                                       as  s1_proportion  	 ,
t1.s2_proportion                                                                       as  s2_proportion  	 ,
t1.s3_proportion                                                                       as  s3_proportion  	 ,
t1.s4_proportion                                                                       as  s4_proportion  	 ,
t1.s5_proportion                                                                       as  s5_proportion  	 ,
(t1.ss1/t1.all_sum-t3.s1_proportion)*ln((t1.ss1/t1.all_sum)/t3.s1_proportion)          as  s1_psi         	 ,
(t1.ss2/t1.all_sum-t3.s2_proportion)*ln((t1.ss2/t1.all_sum)/t3.s2_proportion)          as  s2_psi         	 ,
(t1.ss3/t1.all_sum-t3.s3_proportion)*ln((t1.ss3/t1.all_sum)/t3.s3_proportion)          as  s3_psi         	 ,
(t1.ss4/t1.all_sum-t3.s4_proportion)*ln((t1.ss4/t1.all_sum)/t3.s4_proportion)          as  s4_psi         	 ,
(t1.ss5/t1.all_sum-t3.s5_proportion)*ln((t1.ss5/t1.all_sum)/t3.s5_proportion)          as  s5_psi         	 ,

(t1.ss1/t1.all_sum-t3.s1_proportion)*ln((t1.ss1/t1.all_sum)/t3.s1_proportion)
+(t1.ss2/t1.all_sum-t3.s2_proportion)*ln((t1.ss2/t1.all_sum)/t3.s2_proportion)
+(t1.ss3/t1.all_sum-t3.s3_proportion)*ln((t1.ss3/t1.all_sum)/t3.s3_proportion) 
+(t1.ss4/t1.all_sum-t3.s4_proportion)*ln((t1.ss4/t1.all_sum)/t3.s4_proportion)
+(t1.ss5/t1.all_sum-t3.s5_proportion)*ln((t1.ss5/t1.all_sum)/t3.s5_proportion)         as  s_psi

from 
(
select 
SUBSTRING(t1.biz_date,1,7)                                                     as   month_date 		,
t1.platform_model                                                              as   platform_model ,
sum(t1.s1)                                                            		   as  s1             ,
sum(t1.s2)                                                            		   as  s2             ,
sum(t1.s3)                                                            		   as  s3             ,
sum(t1.s4)                                                            		   as  s4             ,
sum(t1.s5)                                                            		   as  s5             ,
sum(t1.s_na)                                                            	   as  na             ,
sum(t1.s_unknown)                                                     		   as  unknown        ,
sum(t1.s_sn)                                                            	   as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                		   as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   		   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   		   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   		   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   		   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   		   as  s5_proportion  ,

sum(t1.s1) + sum(t1.s2) + sum(t1.s3) + sum(t1.s4) + sum(t1.s5) +
    CAST(sum(t1.s1) = 0 as INT)  +    
    CAST(sum(t1.s2) = 0 as INT)  +
    CAST(sum(t1.s3) = 0 as INT)  +
    CAST(sum(t1.s4) = 0 as INT)  +
	CAST(sum(t1.s5) = 0 as INT)                                       as  all_sum        ,
IF(sum(t1.s1) = 0,1,sum(t1.s1))   				  					  as  ss1            ,
IF(sum(t1.s2) = 0,1,sum(t1.s2))   				  					  as  ss2            ,
IF(sum(t1.s3) = 0,1,sum(t1.s3))   				  					  as  ss3            ,
IF(sum(t1.s4) = 0,1,sum(t1.s4))   				  					  as  ss4            ,
IF(sum(t1.s5) = 0,1,sum(t1.s5))   				  					  as  ss5        

from wp_rpt.psi_supervisory_android t1
group by SUBSTRING(t1.biz_date,1,7) , t1.platform_model
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'android-3') t3  on 1=1
;


-- 按天明细监控PSI指标 (iOS)

DROP TABLE IF EXISTS  wp_rpt.psi_supervisory_iOS ;

CREATE  table wp_rpt.psi_supervisory_iOS
AS
select 
t1.biz_date                                                                            as  biz_date ,
t1.platform_model                                                                      as  platform_model ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.sn                                                                                  as  s_sn           ,
t1.na                                                                                  as  s_na           ,
t1.`unknown`                                                                           as  s_unknown      ,
t1.sum_s                                                                               as  sum_s          ,
t2.wek_day                                                                             as  wek_day        ,
t2.month_day                                                                           as  month_day      ,
t2.week_day                                                                            as  week_day       ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
(t1.ss1/all_sum-t3.s1_proportion) * ln((t1.ss1/all_sum)/t3.s1_proportion)              as  s1_psi         ,
(t1.ss2/all_sum-t3.s2_proportion) * ln((t1.ss2/all_sum)/t3.s2_proportion)              as  s2_psi         ,
(t1.ss3/all_sum-t3.s3_proportion) * ln((t1.ss3/all_sum)/t3.s3_proportion)              as  s3_psi         ,
(t1.ss4/all_sum-t3.s4_proportion) * ln((t1.ss4/all_sum)/t3.s4_proportion)              as  s4_psi         ,
(t1.ss5/all_sum-t3.s5_proportion) * ln((t1.ss5/all_sum)/t3.s5_proportion)              as  s5_psi         ,

(t1.ss1/all_sum-t3.s1_proportion) * ln((t1.ss1/all_sum)/t3.s1_proportion) 
+(t1.ss2/all_sum-t3.s2_proportion) * ln((t1.ss2/all_sum)/t3.s2_proportion)  
+(t1.ss3/all_sum-t3.s3_proportion) * ln((t1.ss3/all_sum)/t3.s3_proportion) 
+(t1.ss4/all_sum-t3.s4_proportion) * ln((t1.ss4/all_sum)/t3.s4_proportion) 
+(t1.ss5/all_sum-t3.s5_proportion) * ln((t1.ss5/all_sum)/t3.s5_proportion)             as  s_psi


from
(
select 
to_date(t1.application_date)                                                             as  biz_date   ,
'iOS'                                                                                    as  platform_model ,
count(case when t1.self_score_class = 'S1'  then t1.loan_id end)                         as  s1            ,
count(case when t1.self_score_class = 'S2'  then t1.loan_id end)                         as  s2            ,
count(case when t1.self_score_class = 'S3'  then t1.loan_id end)                         as  s3            ,
count(case when t1.self_score_class = 'S4'  then t1.loan_id end)                         as  s4            ,
count(case when t1.self_score_class = 'S5'  then t1.loan_id end)                         as  s5            ,
count(case when t1.self_score_class = 'NA' then t1.loan_id end)                          as  na            ,
count(case when t1.self_score_class = 'unknown' then t1.loan_id end)                     as  unknown       ,
count(case when t1.self_score_class = 'SN' then t1.loan_id end)                          as  sn            ,
count(case when t1.self_score_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)   as  sum_s         ,
count(case when t1.self_score_class = 'S1'  then t1.loan_id end)/count(case when t1.self_score_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)       as  s1_proportion ,
count(case when t1.self_score_class = 'S2'  then t1.loan_id end)/count(case when t1.self_score_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)       as  s2_proportion ,
count(case when t1.self_score_class = 'S3'  then t1.loan_id end)/count(case when t1.self_score_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)       as  s3_proportion ,
count(case when t1.self_score_class = 'S4'  then t1.loan_id end)/count(case when t1.self_score_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)       as  s4_proportion ,
count(case when t1.self_score_class = 'S5'  then t1.loan_id end)/count(case when t1.self_score_class in ('S1','S2','S3','S4','S5') then t1.loan_id end)       as  s5_proportion ,
COUNT(CASE WHEN t1.self_score_class IN ('S1','S2','S3','S4','S5') THEN t1.loan_id END)  +
      CAST(COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END) = 0 as INT)  +    
      CAST(COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END) = 0 as INT)  +
      CAST(COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END) = 0 as INT)  +
      CAST(COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END) = 0 as INT)  +
	  CAST(COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END) = 0 as INT)                                                            		      as  all_sum       ,
IF(COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END))   				  as  ss1           ,
IF(COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END))   				  as  ss2           ,
IF(COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END))   				  as  ss3           ,
IF(COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END))   				  as  ss4           ,
IF(COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END))   				  as  ss5

from  weplay.rules_engine_item  t1 
left join weplay.merged_data t2 ON t1.loan_id = t2.loan_id
where to_date(t1.application_date) >= to_date(CURRENT_TIMESTAMP() - interval 60 day) 
and  to_date(t1.application_date) < to_date(CURRENT_TIMESTAMP())  
and  t2.score_card_version = '3.2'
and  t1.self_score_class in ('S1','S2','S3','S4','S5','SN','unknown','NA')
group by to_date(t1.application_date)
)  t1
left join wp_rpt.weekdate_model  t2   on  to_date(t1.biz_date) = to_date(t2.date_day)
left join (select * from  wp_rpt.benchmark_value where platform = 'iOS-2') t3  on 1=1
where t1.biz_date >= '2018-02-06'
; 


-- 汇总数据监控安卓PSI指标

DROP TABLE IF EXISTS  wp_rpt.psi_supervisory_android_total;

CREATE  TABLE wp_rpt.psi_supervisory_android_total
AS
select 
t1.title                                                                               as  title        ,
t1.platform_model                                                                      as  platform_model ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.sn                                                                                  as  s_sn             ,
t1.na                                                                                  as  s_na             ,
t1.`unknown`                                                                           as  s_unknown        ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi
from
(
select 
'总计'                                                                 as  title ,
'android'                                                             as  platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                            as  na             ,
sum(t1.s_unknown)                                                     as  unknown        ,
sum(t1.s_sn)                                                            as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_android t1
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'android-3') t3  on 1=1
;


-- 汇总数据监控PSI指标(iOS)

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_iOS_total ;

CREATE  TABLE  wp_rpt.psi_supervisory_iOS_total
AS
select 
t1.title                                                                               as  title        ,
t1.platform_model                                                                      as  platform_model   ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.sn                                                                                  as  s_sn             ,
t1.na                                                                                  as  s_na             ,
t1.`unknown`                                                                           as  s_unknown        ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi
from
(
select 
'总计'                                                                 as   title ,
'iOS'                                                                 as   platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                            as  na             ,
sum(t1.s_unknown)                                                     as  unknown        ,
sum(t1.s_sn)                                                            as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_iOS t1
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'iOS-2') t3  on 1=1
;



-- 按周明细监控PSI指标(iOS)

DROP TABLE IF EXISTS  wp_rpt.psi_supervisory_iOS_week ;

CREATE TABLE wp_rpt.psi_supervisory_iOS_week
AS 
select 
t1.week_date                                                                           as  week_date        ,
t1.platform_model                                                                      as  platform_model   ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.sn                                                                                  as  s_sn             ,
t1.na                                                                                  as  s_na             ,
t1.unknown                                                                             as  s_unknown        ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,

(t1.ss1/t1.all_sum-t3.s1_proportion)*ln((t1.ss1/t1.all_sum)/t3.s1_proportion)          as  s1_psi           ,
(t1.ss2/t1.all_sum-t3.s2_proportion)*ln((t1.ss2/t1.all_sum)/t3.s2_proportion)          as  s2_psi           ,
(t1.ss3/t1.all_sum-t3.s3_proportion)*ln((t1.ss3/t1.all_sum)/t3.s3_proportion)              as  s3_psi       ,
(t1.ss4/t1.all_sum-t3.s4_proportion)*ln((t1.ss4/t1.all_sum)/t3.s4_proportion)              as  s4_psi       ,
(t1.ss5/t1.all_sum-t3.s5_proportion)*ln((t1.ss5/t1.all_sum)/t3.s5_proportion)              as  s5_psi       ,

(t1.ss1/t1.all_sum-t3.s1_proportion)*ln((t1.ss1/t1.all_sum)/t3.s1_proportion)
+(t1.ss2/t1.all_sum-t3.s2_proportion)*ln((t1.ss2/t1.all_sum)/t3.s2_proportion)
+(t1.ss3/t1.all_sum-t3.s3_proportion)*ln((t1.ss3/t1.all_sum)/t3.s3_proportion) 
+(t1.ss4/t1.all_sum-t3.s4_proportion)*ln((t1.ss4/t1.all_sum)/t3.s4_proportion)
+(t1.ss5/t1.all_sum-t3.s5_proportion)*ln((t1.ss5/t1.all_sum)/t3.s5_proportion)             as  s_psi

from
(
select 
t1.week_day                                                           as  week_date,
t1.platform_model                                                     as  platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                          as  na             ,
sum(t1.s_unknown)                                                     as  unknown        ,
sum(t1.s_sn)                                                          as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  ,

sum(t1.s1) + sum(t1.s2) + sum(t1.s3) + sum(t1.s4) + sum(t1.s5) +
    CAST(sum(t1.s1) = 0 as INT)  +    
    CAST(sum(t1.s2) = 0 as INT)  +
    CAST(sum(t1.s3) = 0 as INT)  +
    CAST(sum(t1.s4) = 0 as INT)  +
	CAST(sum(t1.s5) = 0 as INT)                                       as  all_sum        ,
IF(sum(t1.s1) = 0,1,sum(t1.s1))   				  					  as  ss1            ,
IF(sum(t1.s2) = 0,1,sum(t1.s2))   				  					  as  ss2            ,
IF(sum(t1.s3) = 0,1,sum(t1.s3))   				  					  as  ss3            ,
IF(sum(t1.s4) = 0,1,sum(t1.s4))   				  					  as  ss4            ,
IF(sum(t1.s5) = 0,1,sum(t1.s5))   				  					  as  ss5 
from wp_rpt.psi_supervisory_iOS t1
group by t1.week_day , t1.platform_model
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'iOS-2') t3  on 1=1
;



-- 按月明细监控PSI指标(iOS)

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_iOS_month ;

CREATE TABLE wp_rpt.psi_supervisory_iOS_month
AS
select 
t1.month_date                                                                          as  month_date        ,
t1.platform_model                                                                      as  platform_model   ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.sn                                                                                  as  s_sn             ,
t1.na                                                                                  as  s_na             ,
t1.`unknown`                                                                           as  s_unknown        ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
(t1.ss1/t1.all_sum-t3.s1_proportion)*ln((t1.ss1/t1.all_sum)/t3.s1_proportion)          as  s1_psi         	 ,
(t1.ss2/t1.all_sum-t3.s2_proportion)*ln((t1.ss2/t1.all_sum)/t3.s2_proportion)          as  s2_psi         	 ,
(t1.ss3/t1.all_sum-t3.s3_proportion)*ln((t1.ss3/t1.all_sum)/t3.s3_proportion)          as  s3_psi         	 ,
(t1.ss4/t1.all_sum-t3.s4_proportion)*ln((t1.ss4/t1.all_sum)/t3.s4_proportion)          as  s4_psi         	 ,
(t1.ss5/t1.all_sum-t3.s5_proportion)*ln((t1.ss5/t1.all_sum)/t3.s5_proportion)          as  s5_psi         	 ,

(t1.ss1/t1.all_sum-t3.s1_proportion)*ln((t1.ss1/t1.all_sum)/t3.s1_proportion)
+(t1.ss2/t1.all_sum-t3.s2_proportion)*ln((t1.ss2/t1.all_sum)/t3.s2_proportion)
+(t1.ss3/t1.all_sum-t3.s3_proportion)*ln((t1.ss3/t1.all_sum)/t3.s3_proportion) 
+(t1.ss4/t1.all_sum-t3.s4_proportion)*ln((t1.ss4/t1.all_sum)/t3.s4_proportion)
+(t1.ss5/t1.all_sum-t3.s5_proportion)*ln((t1.ss5/t1.all_sum)/t3.s5_proportion)         as  s_psi

from 
(
select 
SUBSTRING(t1.biz_date,1,7)                                                     as   month_date ,
t1.platform_model                                                              as   platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                          as  na             ,
sum(t1.s_unknown)                                                     as  unknown        ,
sum(t1.s_sn)                                                          as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  ,

sum(t1.s1) + sum(t1.s2) + sum(t1.s3) + sum(t1.s4) + sum(t1.s5) +
    CAST(sum(t1.s1) = 0 as INT)  +    
    CAST(sum(t1.s2) = 0 as INT)  +
    CAST(sum(t1.s3) = 0 as INT)  +
    CAST(sum(t1.s4) = 0 as INT)  +
	CAST(sum(t1.s5) = 0 as INT)                                       as  all_sum        ,
IF(sum(t1.s1) = 0,1,sum(t1.s1))   				  					  as  ss1            ,
IF(sum(t1.s2) = 0,1,sum(t1.s2))   				  					  as  ss2            ,
IF(sum(t1.s3) = 0,1,sum(t1.s3))   				  					  as  ss3            ,
IF(sum(t1.s4) = 0,1,sum(t1.s4))   				  					  as  ss4            ,
IF(sum(t1.s5) = 0,1,sum(t1.s5))   				  					  as  ss5        

from wp_rpt.psi_supervisory_iOS t1
group by SUBSTRING(t1.biz_date,1,7) , t1.platform_model
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'iOS-2') t3  on 1=1
;



-- 按天明细监控PSI指标(H5-联通)

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_h5_unicom ;

CREATE table wp_rpt.psi_supervisory_h5_unicom
AS
SELECT
    t1.biz_date                                                                                                         AS biz_date       ,
    t1.platform_model                                                                                                   AS platform_model ,
    t1.s1                                                                                                               AS s1             ,
    t1.s2                                                                                                               AS s2             ,
    t1.s3                                                                                                               AS s3             ,
    t1.s4                                                                                                               AS s4             ,
    t1.s5                                                                                                               AS s5             ,
    t1.sn                                                                                                               AS s_sn           ,
    t1.na                                                                                                               AS s_na           ,
    t1.`unknown`                                                                                                        AS s_unknown      ,
    t1.sum_s                                                                                                            AS sum_s          ,
    t2.wek_day                                                                                                          AS wek_day        ,
    t2.month_day                                                                                                        AS month_day      ,
    t2.week_day                                                                                                         AS week_day       ,
    IF(t1.sum_s = 0,0,t1.s1/t1.sum_s)                                                                                   AS s1_proportion  ,
    IF(t1.sum_s = 0,0,t1.s2/t1.sum_s)                                                                                   AS s2_proportion  ,
    IF(t1.sum_s = 0,0,t1.s3/t1.sum_s)                                                                                   AS s3_proportion  ,
    IF(t1.sum_s = 0,0,t1.s4/t1.sum_s)                                                                                   AS s4_proportion  ,
    IF(t1.sum_s = 0,0,t1.s5/t1.sum_s)                                                                                   AS s5_proportion  ,

    (t1.ss1/all_sum-t3.s1_proportion) * ln((t1.ss1/all_sum)/t3.s1_proportion)                                           AS  s1_psi        ,
    (t1.ss2/all_sum-t3.s2_proportion) * ln((t1.ss2/all_sum)/t3.s2_proportion)                                           AS  s2_psi        ,
    (t1.ss3/all_sum-t3.s3_proportion) * ln((t1.ss3/all_sum)/t3.s3_proportion)                                           AS  s3_psi        ,
    (t1.ss4/all_sum-t3.s4_proportion) * ln((t1.ss4/all_sum)/t3.s4_proportion)                                           AS  s4_psi        ,
    (t1.ss5/all_sum-t3.s5_proportion) * ln((t1.ss5/all_sum)/t3.s5_proportion)                                           AS  s5_psi        ,

    (t1.ss1/all_sum-t3.s1_proportion) * ln((t1.ss1/all_sum)/t3.s1_proportion) +
    (t1.ss2/all_sum-t3.s2_proportion) * ln((t1.ss2/all_sum)/t3.s2_proportion) +
    (t1.ss3/all_sum-t3.s3_proportion) * ln((t1.ss3/all_sum)/t3.s3_proportion) +
    (t1.ss4/all_sum-t3.s4_proportion) * ln((t1.ss4/all_sum)/t3.s4_proportion) +
    (t1.ss5/all_sum-t3.s5_proportion) * ln((t1.ss5/all_sum)/t3.s5_proportion)                                           AS  psi
FROM
(
    SELECT
    to_date(t1.application_date)                                                                                                                AS biz_date          ,
    'H5_unicom'                                                                                                                                 AS platform_model    ,
    COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END)                                                                             AS s1                ,
    COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END)                                                                             AS s2                ,
    COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END)                                                                             AS s3                ,
    COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END)                                                                             AS s4                ,
    COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END)                                                                             AS s5                ,
    COUNT(CASE WHEN t1.self_score_class = 'NA' THEN t1.loan_id END)                                                                             AS na                ,
    COUNT(CASE WHEN t1.self_score_class = 'SN' THEN t1.loan_id END)                                                                             AS sn                ,
    COUNT(CASE WHEN t1.self_score_class = 'unknown' THEN t1.loan_id END)                                                                        AS `unknown`         ,
    COUNT(CASE WHEN t1.self_score_class IN ('S1','S2','S3','S4','S5') THEN t1.loan_id END)                                                      AS sum_s             ,
    COUNT(CASE WHEN t1.self_score_class IN ('S1','S2','S3','S4','S5') THEN t1.loan_id END) +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END) = 0 as INT)                                                            AS all_sum           ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END))   AS ss1               ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END))   AS ss2               ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END))   AS ss3               ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END))   AS ss4               ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END))   AS ss5
FROM  weplay.rules_engine_item  t1
LEFT JOIN weplay.merged_data t2 ON t1.loan_id = t2.loan_id
WHERE to_date(t1.application_date) >= to_date(CURRENT_TIMESTAMP() - interval 60 day)
AND to_date(t1.application_date) < to_date(CURRENT_TIMESTAMP())
AND t2.score_card_version = '4.1'
AND t1.self_score_class in ('S1','S2','S3','S4','S5','SN','unknown','NA')
GROUP BY to_date(t1.application_date)
)  t1
LEFT JOIN wp_rpt.weekdate_model t2 ON to_date(t1.biz_date) = to_date(t2.date_day)
LEFT JOIN (SELECT * FROM wp_rpt.benchmark_value WHERE platform = 'H5_unicom-2') t3 ON 1 = 1
;




-- 汇总数据监控PSI指标(h5-联通)

DROP TABLE IF EXISTS  wp_rpt.psi_supervisory_h5_unicom_total ;

CREATE TABLE wp_rpt.psi_supervisory_h5_unicom_total
AS
select 
t1.title                                                                               as  title        ,
t1.platform_model                                                                      as  platform_model   ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.na                                                                                  as  na             ,
t1.`unknown`                                                                           as  unknown        ,
t1.sn                                                                                  as  sn             ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi
from
(
select 
'总计'                                                                 as  title ,
'H5_unicom'                                                           as  platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                          as  na             ,
sum(t1.s_unknown)                                                     as  `unknown`      ,
sum(t1.s_sn)                                                          as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_h5_unicom t1
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'H5_unicom-2') t3  on 1=1
;




-- 按周明细监控PSI指标(h5-联通)

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_h5_unicom_week;

CREATE TABLE wp_rpt.psi_supervisory_h5_unicom_week
AS
select 
t1.week_date                                                                           as  week_date        ,
t1.platform_model                                                                      as  platform_model   ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.na                                                                                  as  na             ,
t1.unknown                                                                             as  unknown        ,
t1.sn                                                                                  as  sn             ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi

from
(
select 
t1.week_day                                                           as  week_date,
t1.platform_model                                                     as  platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                            as  na             ,
sum(t1.s_unknown)                                                       as  unknown        ,
sum(t1.s_sn)                                                            as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_h5_unicom t1
group by t1.week_day , t1.platform_model
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'H5_unicom-2') t3  on 1=1
;




-- 按月明细监控PSI指标(h5-联通)

DROP TABLE IF EXISTS  wp_rpt.psi_supervisory_h5_unicom_month ;

CREATE TABLE wp_rpt.psi_supervisory_h5_unicom_month
AS
select 
t1.month_date                                                                          as  month_date     ,
t1.platform_model                                                                      as  platform_model ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.na                                                                                  as  na             ,
t1.`unknown`                                                                           as  unknown        ,
t1.sn                                                                                  as  sn             ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi

from 
(
select 
SUBSTRING(t1.biz_date,1,7)                                                     as   month_date ,
t1.platform_model                                                              as   platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                          as  na             ,
sum(t1.s_unknown)                                                     as  unknown        ,
sum(t1.s_sn)                                                          as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_h5_unicom t1
group by SUBSTRING(t1.biz_date,1,7) , t1.platform_model
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'H5_unicom-2') t3  on 1=1
;





-- 按天明细监控PSI指标(H5-移动)

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_h5_mob;

CREATE TABLE wp_rpt.psi_supervisory_h5_mob
AS
SELECT
    t1.biz_date                                                                                                         AS biz_date       ,
    t1.platform_model                                                                                                   AS platform_model ,
    t1.s1                                                                                                               AS s1             ,
    t1.s2                                                                                                               AS s2             ,
    t1.s3                                                                                                               AS s3             ,
    t1.s4                                                                                                               AS s4             ,
    t1.s5                                                                                                               AS s5             ,
    t1.sn                                                                                                               AS s_sn           ,
    t1.na                                                                                                               AS s_na           ,
    t1.`unknown`                                                                                                        AS s_unknown      ,
    t1.sum_s                                                                                                            AS sum_s          ,
    t2.wek_day                                                                                                          AS wek_day        ,
    t2.month_day                                                                                                        AS month_day      ,
    t2.week_day                                                                                                         AS week_day       ,
    IF(t1.sum_s = 0,0,t1.s1/t1.sum_s)                                                                                   AS s1_proportion  ,
    IF(t1.sum_s = 0,0,t1.s2/t1.sum_s)                                                                                   AS s2_proportion  ,
    IF(t1.sum_s = 0,0,t1.s3/t1.sum_s)                                                                                   AS s3_proportion  ,
    IF(t1.sum_s = 0,0,t1.s4/t1.sum_s)                                                                                   AS s4_proportion  ,
    IF(t1.sum_s = 0,0,t1.s5/t1.sum_s)                                                                                   AS s5_proportion  ,

    (t1.ss1/all_sum-t3.s1_proportion) * ln((t1.ss1/all_sum)/t3.s1_proportion)                                           AS  s1_psi        ,
    (t1.ss2/all_sum-t3.s2_proportion) * ln((t1.ss2/all_sum)/t3.s2_proportion)                                           AS  s2_psi        ,
    (t1.ss3/all_sum-t3.s3_proportion) * ln((t1.ss3/all_sum)/t3.s3_proportion)                                           AS  s3_psi        ,
    (t1.ss4/all_sum-t3.s4_proportion) * ln((t1.ss4/all_sum)/t3.s4_proportion)                                           AS  s4_psi        ,
    (t1.ss5/all_sum-t3.s5_proportion) * ln((t1.ss5/all_sum)/t3.s5_proportion)                                           AS  s5_psi        ,

    (t1.ss1/all_sum-t3.s1_proportion) * ln((t1.ss1/all_sum)/t3.s1_proportion) +
    (t1.ss2/all_sum-t3.s2_proportion) * ln((t1.ss2/all_sum)/t3.s2_proportion) +
    (t1.ss3/all_sum-t3.s3_proportion) * ln((t1.ss3/all_sum)/t3.s3_proportion) +
    (t1.ss4/all_sum-t3.s4_proportion) * ln((t1.ss4/all_sum)/t3.s4_proportion) +
    (t1.ss5/all_sum-t3.s5_proportion) * ln((t1.ss5/all_sum)/t3.s5_proportion)                                           AS  psi
FROM
(
    SELECT
    to_date(t1.application_date)                                                                                                                AS biz_date          ,
    'H5_mob'                                                                                                                                    AS platform_model    ,
    COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END)                                                                             AS s1                ,
    COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END)                                                                             AS s2                ,
    COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END)                                                                             AS s3                ,
    COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END)                                                                             AS s4                ,
    COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END)                                                                             AS s5                ,
    COUNT(CASE WHEN t1.self_score_class = 'NA' THEN t1.loan_id END)                                                                             AS na                ,
    COUNT(CASE WHEN t1.self_score_class = 'SN' THEN t1.loan_id END)                                                                             AS sn                ,
    COUNT(CASE WHEN t1.self_score_class = 'unknown' THEN t1.loan_id END)                                                                        AS `unknown`         ,
    COUNT(CASE WHEN t1.self_score_class IN ('S1','S2','S3','S4','S5') THEN t1.loan_id END)                                                      AS sum_s             ,
    COUNT(CASE WHEN t1.self_score_class IN ('S1','S2','S3','S4','S5') THEN t1.loan_id END) +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END) = 0 as INT)                                                            AS all_sum           ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END))   AS ss1               ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END))   AS ss2               ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END))   AS ss3               ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END))   AS ss4               ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END))   AS ss5
FROM  weplay.rules_engine_item  t1
LEFT JOIN weplay.merged_data t2 ON t1.loan_id = t2.loan_id
WHERE to_date(t1.application_date) >= to_date(CURRENT_TIMESTAMP() - interval 60 day)
AND to_date(t1.application_date) < to_date(CURRENT_TIMESTAMP())
AND t2.score_card_version = '6.0'
AND t1.self_score_class in ('S1','S2','S3','S4','S5','SN','unknown','NA')
GROUP BY to_date(t1.application_date)
)  t1
LEFT JOIN wp_rpt.weekdate_model t2 ON to_date(t1.biz_date) = to_date(t2.date_day)
LEFT JOIN (SELECT * FROM wp_rpt.benchmark_value WHERE platform = 'H5_mobile-2') t3 ON 1 = 1
;



-- 汇总数据监控PSI指标(h5-移动)

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_h5_mob_total;

CREATE TABLE wp_rpt.psi_supervisory_h5_mob_total
AS
SELECT 
t1.title                                                                               as  title        ,
t1.platform_model                                                                      as  platform_model ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.na                                                                                  as  na             ,
t1.`unknown`                                                                           as  unknown        ,
t1.sn                                                                                  as  sn             ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi
from
(
select 
'总计'                                                                 as  title ,
'H5_mob'                                                              as  platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                            as  na             ,
sum(t1.s_unknown)                                                     as  unknown        ,
sum(t1.s_sn)                                                            as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_h5_mob t1
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'H5_mobile-2') t3  on 1=1
;



-- 按周明细监控PSI指标(h5-移动)

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_h5_mob_week;

CREATE TABLE wp_rpt.psi_supervisory_h5_mob_week
AS
select 
t1.week_date                                                                           as  week_date        ,
t1.platform_model                                                                      as  platform_model   ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.na                                                                                  as  na             ,
t1.unknown                                                                             as  unknown        ,
t1.sn                                                                                  as  sn             ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi

from
(
select 
t1.week_day                                                           as  week_date,
t1.platform_model                                                     as  platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                            as  na             ,
sum(t1.s_unknown)                                                     as  unknown        ,
sum(t1.s_sn)                                                            as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_h5_mob t1
group by t1.week_day , t1.platform_model
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'H5_mobile-2') t3  on 1=1
;



-- 按月明细监控PSI指标(h5-移动)
DROP TABLE IF EXISTS wp_rpt.psi_supervisory_h5_mob_month ;

CREATE TABLE wp_rpt.psi_supervisory_h5_mob_month
AS
SELECT 
t1.month_date                                                                          as  month_date     ,
t1.platform_model                                                                      as  platform_model ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.na                                                                                  as  na             ,
t1.`unknown`                                                                           as  unknown        ,
t1.sn                                                                                  as  sn             ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi

from 
(
select 
SUBSTRING(t1.biz_date,1,7)                                            as  month_date    ,
t1.platform_model                                                     as  platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                            as  na             ,
sum(t1.s_unknown)                                                     as  unknown        ,
sum(t1.s_sn)                                                            as  sn             ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_h5_mob t1
group by SUBSTRING(t1.biz_date,1,7) , t1.platform_model
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'H5_mobile-2') t3  on 1=1
;




-- 按天明细监控PSI指标(h5-电信)
-- by Chace

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_h5_telecom;

CREATE TABLE wp_rpt.psi_supervisory_h5_telecom
AS
SELECT
    t1.biz_date                                                                                                         AS biz_date       ,
    t1.platform_model                                                                                                   AS platform_model ,
    t1.s1                                                                                                               AS s1             ,
    t1.s2                                                                                                               AS s2             ,
    t1.s3                                                                                                               AS s3             ,
    t1.s4                                                                                                               AS s4             ,
    t1.s5                                                                                                               AS s5             ,
    t1.sn                                                                                                               AS s_sn           ,
    t1.na                                                                                                               AS s_na           ,
    t1.`unknown`                                                                                                        AS s_unknown      ,
    t1.sum_s                                                                                                            AS sum_s          ,
    t2.wek_day                                                                                                          AS wek_day        ,
    t2.month_day                                                                                                        AS month_day      ,
    t2.week_day                                                                                                         AS week_day       ,
    IF(t1.sum_s = 0,0,t1.s1/t1.sum_s)                                                                                   AS s1_proportion  ,
    IF(t1.sum_s = 0,0,t1.s2/t1.sum_s)                                                                                   AS s2_proportion  ,
    IF(t1.sum_s = 0,0,t1.s3/t1.sum_s)                                                                                   AS s3_proportion  ,
    IF(t1.sum_s = 0,0,t1.s4/t1.sum_s)                                                                                   AS s4_proportion  ,
    IF(t1.sum_s = 0,0,t1.s5/t1.sum_s)                                                                                   AS s5_proportion  ,

    (t1.ss1/all_sum-t3.s1_proportion) * ln((t1.ss1/all_sum)/t3.s1_proportion)                                           AS  s1_psi        ,
    (t1.ss2/all_sum-t3.s2_proportion) * ln((t1.ss2/all_sum)/t3.s2_proportion)                                           AS  s2_psi        ,
    (t1.ss3/all_sum-t3.s3_proportion) * ln((t1.ss3/all_sum)/t3.s3_proportion)                                           AS  s3_psi        ,
    (t1.ss4/all_sum-t3.s4_proportion) * ln((t1.ss4/all_sum)/t3.s4_proportion)                                           AS  s4_psi        ,
    (t1.ss5/all_sum-t3.s5_proportion) * ln((t1.ss5/all_sum)/t3.s5_proportion)                                           AS  s5_psi        ,

    (t1.ss1/all_sum-t3.s1_proportion) * ln((t1.ss1/all_sum)/t3.s1_proportion) +
    (t1.ss2/all_sum-t3.s2_proportion) * ln((t1.ss2/all_sum)/t3.s2_proportion) +
    (t1.ss3/all_sum-t3.s3_proportion) * ln((t1.ss3/all_sum)/t3.s3_proportion) +
    (t1.ss4/all_sum-t3.s4_proportion) * ln((t1.ss4/all_sum)/t3.s4_proportion) +
    (t1.ss5/all_sum-t3.s5_proportion) * ln((t1.ss5/all_sum)/t3.s5_proportion)                                           AS  psi
FROM
(
    SELECT
    to_date(t1.application_date)                                                                                                                AS biz_date          ,
    'H5_telecom'                                                                                                                                AS platform_model    ,
    COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END)                                                                             AS s1                ,
    COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END)                                                                             AS s2                ,
    COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END)                                                                             AS s3                ,
    COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END)                                                                             AS s4                ,
    COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END)                                                                             AS s5                ,
    COUNT(CASE WHEN t1.self_score_class = 'NA' THEN t1.loan_id END)                                                                             AS na                ,
    COUNT(CASE WHEN t1.self_score_class = 'SN' THEN t1.loan_id END)                                                                             AS sn                ,
    COUNT(CASE WHEN t1.self_score_class = 'unknown' THEN t1.loan_id END)                                                                        AS `unknown`         ,
    COUNT(CASE WHEN t1.self_score_class IN ('S1','S2','S3','S4','S5') THEN t1.loan_id END)                                                      AS sum_s             ,
    COUNT(CASE WHEN t1.self_score_class IN ('S1','S2','S3','S4','S5') THEN t1.loan_id END) +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END) = 0 as INT)  +
    CAST(COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END) = 0 as INT)                                                            AS all_sum           ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S1' THEN t1.loan_id END))   AS  ss1              ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S2' THEN t1.loan_id END))   AS  ss2              ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S3' THEN t1.loan_id END))   AS  ss3              ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S4' THEN t1.loan_id END))   AS  ss4              ,
    IF(COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END) = 0,1,COUNT(CASE WHEN t1.self_score_class = 'S5' THEN t1.loan_id END))   AS  ss5
FROM  weplay.rules_engine_item  t1
LEFT JOIN weplay.merged_data t2 ON t1.loan_id = t2.loan_id
WHERE to_date(t1.application_date) >= to_date(CURRENT_TIMESTAMP() - interval 60 day)
AND to_date(t1.application_date) < to_date(CURRENT_TIMESTAMP())
AND t2.score_card_version = '7.0'
AND t1.self_score_class in ('S1','S2','S3','S4','S5','SN','unknown','NA')
GROUP BY to_date(t1.application_date)
)  t1
LEFT JOIN wp_rpt.weekdate_model t2 ON to_date(t1.biz_date) = to_date(t2.date_day)
LEFT JOIN (SELECT * FROM wp_rpt.benchmark_value WHERE platform = 'H5_telecom') t3 ON 1 = 1
WHERE t1.biz_date >= '2018-02-07'
;



-- 按周明细监控PSI指标(h5-电信)

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_h5_telecom_week;

CREATE TABLE wp_rpt.psi_supervisory_h5_telecom_week
AS
select 
t1.week_date                                                                           as  week_date        ,
t1.platform_model                                                                      as  platform_model   ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.na                                                                                  as  na             ,
t1.sn                                                                                  as  sn             ,
t1.s_unknown                                                                           as  s_unknown      ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi

from
(
select 
t1.week_day                                                           as  week_date,
t1.platform_model                                                     as  platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                          as  na             ,
sum(t1.s_sn)                                                          as  sn             ,
sum(t1.s_unknown)                                                     as  s_unknown      ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_h5_telecom t1
group by t1.week_day , t1.platform_model
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'H5_telecom') t3  on 1=1
;



-- 按月明细监控PSI指标(h5-电信)
DROP TABLE IF EXISTS wp_rpt.psi_supervisory_h5_telecom_month ;

CREATE TABLE wp_rpt.psi_supervisory_h5_telecom_month
AS
SELECT 
t1.month_date                                                                          as  month_date     ,
t1.platform_model                                                                      as  platform_model ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.na                                                                                  as  na             ,
t1.sn                                                                                  as  sn             ,
t1.s_unknown                                                                           as  s_unknown      ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi

from 
(
select 
SUBSTRING(t1.biz_date,1,7)                                            as   month_date    ,
t1.platform_model                                                     as   platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                          as  na             ,
sum(t1.s_sn)                                                          as  sn             ,
sum(t1.s_unknown)                                                     as  s_unknown      ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_h5_telecom t1
group by SUBSTRING(t1.biz_date,1,7) , t1.platform_model
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'H5_telecom') t3  on 1=1
;


-- 汇总数据监控PSI指标(h5-电信）

DROP TABLE IF EXISTS wp_rpt.psi_supervisory_h5_telecom_total ;

CREATE  TABLE  wp_rpt.psi_supervisory_h5_telecom_total
AS
select 
t1.title                                                                               as  title        ,
t1.platform_model                                                                      as  platform_model   ,
t1.s1                                                                                  as  s1             ,
t1.s2                                                                                  as  s2             ,
t1.s3                                                                                  as  s3             ,
t1.s4                                                                                  as  s4             ,
t1.s5                                                                                  as  s5             ,
t1.na                                                                                  as  na             ,
t1.sn                                                                                  as  sn             ,
t1.s_unknown                                                                           as  s_unknown      ,
t1.sum_s                                                                               as  sum_s          ,
t1.s1_proportion                                                                       as  s1_proportion  ,
t1.s2_proportion                                                                       as  s2_proportion  ,
t1.s3_proportion                                                                       as  s3_proportion  ,
t1.s4_proportion                                                                       as  s4_proportion  ,
t1.s5_proportion                                                                       as  s5_proportion  ,
IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0)              as  s1_psi         ,
IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0)              as  s2_psi         ,
IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)              as  s3_psi         ,
IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)              as  s4_psi         ,
IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)              as  s5_psi         ,

IF(t1.s1_proportion <>0,(t1.s1_proportion-t3.s1_proportion)*ln(t1.s1_proportion/t3.s1_proportion),0) 
+IF(t1.s2_proportion <>0,(t1.s2_proportion-t3.s2_proportion)*ln(t1.s2_proportion/t3.s2_proportion),0) 
+IF(t1.s3_proportion <>0,(t1.s3_proportion-t3.s3_proportion)*ln(t1.s3_proportion/t3.s3_proportion),0)
+IF(t1.s4_proportion <>0,(t1.s4_proportion-t3.s4_proportion)*ln(t1.s4_proportion/t3.s4_proportion),0)
+IF(t1.s5_proportion <>0,(t1.s5_proportion-t3.s5_proportion)*ln(t1.s5_proportion/t3.s5_proportion),0)             as  s_psi
from
(
select 
'总计'                                                                 as   title ,
'H5_telecom'                                                          as   platform_model ,
sum(t1.s1)                                                            as  s1             ,
sum(t1.s2)                                                            as  s2             ,
sum(t1.s3)                                                            as  s3             ,
sum(t1.s4)                                                            as  s4             ,
sum(t1.s5)                                                            as  s5             ,
sum(t1.s_na)                                                          as  na             ,
sum(t1.s_sn)                                                          as  sn             ,
sum(t1.s_unknown)                                                     as  s_unknown      ,
sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5)                as  sum_s          ,
sum(t1.s1)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s1_proportion  ,
sum(t1.s2)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s2_proportion  ,
sum(t1.s3)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s3_proportion  ,
sum(t1.s4)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s4_proportion  ,
sum(t1.s5)/(sum(t1.s1)+sum(t1.s2)+sum(t1.s3)+sum(t1.s4)+sum(t1.s5))   as  s5_proportion  

from wp_rpt.psi_supervisory_h5_telecom t1
) t1
left join (select * from  wp_rpt.benchmark_value where platform = 'H5_telecom') t3  on 1=1
;



-- CSI 变量监控
-- Kaka 

-- android 3.3 

DROP TABLE IF EXISTS  wp_rpt.android_daily_val ;

CREATE TABLE wp_rpt.android_daily_val
AS
SELECT 
        to_date(application_date)                                           AS     biz_date  ,
        COUNT(CASE WHEN score_card_sv1 = '0.0' THEN loan_id END)            AS     v11       , 
        COUNT(CASE WHEN score_card_sv1 = '14.0' THEN loan_id END)           AS     v12       ,
        COUNT(CASE WHEN score_card_sv2 = '0.0' THEN loan_id END)            AS     v21       ,
        COUNT(CASE WHEN score_card_sv2 = '4.0' THEN loan_id END)            AS     v22       ,
        COUNT(CASE WHEN score_card_sv2 = '7.0' THEN loan_id END)            AS     v23       ,
        COUNT(CASE WHEN score_card_sv2 = '10.0' THEN loan_id END)           AS     v24       ,
        COUNT(CASE WHEN score_card_sv2 = '5.0' THEN loan_id END)            AS     v25       ,
        COUNT(CASE WHEN score_card_sv3 = '0.0' THEN loan_id END)            AS     v31       , 
        COUNT(CASE WHEN score_card_sv3 = '4.0' THEN loan_id END)            AS     v32       , 
        COUNT(CASE WHEN score_card_sv4 = '0.0' THEN loan_id END)            AS     v41       , 
        COUNT(CASE WHEN score_card_sv4 = '7.0' THEN loan_id END)            AS     v42       , 
        COUNT(CASE WHEN score_card_sv4 = '17.0' THEN loan_id END)           AS     v43       , 
        COUNT(CASE WHEN score_card_sv5 = '0.0' THEN loan_id END)            AS     v51       , 
        COUNT(CASE WHEN score_card_sv5 = '12.0' THEN loan_id END)           AS     v52       , 
        COUNT(CASE WHEN score_card_sv5 = '27.0' THEN loan_id END)           AS     v53       , 
        COUNT(CASE WHEN score_card_sv5 = '41.0' THEN loan_id END)           AS     v54       , 
        COUNT(CASE WHEN score_card_sv5 = '19.0' THEN loan_id END)           AS     v55       , 
        COUNT(CASE WHEN score_card_sv5 = '23.0' THEN loan_id END)           AS     v56       , 
        COUNT(CASE WHEN score_card_sv6 = '0.0' THEN loan_id END)            AS     v64       , 
        COUNT(CASE WHEN score_card_sv6 = '1.0' THEN loan_id END)            AS     v61       , 
        COUNT(CASE WHEN score_card_sv6 = '2.0' THEN loan_id END)            AS     v62       , 
        COUNT(CASE WHEN score_card_sv6 = '5.0' THEN loan_id END)            AS     v63       , 
        COUNT(CASE WHEN score_card_sv7 = '0.0' THEN loan_id END)            AS     v71       ,
        COUNT(CASE WHEN score_card_sv7 = '6.0' THEN loan_id END)            AS     v72       ,
        COUNT(CASE WHEN score_card_sv8 = '2.0' THEN loan_id END)            AS     v85       ,
        COUNT(CASE WHEN score_card_sv8 = '0.0' THEN loan_id END)            AS     v81       ,
        COUNT(CASE WHEN score_card_sv8 = '3.0' THEN loan_id END)            AS     v82       ,
        COUNT(CASE WHEN score_card_sv8 = '7.0' THEN loan_id END)            AS     v83       ,
        COUNT(CASE WHEN score_card_sv8 = '10.0' THEN loan_id END)           AS     v84       ,
        COUNT(CASE WHEN score_card_sv9 = '0.0' THEN loan_id END)            AS     v91       ,
        COUNT(CASE WHEN score_card_sv9 = '1.0' THEN loan_id END)            AS     v92       ,
        COUNT(CASE WHEN score_card_sv9 = '4.0' THEN loan_id END)            AS     v93       ,
        COUNT(CASE WHEN score_card_sv9 = '7.0' THEN loan_id END)            AS     v94       ,
        COUNT(CASE WHEN score_card_sv10 = '0.0' THEN loan_id END)           AS     v103      ,
        COUNT(CASE WHEN score_card_sv10 = '14.0' THEN loan_id END)          AS     v101      ,
        COUNT(CASE WHEN score_card_sv10 = '21.0' THEN loan_id END)          AS     v102      ,
        COUNT(CASE WHEN score_card_sv11 = '10.0' THEN loan_id END)          AS     v115      ,
        COUNT(CASE WHEN score_card_sv11 = '16.0' THEN loan_id END)          AS     v111      ,
        COUNT(CASE WHEN score_card_sv11 = '12.0' THEN loan_id END)          AS     v112      ,
        COUNT(CASE WHEN score_card_sv11 = '4.0' THEN loan_id END)           AS     v113      ,
        COUNT(CASE WHEN score_card_sv11 = '0.0' THEN loan_id END)           AS     v114      ,
        COUNT(CASE WHEN score_card_sv12 = '5.0' THEN loan_id END)           AS     v125      ,
        COUNT(CASE WHEN score_card_sv12 = '9.0' THEN loan_id END)           AS     v121      ,
        COUNT(CASE WHEN score_card_sv12 = '4.0' THEN loan_id END)           AS     v122      ,
        COUNT(CASE WHEN score_card_sv12 = '2.0' THEN loan_id END)           AS     v123      ,
        COUNT(CASE WHEN score_card_sv12 = '0.0' THEN loan_id END)           AS     v124      ,
        COUNT(CASE WHEN score_card_sv13 = '25.0' THEN loan_id END)          AS     v135      ,
        COUNT(CASE WHEN score_card_sv13 = '27.0' THEN loan_id END)          AS     v131      ,
        COUNT(CASE WHEN score_card_sv13 = '19.0' THEN loan_id END)          AS     v132      ,
        COUNT(CASE WHEN score_card_sv13 = '10.0' THEN loan_id END)          AS     v133      ,
        COUNT(CASE WHEN score_card_sv13 = '0.0' THEN loan_id END)           AS     v134      ,
        COUNT(CASE WHEN score_card_sv14 = '0.0' THEN loan_id END)           AS     v141      ,
        COUNT(CASE WHEN score_card_sv14 = '1.0' THEN loan_id END)           AS     v142      ,
        COUNT(CASE WHEN score_card_sv14 = '3.0' THEN loan_id END)           AS     v143      ,
        COUNT(CASE WHEN score_card_sv14 = '6.0' THEN loan_id END)           AS     v144      ,
        COUNT(CASE WHEN score_card_sv14 = '9.0' THEN loan_id END)           AS     v145      ,
        COUNT(CASE WHEN score_card_sv15 = '32.0' THEN loan_id END)          AS     v151      ,
        COUNT(CASE WHEN score_card_sv15 = '22.0' THEN loan_id END)          AS     v152      ,
        COUNT(CASE WHEN score_card_sv15 = '10.0' THEN loan_id END)          AS     v153      ,
        COUNT(CASE WHEN score_card_sv15 = '0.0' THEN loan_id END)           AS     v154      
FROM weplay.merged_data
WHERE score_card_version = '3.3'
AND self_score_class in ('S1','S2','S3','S4','S5')
AND to_date(application_date) >= '2018-01-26'
AND to_date(application_date) < to_date(CURRENT_TIMESTAMP())
GROUP BY to_date(application_date);




-- android 各自占比

DROP TABLE IF EXISTS wp_rpt.android_daily_pct  ;

CREATE TABLE  wp_rpt.android_daily_pct 
AS
SELECT 
        to_date(application_date)                                                                                                 AS     biz_date  ,
        COUNT(CASE WHEN score_card_sv1 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv1 is not null THEN loan_id END)     AS     v11       , 
        COUNT(CASE WHEN score_card_sv1 = '14.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv1 is not null THEN loan_id END)    AS     v12       ,
        COUNT(CASE WHEN score_card_sv2 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv2 is not null THEN loan_id END)     AS     v21       ,
        COUNT(CASE WHEN score_card_sv2 = '4.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv2 is not null THEN loan_id END)     AS     v22       ,
        COUNT(CASE WHEN score_card_sv2 = '7.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv2 is not null THEN loan_id END)     AS     v23       ,
        COUNT(CASE WHEN score_card_sv2 = '10.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv2 is not null THEN loan_id END)    AS     v24       ,
        COUNT(CASE WHEN score_card_sv2 = '5.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv2 is not null THEN loan_id END)     AS     v25       ,
        COUNT(CASE WHEN score_card_sv3 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv3 is not null THEN loan_id END)     AS     v31       , 
        COUNT(CASE WHEN score_card_sv3 = '4.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv3 is not null THEN loan_id END)     AS     v32       , 
        COUNT(CASE WHEN score_card_sv4 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv4 is not null THEN loan_id END)     AS     v41       , 
        COUNT(CASE WHEN score_card_sv4 = '7.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv4 is not null THEN loan_id END)     AS     v42       , 
        COUNT(CASE WHEN score_card_sv4 = '17.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv4 is not null THEN loan_id END)    AS     v43       , 
        COUNT(CASE WHEN score_card_sv5 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv5 is not null THEN loan_id END)     AS     v51       , 
        COUNT(CASE WHEN score_card_sv5 = '12.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv5 is not null THEN loan_id END)    AS     v52       , 
        COUNT(CASE WHEN score_card_sv5 = '27.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv5 is not null THEN loan_id END)    AS     v53       , 
        COUNT(CASE WHEN score_card_sv5 = '41.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv5 is not null THEN loan_id END)    AS     v54       , 
        COUNT(CASE WHEN score_card_sv5 = '19.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv5 is not null THEN loan_id END)    AS     v55       , 
        COUNT(CASE WHEN score_card_sv5 = '23.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv5 is not null THEN loan_id END)    AS     v56       , 
        COUNT(CASE WHEN score_card_sv6 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv6 is not null THEN loan_id END)     AS     v64       , 
        COUNT(CASE WHEN score_card_sv6 = '1.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv6 is not null THEN loan_id END)     AS     v61       , 
        COUNT(CASE WHEN score_card_sv6 = '2.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv6 is not null THEN loan_id END)     AS     v62       , 
        COUNT(CASE WHEN score_card_sv6 = '5.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv6 is not null THEN loan_id END)     AS     v63       , 
        COUNT(CASE WHEN score_card_sv7 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv7 is not null THEN loan_id END)     AS     v71       ,
        COUNT(CASE WHEN score_card_sv7 = '6.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv7 is not null THEN loan_id END)     AS     v72       ,
        COUNT(CASE WHEN score_card_sv8 = '2.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv8 is not null THEN loan_id END)     AS     v85       ,
        COUNT(CASE WHEN score_card_sv8 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv8 is not null THEN loan_id END)     AS     v81       ,
        COUNT(CASE WHEN score_card_sv8 = '3.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv8 is not null THEN loan_id END)     AS     v82       ,
        COUNT(CASE WHEN score_card_sv8 = '7.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv8 is not null THEN loan_id END)     AS     v83       ,
        COUNT(CASE WHEN score_card_sv8 = '10.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv8 is not null THEN loan_id END)    AS     v84       ,
        COUNT(CASE WHEN score_card_sv9 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv9 is not null THEN loan_id END)     AS     v91       ,
        COUNT(CASE WHEN score_card_sv9 = '1.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv9 is not null THEN loan_id END)     AS     v92       ,
        COUNT(CASE WHEN score_card_sv9 = '4.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv9 is not null THEN loan_id END)     AS     v93       ,
        COUNT(CASE WHEN score_card_sv9 = '7.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv9 is not null THEN loan_id END)     AS     v94       ,
        COUNT(CASE WHEN score_card_sv10 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv10 is not null THEN loan_id END)   AS     v103      ,
        COUNT(CASE WHEN score_card_sv10 = '14.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv10 is not null THEN loan_id END)  AS     v101      ,
        COUNT(CASE WHEN score_card_sv10 = '21.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv10 is not null THEN loan_id END)  AS     v102      ,
        COUNT(CASE WHEN score_card_sv11 = '10.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv11 is not null THEN loan_id END)  AS     v115      ,
        COUNT(CASE WHEN score_card_sv11 = '16.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv11 is not null THEN loan_id END)  AS     v111      ,
        COUNT(CASE WHEN score_card_sv11 = '12.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv11 is not null THEN loan_id END)  AS     v112      ,
        COUNT(CASE WHEN score_card_sv11 = '4.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv11 is not null THEN loan_id END)   AS     v113      ,
        COUNT(CASE WHEN score_card_sv11 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv11 is not null THEN loan_id END)   AS     v114      ,
        COUNT(CASE WHEN score_card_sv12 = '5.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv12 is not null THEN loan_id END)   AS     v125      ,
        COUNT(CASE WHEN score_card_sv12 = '9.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv12 is not null THEN loan_id END)   AS     v121      ,
        COUNT(CASE WHEN score_card_sv12 = '4.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv12 is not null THEN loan_id END)   AS     v122      ,
        COUNT(CASE WHEN score_card_sv12 = '2.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv12 is not null THEN loan_id END)   AS     v123      ,
        COUNT(CASE WHEN score_card_sv12 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv12 is not null THEN loan_id END)   AS     v124      ,
        COUNT(CASE WHEN score_card_sv13 = '25.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv13 is not null THEN loan_id END)  AS     v135      ,
        COUNT(CASE WHEN score_card_sv13 = '27.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv13 is not null THEN loan_id END)  AS     v131      ,
        COUNT(CASE WHEN score_card_sv13 = '19.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv13 is not null THEN loan_id END)  AS     v132      ,
        COUNT(CASE WHEN score_card_sv13 = '10.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv13 is not null THEN loan_id END)  AS     v133      ,
        COUNT(CASE WHEN score_card_sv13 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv13 is not null THEN loan_id END)   AS     v134      ,
        COUNT(CASE WHEN score_card_sv14 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv14 is not null THEN loan_id END)   AS     v141      ,
        COUNT(CASE WHEN score_card_sv14 = '1.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv14 is not null THEN loan_id END)   AS     v142      ,
        COUNT(CASE WHEN score_card_sv14 = '3.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv14 is not null THEN loan_id END)   AS     v143      ,
        COUNT(CASE WHEN score_card_sv14 = '6.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv14 is not null THEN loan_id END)   AS     v144      ,
        COUNT(CASE WHEN score_card_sv14 = '9.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv14 is not null THEN loan_id END)   AS     v145      ,
        COUNT(CASE WHEN score_card_sv15 = '32.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv15 is not null THEN loan_id END)  AS     v151      ,
        COUNT(CASE WHEN score_card_sv15 = '22.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv15 is not null THEN loan_id END)  AS     v152      ,
        COUNT(CASE WHEN score_card_sv15 = '10.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv15 is not null THEN loan_id END)  AS     v153      ,
        COUNT(CASE WHEN score_card_sv15 = '0.0' THEN loan_id END)/COUNT(CASE WHEN score_card_sv15 is not null THEN loan_id END)   AS     v154      
FROM weplay.merged_data
WHERE  score_card_version = '3.3'
AND to_date(application_date) >= '2018-01-26'
AND to_date(application_date) < to_date(CURRENT_TIMESTAMP())
GROUP BY to_date(application_date)
;



-- android CSI详情

DROP TABLE IF EXISTS wp_rpt.android_daily_csi_detail ;

CREATE TABLE wp_rpt.android_daily_csi_detail 
AS
SELECT 
        t1.biz_date                                                      AS   biz_date   ,
        IF(t1.v11<>0,(t1.v11-t2.v11)*LN(t1.v11/t2.v11),null)             AS   v11        ,
        IF(t1.v12<>0,(t1.v12-t2.v12)*LN(t1.v12/t2.v12),null)             AS   v12        , 
        IF(t1.v21<>0,(t1.v21-t2.v21)*LN(t1.v21/t2.v21),null)             AS   v21        ,
        IF(t1.v22<>0,(t1.v22-t2.v22)*LN(t1.v22/t2.v22),null)             AS   v22        ,
        IF(t1.v23<>0,(t1.v23-t2.v23)*LN(t1.v23/t2.v23),null)             AS   v23        ,
        IF(t1.v24<>0,(t1.v24-t2.v24)*LN(t1.v24/t2.v24),null)             AS   v24        ,
        IF(t1.v25<>0,(t1.v25-t2.v25)*LN(t1.v25/t2.v25),null)             AS   v25        ,
        IF(t1.v31<>0,(t1.v31-t2.v31)*LN(t1.v31/t2.v31),null)             AS   v31        ,
        IF(t1.v32<>0,(t1.v32-t2.v32)*LN(t1.v32/t2.v32),null)             AS   v32        ,
        IF(t1.v41<>0,(t1.v41-t2.v41)*LN(t1.v41/t2.v41),null)             AS   v41        ,
        IF(t1.v42<>0,(t1.v42-t2.v42)*LN(t1.v42/t2.v42),null)             AS   v42        ,
        IF(t1.v43<>0,(t1.v43-t2.v43)*LN(t1.v43/t2.v43),null)             AS   v43        ,
        IF(t1.v51<>0,(t1.v51-t2.v51)*LN(t1.v51/t2.v51),null)             AS   v51        ,
        IF(t1.v52<>0,(t1.v52-t2.v52)*LN(t1.v52/t2.v52),null)             AS   v52        ,
        IF(t1.v53<>0,(t1.v53-t2.v53)*LN(t1.v53/t2.v53),null)             AS   v53        ,
        IF(t1.v54<>0,(t1.v54-t2.v54)*LN(t1.v54/t2.v54),null)             AS   v54        ,
        IF(t1.v55<>0,(t1.v55-t2.v55)*LN(t1.v55/t2.v55),null)             AS   v55        ,
        IF(t1.v56<>0,(t1.v56-0)*0,null)                                  AS   v56        , 
        IF(t1.v64<>0,(t1.v64-t2.v64)*LN(t1.v64/t2.v64),null)             AS   v64        ,
        IF(t1.v61<>0,(t1.v61-t2.v61)*LN(t1.v61/t2.v61),null)             AS   v61        ,
        IF(t1.v62<>0,(t1.v62-t2.v62)*LN(t1.v62/t2.v62),null)             AS   v62        ,
        IF(t1.v63<>0,(t1.v63-t2.v63)*LN(t1.v63/t2.v63),null)             AS   v63        ,
        IF(t1.v71<>0,(t1.v71-t2.v71)*LN(t1.v71/t2.v71),null)             AS   v71        ,
        IF(t1.v72<>0,(t1.v72-t2.v72)*LN(t1.v72/t2.v72),null)             AS   v72        ,
        IF(t1.v85<>0,(t1.v85-t2.v85)*LN(t1.v85/t2.v85),null)             AS   v85        ,
        IF(t1.v81<>0,(t1.v81-t2.v81)*LN(t1.v81/t2.v81),null)             AS   v81        ,
        IF(t1.v82<>0,(t1.v82-t2.v82)*LN(t1.v82/t2.v82),null)             AS   v82        ,
        IF(t1.v83<>0,(t1.v83-t2.v83)*LN(t1.v83/t2.v83),null)             AS   v83        ,
        IF(t1.v84<>0,(t1.v84-t2.v84)*LN(t1.v84/t2.v84),null)             AS   v84        ,
        IF(t1.v91<>0,(t1.v91-t2.v91)*LN(t1.v91/t2.v91),null)             AS   v91        ,
        IF(t1.v92<>0,(t1.v92-t2.v92)*LN(t1.v92/t2.v92),null)             AS   v92        ,
        IF(t1.v93<>0,(t1.v93-t2.v93)*LN(t1.v93/t2.v93),null)             AS   v93        ,
        IF(t1.v94<>0,(t1.v94-t2.v94)*LN(t1.v94/t2.v94),null)             AS   v94        ,
        IF(t1.v103<>0,(t1.v103-t2.v103)*LN(t1.v103/t2.v103),null)        AS   v103        ,
        IF(t1.v101<>0,(t1.v101-t2.v101)*LN(t1.v101/t2.v101),null)        AS   v101        ,
        IF(t1.v102<>0,(t1.v102-t2.v102)*LN(t1.v102/t2.v102),null)        AS   v102        ,
        IF(t1.v115<>0,(t1.v115-t2.v115)*LN(t1.v115/t2.v115),null)        AS   v115        ,
        IF(t1.v111<>0,(t1.v111-t2.v111)*LN(t1.v111/t2.v111),null)        AS   v111        ,
        IF(t1.v112<>0,(t1.v112-t2.v112)*LN(t1.v112/t2.v112),null)        AS   v112        ,
        IF(t1.v113<>0,(t1.v113-t2.v113)*LN(t1.v113/t2.v113),null)        AS   v113        ,
        IF(t1.v114<>0,(t1.v114-t2.v114)*LN(t1.v114/t2.v114),null)        AS   v114        ,
        IF(t1.v125<>0,(t1.v125-t2.v125)*LN(t1.v125/t2.v125),null)        AS   v125        ,
        IF(t1.v121<>0,(t1.v121-t2.v121)*LN(t1.v121/t2.v121),null)        AS   v121        ,
        IF(t1.v122<>0,(t1.v122-t2.v122)*LN(t1.v122/t2.v122),null)        AS   v122        ,
        IF(t1.v123<>0,(t1.v123-t2.v123)*LN(t1.v123/t2.v123),null)        AS   v123        ,
        IF(t1.v124<>0,(t1.v124-t2.v124)*LN(t1.v124/t2.v124),null)        AS   v124        ,
        IF(t1.v135<>0,(t1.v135-t2.v135)*LN(t1.v135/t2.v135),null)        AS   v135        ,
        IF(t1.v131<>0,(t1.v131-t2.v131)*LN(t1.v131/t2.v131),null)        AS   v131        ,
        IF(t1.v132<>0,(t1.v132-t2.v132)*LN(t1.v132/t2.v132),null)        AS   v132        ,
        IF(t1.v133<>0,(t1.v133-t2.v133)*LN(t1.v133/t2.v133),null)        AS   v133        ,
        IF(t1.v134<>0,(t1.v134-t2.v134)*LN(t1.v134/t2.v134),null)        AS   v134        ,
        IF(t1.v141<>0,(t1.v141-t2.v141)*LN(t1.v141/t2.v141),null)        AS   v141        ,
        IF(t1.v142<>0,(t1.v142-t2.v142)*LN(t1.v142/t2.v142),null)        AS   v142        ,
        IF(t1.v143<>0,(t1.v143-t2.v143)*LN(t1.v143/t2.v143),null)        AS   v143        ,
        IF(t1.v144<>0,(t1.v144-t2.v144)*LN(t1.v144/t2.v144),null)        AS   v144        ,
        IF(t1.v145<>0,(t1.v145-t2.v145)*LN(t1.v145/t2.v145),null)        AS   v145        ,
        IF(t1.v151<>0,(t1.v151-t2.v151)*LN(t1.v151/t2.v151),null)        AS   v151        ,
        IF(t1.v152<>0,(t1.v152-t2.v152)*LN(t1.v152/t2.v152),null)        AS   v152        ,
        IF(t1.v153<>0,(t1.v153-t2.v153)*LN(t1.v153/t2.v153),null)        AS   v153        ,
        IF(t1.v154<>0,(t1.v154-t2.v154)*LN(t1.v154/t2.v154),null)        AS   v154        
FROM  wp_rpt.android_daily_pct  t1 
LEFT JOIN wp_rpt.android_csi_benchmark  t2  on 1=1 
;



-- 生成最终android的CSI变量监控 

DROP TABLE IF EXISTS wp_rpt.android_daily_csi ;

CREATE TABLE wp_rpt.android_daily_csi 
AS
SELECT 
        biz_date                                                                                                                       AS  biz_date                                  ,
        IF(v11 is null,0,v11)+IF(v12 is null,0,v12)                                                                                    AS  gender                                    ,
        IF(v21 is null,0,v21)+IF(v22 is null,0,v22)+IF(v23 is null,0,v23)+IF(v24 is null,0,v24)+IF(v25 is null,0,v25)                  AS  industry_code                             ,
        IF(v31 is null,0,v31)+IF(v32 is null,0,v32)                                                                                    AS  nfcs_loan_cnt                             ,
        IF(v41 is null,0,v41)+IF(v42 is null,0,v42)+IF(v43 is null,0,v43)                                                              AS  staff_level                               ,
        IF(v51 is null,0,v51)+IF(v52 is null,0,v52)+IF(v53 is null,0,v53)+IF(v54 is null,0,v54)+IF(v55 is null,0,v55)+IF(v56 is null,0,v56) AS  zmxy_score                                ,
        IF(v64 is null,0,v64)+IF(v61 is null,0,v61)+IF(v62 is null,0,v62)+IF(v63 is null,0,v63)                                        AS  cons_total_m12_p_catenum                  ,
        IF(v71 is null,0,v71)+IF(v72 is null,0,v72)                                                                                    AS  smsbill_fix_period_amount_out             ,
        IF(v85 is null,0,v85)+IF(v81 is null,0,v81)+IF(v82 is null,0,v82)+IF(v83 is null,0,v83)+IF(v84 is null,0,v84)                  AS  smslog_one_month_out_cnt                  ,
        IF(v91 is null,0,v91)+IF(v92 is null,0,v92)+IF(v93 is null,0,v93)+IF(v94 is null,0,v94)                                        AS  smslog_total_cnt                          ,
        IF(v103 is null,0,v103)+IF(v101 is null,0,v101)+IF(v102 is null,0,v102)                                                        AS  disb_cnt                                  ,
        IF(v115 is null,0,v115)+IF(v111 is null,0,v111)+IF(v112 is null,0,v112)+IF(v113 is null,0,v113)+IF(v114 is null,0,v114)        AS  midnight_call_cnt_ratio                   ,
        IF(v125 is null,0,v125)+IF(v121 is null,0,v121)+IF(v122 is null,0,v122)+IF(v123 is null,0,v123)+IF(v124 is null,0,v124)        AS  midnight_called_duration_ratio            ,
        IF(v135 is null,0,v135)+IF(v131 is null,0,v131)+IF(v132 is null,0,v132)+IF(v133 is null,0,v133)+IF(v134 is null,0,v134)        AS  brong_applyloan_orgnum                    ,
        IF(v141 is null,0,v141)+IF(v142 is null,0,v142)+IF(v143 is null,0,v143)+IF(v144 is null,0,v144)+IF(v145 is null,0,v145)        AS  device_contact_number                     ,
        IF(v151 is null,0,v151)+IF(v152 is null,0,v152)+IF(v153 is null,0,v153)+IF(v154 is null,0,v154)                                AS  fm_cnid_external_platform_loan_cnt_1month 
FROM wp_rpt.android_daily_csi_detail 
;



-- 生成android ScoreShift_daily_detail

DROP TABLE IF EXISTS  wp_rpt.android_scoreshift_daily_detail  ;

CREATE table wp_rpt.android_scoreshift_daily_detail  
AS
SELECT 
        t1.biz_date                                                AS   biz_date    ,
        abs(t1.v11-t2.v11)*0                                       AS   v11         ,
        (t1.v12-t2.v12)*14                                         AS   v12         ,
        abs(t1.v21-t2.v21)*0                                       AS   v21         ,
        (t1.v22-t2.v22)*4                                          AS   v22         ,
        (t1.v23-t2.v23)*7                                          AS   v23         ,
        (t1.v24-t2.v24)*10                                         AS   v24         ,
        (t1.v25-t2.v25)*5                                          AS   v25         ,
        abs(t1.v31-t2.v31)*0                                       AS   v31         ,
        (t1.v32-t2.v32)*4                                          AS   v32         ,
        abs(t1.v41-t2.v41)*0                                       AS   v41         ,
        (t1.v42-t2.v42)*7                                          AS   v42         ,
        (t1.v43-t2.v43)*17                                         AS   v43         ,
        abs(t1.v51-t2.v51)*0                                       AS   v51         ,
        (t1.v52-t2.v52)*12                                         AS   v52         ,
        (t1.v53-t2.v53)*27                                         AS   v53         ,
        (t1.v54-t2.v54)*41                                         AS   v54         ,
        (t1.v55-t2.v55)*19                                         AS   v55         ,
        (t1.v56-0)*19                                              AS   v56         ,
        abs(t1.v64-t2.v64)*0                                       AS   v64         ,
        (t1.v61-t2.v61)*1                                          AS   v61         ,
        (t1.v62-t2.v62)*2                                          AS   v62         ,
        (t1.v63-t2.v63)*5                                          AS   v63         ,
        abs(t1.v71-t2.v71)*0                                       AS   v71         ,
        (t1.v72-t2.v72)*6                                          AS   v72         ,
        (t1.v85-t2.v85)*2                                          AS   v85         ,
        abs(t1.v81-t2.v81)*0                                       AS   v81         ,
        (t1.v82-t2.v82)*3                                          AS   v82         ,
        (t1.v83-t2.v83)*7                                          AS   v83         ,
        (t1.v84-t2.v84)*10                                         AS   v84         ,
        abs(t1.v91-t2.v91)*0                                       AS   v91         ,
        (t1.v92-t2.v92)*1                                          AS   v92         ,
        (t1.v93-t2.v93)*4                                          AS   v93         ,
        (t1.v94-t2.v94)*7                                          AS   v94         ,
        abs(t1.v103-t2.v103)*0                                     AS   v103         ,
        (t1.v101-t2.v101)*14                                       AS   v101         ,
        (t1.v102-t2.v102)*21                                       AS   v102         ,
        (t1.v115-t2.v115)*10                                       AS   v115         ,
        (t1.v111-t2.v111)*16                                       AS   v111         ,
        (t1.v112-t2.v112)*12                                       AS   v112         ,
        (t1.v113-t2.v113)*4                                        AS   v113         ,
        abs(t1.v114-t2.v114)*0                                     AS   v114         ,
        (t1.v125-t2.v125)*5                                        AS   v125         ,
        (t1.v121-t2.v121)*9                                        AS   v121         ,
        (t1.v122-t2.v122)*4                                        AS   v122         ,
        (t1.v123-t2.v123)*2                                        AS   v123         ,
        abs(t1.v124-t2.v124)*0                                     AS   v124         ,
        (t1.v135-t2.v135)*25                                       AS   v135         ,
        (t1.v131-t2.v131)*27                                       AS   v131         ,
        (t1.v132-t2.v132)*19                                       AS   v132         ,
        (t1.v133-t2.v133)*10                                       AS   v133         ,
        abs(t1.v134-t2.v134)*0                                     AS   v134         ,
        abs(t1.v141-t2.v141)*0                                     AS   v141         ,
        (t1.v142-t2.v142)*1                                        AS   v142         ,
        (t1.v143-t2.v143)*3                                        AS   v143         ,
        (t1.v144-t2.v144)*6                                        AS   v144         ,
        (t1.v145-t2.v145)*9                                        AS   v145         ,
        (t1.v151-t2.v151)*32                                       AS   v151         ,
        (t1.v152-t2.v152)*22                                       AS   v152         ,
        (t1.v153-t2.v153)*10                                       AS   v153         ,
        abs(t1.v154-t2.v154)*0                                     AS   v154         
FROM  wp_rpt.android_daily_pct  t1 
LEFT JOIN wp_rpt.android_csi_benchmark  t2  on 1=1
;



-- 生成android ScoreShift_daily

DROP TABLE IF EXISTS wp_rpt.android_scoreshift_daily ;

CREATE TABLE wp_rpt.android_scoreshift_daily
AS
SELECT 
        biz_date                                                AS  biz_date       ,
        v11+v12                                                 AS  gender         ,
        v21+v22+v23+v24+v25                                     AS  industry_code  ,
        v31+v32                                                 AS  nfcs_loan_cnt  ,
        v41+v42+v43                                             AS  staff_level    ,
        v51+v52+v53+v54+v55+v56                                 AS  zmxy_score     ,
        v64+v61+v62+v63                                         AS  cons_total_m12_p_catenum      ,
        v71+v72                                                 AS  smsbill_fix_period_amount_out ,
        v85+v81+v82+v83+v84                                     AS  smslog_one_month_out_cnt      ,
        v91+v92+v93+v94                                         AS  smslog_total_cnt              ,
        v103+v101+v102                                          AS  disb_cnt                      ,
        v115+v111+v112+v113+v114                                AS  midnight_call_cnt_ratio       ,
        v125+v121+v122+v123+v124                                AS  midnight_called_duration_ratio  ,
        v135+v131+v132+v133+v134                                AS  brong_applyloan_orgnum          ,
        v141+v142+v143+v144+v145                                AS  device_contact_number           ,
        v151+v152+v153+v154                                     AS  fm_cnid_external_platform_loan_cnt_1month       ,
        v11+v12
        +v21+v22+v23+v24+v25
        +v31+v32
        +v41+v42+v43
        +v51+v52+v53+v54+v55+v56
        +v64+v61+v62+v63
        +v71+v72
        +v85+v81+v82+v83+v84
        +v91+v92+v93+v94
        +v103+v101+v102
        +v115+v111+v112+v113+v114
        +v125+v121+v122+v123+v124 
        +v135+v131+v132+v133+v134 
        +v141+v142+v143+v144+v145 
        +v151+v152+v153+v154                                   AS sum_shift
FROM wp_rpt.android_scoreshift_daily_detail  
;




-- IOS
-- IOS 3.2
-- 生成IOS ios_daily_val表

DROP TABLE IF EXISTS wp_rpt.ios_daily_val;

CREATE TABLE wp_rpt.ios_daily_val 
AS
SELECT
        to_date(application_date)                                              AS     biz_date  ,
        COUNT(CASE WHEN score_card_sv1 = '0.0'  THEN loan_id END)              AS     v11       ,
        COUNT(CASE WHEN score_card_sv1 = '11.0' THEN loan_id END)              AS     v12       ,
        COUNT(CASE WHEN score_card_sv2 = '0.0'  THEN loan_id END)              AS     v21       ,
        COUNT(CASE WHEN score_card_sv2 = '18.0' THEN loan_id END)              AS     v22       ,
        COUNT(CASE WHEN score_card_sv3 = '0.0'  THEN loan_id END)              AS     v31       ,
        COUNT(CASE WHEN score_card_sv3 = '7.0'  THEN loan_id END)              AS     v32       ,
        COUNT(CASE WHEN score_card_sv3 = '11.0' THEN loan_id END)              AS     v33       ,
        COUNT(CASE WHEN score_card_sv3 = '19.0' THEN loan_id END)              AS     v34       ,
        COUNT(CASE WHEN score_card_sv3 = '29.0' THEN loan_id END)              AS     v35       ,
        COUNT(CASE WHEN score_card_sv4 = '0.0'  THEN loan_id END)              AS     v41       ,
        COUNT(CASE WHEN score_card_sv4 = '22.0' THEN loan_id END)              AS     v42       ,
        COUNT(CASE WHEN score_card_sv5 = '0.0'  THEN loan_id END)              AS     v51       ,
        COUNT(CASE WHEN score_card_sv5 = '4.0'  THEN loan_id END)              AS     v52       ,
        COUNT(CASE WHEN score_card_sv5 = '11.0' THEN loan_id END)              AS     v53       ,
        COUNT(CASE WHEN score_card_sv6 = '17.0' THEN loan_id END)              AS     v61       ,
        COUNT(CASE WHEN score_card_sv6 = '0.0'  THEN loan_id END)              AS     v62       ,
        COUNT(CASE WHEN score_card_sv7 = '0.0'  THEN loan_id END)              AS     v71       ,
        COUNT(CASE WHEN score_card_sv7 = '8.0'  THEN loan_id END)              AS     v72       ,
        COUNT(CASE WHEN score_card_sv7 = '21.0' THEN loan_id END)              AS     v73       ,
        COUNT(CASE WHEN score_card_sv7 = '34.0' THEN loan_id END)              AS     v74       ,
        COUNT(CASE WHEN score_card_sv7 = '51.0' THEN loan_id END)              AS     v75       ,
        COUNT(CASE WHEN score_card_sv8 = '10.0' and CAST(score_card_v8 as int) < 6 THEN loan_id END)                AS     v81       ,
        COUNT(CASE WHEN score_card_sv8 = '7.0'  THEN loan_id END)              AS     v82       ,
        COUNT(CASE WHEN score_card_sv8 = '5.0'  THEN loan_id END)              AS     v83       ,
        COUNT(CASE WHEN score_card_sv8 = '0.0'  THEN loan_id END)              AS     v84       ,
        COUNT(CASE WHEN score_card_sv8 = '10.0' and LENGTH(score_card_v8) = 0 THEN loan_id END)                     AS     v85       ,
        COUNT(CASE WHEN score_card_sv9 = '9.0'  THEN loan_id END)              AS     v91       ,
        COUNT(CASE WHEN score_card_sv9 = '7.0'  THEN loan_id END)              AS     v92       ,
        COUNT(CASE WHEN score_card_sv9 = '0.0'  THEN loan_id END)              AS     v93       ,
        COUNT(CASE WHEN score_card_sv10 = '18.0' THEN loan_id END)             AS     v101      ,
        COUNT(CASE WHEN score_card_sv10 = '13.0' THEN loan_id END)             AS     v102      ,
        COUNT(CASE WHEN score_card_sv10 = '8.0'  THEN loan_id END)             AS     v103      ,
        COUNT(CASE WHEN score_card_sv10 = '0.0'  THEN loan_id END)             AS     v104      ,
        COUNT(CASE WHEN score_card_sv11 = '0.0'  THEN loan_id END)             AS     v111      ,
        COUNT(CASE WHEN score_card_sv11 = '10.0' THEN loan_id END)             AS     v112      ,
        COUNT(CASE WHEN score_card_sv11 = '17.0' THEN loan_id END)             AS     v113      ,
        COUNT(CASE WHEN score_card_sv12 = '0.0'  THEN loan_id END)             AS     v121      ,
        COUNT(CASE WHEN score_card_sv12 = '4.0'  THEN loan_id END)             AS     v122      ,
        COUNT(CASE WHEN score_card_sv12 = '10.0' THEN loan_id END)             AS     v123      ,
        COUNT(CASE WHEN score_card_sv12 = '16.0' THEN loan_id END)             AS     v124      ,
        COUNT(CASE WHEN score_card_sv13 = '41.0' THEN loan_id END)             AS     v131      ,
        COUNT(CASE WHEN score_card_sv13 = '29.0' THEN loan_id END)             AS     v132      ,
        COUNT(CASE WHEN score_card_sv13 = '19.0' THEN loan_id END)             AS     v133      ,
        COUNT(CASE WHEN score_card_sv13 = '9.0'  THEN loan_id END)             AS     v134      ,
        COUNT(CASE WHEN score_card_sv13 = '0.0'  THEN loan_id END)             AS     v135      
FROM  weplay.merged_data
WHERE  score_card_version = '3.2'
AND self_score_class in ('S1','S2','S3','S4','S5')
AND to_date(application_date) >= '2018-01-26'
AND to_date(application_date) < to_date(CURRENT_TIMESTAMP())
GROUP BY to_date(application_date)
;



-- 生成 IOS ios_daily_pct

DROP TABLE IF EXISTS  wp_rpt.ios_daily_pct  ;

CREATE TABLE wp_rpt.ios_daily_pct  
AS
        SELECT
        biz_date                                              AS  biz_date,
        v11/(v11+v12)                                         AS  v11  ,
        v12/(v11+v12)                                         AS  v12  ,
        v21/(v21+v22)                                         AS  v21  ,
        v22/(v21+v22)                                         AS  v22  ,
        v31/(v31+v32+v33+v34+v35)                             AS  v31  ,
        v32/(v31+v32+v33+v34+v35)                             AS  v32  ,
        v33/(v31+v32+v33+v34+v35)                             AS  v33  ,
        v34/(v31+v32+v33+v34+v35)                             AS  v34  ,
        v35/(v31+v32+v33+v34+v35)                             AS  v35  ,
        v41/(v41+v42)                                         AS  v41  ,
        v42/(v41+v42)                                         AS  v42  ,
        v51/(v51+v52+v53)                                     AS  v51  ,
        v52/(v51+v52+v53)                                     AS  v52  ,
        v53/(v51+v52+v53)                                     AS  v53  ,
        v61/(v61+v62)                                         AS  v61  ,
        v62/(v61+v62)                                         AS  v62  ,
        v71/(v71+v72+v73+v74+v75)                             AS  v71  ,
        v72/(v71+v72+v73+v74+v75)                             AS  v72  ,
        v73/(v71+v72+v73+v74+v75)                             AS  v73  ,
        v74/(v71+v72+v73+v74+v75)                             AS  v74  ,
        v75/(v71+v72+v73+v74+v75)                             AS  v75  ,
        v81/(v81+v82+v83+v84+v85)                             AS  v81  ,
        v82/(v81+v82+v83+v84+v85)                             AS  v82  ,
        v83/(v81+v82+v83+v84+v85)                             AS  v83  ,
        v84/(v81+v82+v83+v84+v85)                             AS  v84  ,
        v85/(v81+v82+v83+v84+v85)                             AS  v85  ,
        v91/(v91+v92+v93)                                     AS  v91  ,
        v92/(v91+v92+v93)                                     AS  v92  ,
        v93/(v91+v92+v93)                                     AS  v93  ,
        v101/(v101+v102+v103+v104)                            AS  v101 ,
        v102/(v101+v102+v103+v104)                            AS  v102 ,
        v103/(v101+v102+v103+v104)                            AS  v103 ,
        v104/(v101+v102+v103+v104)                            AS  v104 ,
        v111/(v111+v112+v113)                                 AS  v111 ,
        v112/(v111+v112+v113)                                 AS  v112 ,
        v113/(v111+v112+v113)                                 AS  v113 ,
        v121/(v121+v122+v123+v124)                            AS  v121 ,
        v122/(v121+v122+v123+v124)                            AS  v122 ,
        v123/(v121+v122+v123+v124)                            AS  v123 ,
        v124/(v121+v122+v123+v124)                            AS  v124 ,
        v131/(v131+v132+v133+v134+v135)                       AS  v131 ,
        v132/(v131+v132+v133+v134+v135)                       AS  v132 ,
        v133/(v131+v132+v133+v134+v135)                       AS  v133 ,
        v134/(v131+v132+v133+v134+v135)                       AS  v134 ,
        v135/(v131+v132+v133+v134+v135)                       AS  v135 
FROM  wp_rpt.ios_daily_val
;




-- IOS CSI详情  ios_daily_csi_detail

DROP TABLE IF EXISTS  wp_rpt.ios_daily_csi_detail ;

CREATE TABLE wp_rpt.ios_daily_csi_detail 
AS
SELECT
        t1.biz_date                                                      AS  biz_date    ,
        IF(t1.v11<>0,(t1.v11-t2.v11)*LN(t1.v11/t2.v11),null)             AS  v11         ,
        IF(t1.v12<>0,(t1.v12-t2.v12)*LN(t1.v12/t2.v12),null)             AS  v12         ,
        IF(t1.v21<>0,(t1.v21-t2.v21)*LN(t1.v21/t2.v21),null)             AS  v21         ,
        IF(t1.v22<>0,(t1.v22-t2.v22)*LN(t1.v22/t2.v22),null)             AS  v22         ,
        IF(t1.v31<>0,(t1.v31-t2.v31)*LN(t1.v31/t2.v31),null)             AS  v31         ,
        IF(t1.v32<>0,(t1.v32-t2.v32)*LN(t1.v32/t2.v32),null)             AS  v32         ,
        IF(t1.v33<>0,(t1.v33-t2.v33)*LN(t1.v33/t2.v33),null)             AS  v33         ,
        IF(t1.v34<>0,(t1.v34-t2.v34)*LN(t1.v34/t2.v34),null)             AS  v34         ,
        IF(t1.v35<>0,(t1.v35-t2.v35)*LN(t1.v35/t2.v35),null)             AS  v35         ,
        IF(t1.v41<>0,(t1.v41-t2.v41)*LN(t1.v41/t2.v41),null)             AS  v41         ,
        IF(t1.v42<>0,(t1.v42-t2.v42)*LN(t1.v42/t2.v42),null)             AS  v42         ,
        IF(t1.v51<>0,(t1.v51-t2.v51)*LN(t1.v51/t2.v51),null)             AS  v51         ,
        IF(t1.v52<>0,(t1.v52-t2.v52)*LN(t1.v52/t2.v52),null)             AS  v52         ,
        IF(t1.v53<>0,(t1.v53-t2.v53)*LN(t1.v53/t2.v53),null)             AS  v53         ,
        IF(t1.v61<>0,(t1.v61-t2.v61)*LN(t1.v61/t2.v61),null)             AS  v61         ,
        IF(t1.v62<>0,(t1.v62-t2.v62)*LN(t1.v62/t2.v62),null)             AS  v62         ,
        IF(t1.v71<>0,(t1.v71-t2.v71)*LN(t1.v71/t2.v71),null)             AS  v71         ,
        IF(t1.v72<>0,(t1.v72-t2.v72)*LN(t1.v72/t2.v72),null)             AS  v72         ,
        IF(t1.v73<>0,(t1.v73-t2.v73)*LN(t1.v73/t2.v73),null)             AS  v73         ,
        IF(t1.v74<>0,(t1.v74-t2.v74)*LN(t1.v74/t2.v74),null)             AS  v74         ,
        IF(t1.v75<>0,(t1.v75-t2.v75)*LN(t1.v75/t2.v75),null)             AS  v75         ,
        IF(t1.v81<>0,(t1.v81-t2.v81)*LN(t1.v81/t2.v81),null)             AS  v81         ,
        IF(t1.v82<>0,(t1.v82-t2.v82)*LN(t1.v82/t2.v82),null)             AS  v82         ,
        IF(t1.v83<>0,(t1.v83-t2.v83)*LN(t1.v83/t2.v83),null)             AS  v83         ,
        IF(t1.v84<>0,(t1.v84-t2.v84)*LN(t1.v84/t2.v84),null)             AS  v84         ,
        IF(t1.v85<>0,(t1.v85-t2.v85)*LN(t1.v85/t2.v85),null)             AS  v85         ,
        IF(t1.v91<>0,(t1.v91-t2.v91)*LN(t1.v91/t2.v91),null)             AS  v91         ,
        IF(t1.v92<>0,(t1.v92-t2.v92)*LN(t1.v92/t2.v92),null)             AS  v92         ,
        IF(t1.v93<>0,(t1.v93-t2.v93)*LN(t1.v93/t2.v93),null)             AS  v93         ,
        IF(t1.v101<>0,(t1.v101-t2.v101)*LN(t1.v101/t2.v101),null)        AS  v101        ,
        IF(t1.v102<>0,(t1.v102-t2.v102)*LN(t1.v102/t2.v102),null)        AS  v102        ,
        IF(t1.v103<>0,(t1.v103-t2.v103)*LN(t1.v103/t2.v103),null)        AS  v103        ,
        IF(t1.v104<>0,(t1.v104-t2.v104)*LN(t1.v104/t2.v104),null)        AS  v104        ,
        IF(t1.v111<>0,(t1.v111-t2.v111)*LN(t1.v111/t2.v111),null)        AS  v111        ,
        IF(t1.v112<>0,(t1.v112-t2.v112)*LN(t1.v112/t2.v112),null)        AS  v112        ,
        IF(t1.v113<>0,(t1.v113-t2.v113)*LN(t1.v113/t2.v113),null)        AS  v113        ,
        IF(t1.v121<>0,(t1.v121-t2.v121)*LN(t1.v121/t2.v121),null)        AS  v121        ,
        IF(t1.v122<>0,(t1.v122-t2.v122)*LN(t1.v122/t2.v122),null)        AS  v122        ,
        IF(t1.v123<>0,(t1.v123-t2.v123)*LN(t1.v123/t2.v123),null)        AS  v123        ,
        IF(t1.v124<>0,(t1.v124-t2.v124)*LN(t1.v124/t2.v124),null)        AS  v124        ,
        IF(t1.v131<>0,(t1.v131-t2.v131)*LN(t1.v131/t2.v131),null)        AS  v131        ,
        IF(t1.v132<>0,(t1.v132-t2.v132)*LN(t1.v132/t2.v132),null)        AS  v132        ,
        IF(t1.v133<>0,(t1.v133-t2.v133)*LN(t1.v133/t2.v133),null)        AS  v133        ,
        IF(t1.v134<>0,(t1.v134-t2.v134)*LN(t1.v134/t2.v134),null)        AS  v134        ,
        IF(t1.v135<>0,(t1.v135-t2.v135)*LN(t1.v135/t2.v135),null)        AS  v135        
FROM  wp_rpt.ios_daily_pct  t1
LEFT JOIN  wp_rpt.ios_csi_benchmark t2   on  1 = 1
;




-- 生成IOS最终 CSI报表 

DROP TABLE IF EXISTS wp_rpt.ios_daily_csi ;

CREATE TABLE wp_rpt.ios_daily_csi  
AS
SELECT
        biz_date                                                                                                                    AS  biz_date                                ,
        IF(v11 is null,0,v11)+IF(v12 is null,0,v12)                                                                                 AS  alipay_company_address_matched          ,
        IF(v21 is null,0,v21)+IF(v22 is null,0,v22)                                                                                 AS  gender                                  ,
        IF(v31 is null,0,v31)+IF(v32 is null,0,v32)+IF(v33 is null,0,v33)+IF(v34 is null,0,v34)+IF(v35 is null,0,v35)               AS  industry_code                           ,
        IF(v41 is null,0,v41)+IF(v42 is null,0,v42)                                                                                 AS  is_good_old_customer                    ,
        IF(v51 is null,0,v51)+IF(v52 is null,0,v52)+IF(v53 is null,0,v53)                                                           AS  nfcs_loan_cnt                           ,
        IF(v61 is null,0,v61)+IF(v62 is null,0,v62)                                                                                 AS  pb_match_liainson_update_in_7_days_cnt  ,
        IF(v71 is null,0,v71)+IF(v72 is null,0,v72)+IF(v73 is null,0,v73)+IF(v74 is null,0,v74)+IF(v75 is null,0,v75)               AS  zmxy_score                              ,
        IF(v81 is null,0,v81)+IF(v82 is null,0,v82)+IF(v83 is null,0,v83)+IF(v84 is null,0,v84)+IF(v85 is null,0,v85)               AS  call_weekday_leisuretime_quarter        ,
        IF(v91 is null,0,v91)+IF(v92 is null,0,v92)+IF(v93 is null,0,v93)                                                           AS  call_weekend_sleeptime_quarter          ,
        IF(v101 is null,0,v101)+IF(v102 is null,0,v102)+IF(v103 is null,0,v103)+IF(v104 is null,0,v104)                             AS  liason_rank_max                         ,
        IF(v111 is null,0,v111)+IF(v112 is null,0,v112)+IF(v113 is null,0,v113)                                                     AS  py_degree                               ,
        IF(v121 is null,0,v121)+IF(v122 is null,0,v122)+IF(v123 is null,0,v123)+IF(v124 is null,0,v124)                             AS  alipay_ant_available_amount             ,
        IF(v131 is null,0,v131)+IF(v132 is null,0,v132)+IF(v133 is null,0,v133)+IF(v134 is null,0,v134)+IF(v135 is null,0,v135)     AS  fm_cnid_external_platform_loan_cnt 
FROM wp_rpt.ios_daily_csi_detail  
;



-- 生成IOS ScoreShift_daily_detail

DROP TABLE IF EXISTS wp_rpt.ios_scoreshift_daily_detail ;

CREATE TABLE  wp_rpt.ios_scoreshift_daily_detail  
AS
SELECT
        t1.biz_date                                           AS  biz_date   ,
        ABS(t1.v11-t2.v11)*0                                  AS  v11        ,
        (t1.v12-t2.v12)*11                                    AS  v12        ,
        ABS(t1.v21-t2.v21)*0                                  AS  v21        ,
        (t1.v22-t2.v22)*18                                    AS  v22        ,
        ABS(t1.v31-t2.v31)*0                                  AS  v31        ,
        (t1.v32-t2.v32)*7                                     AS  v32        ,
        (t1.v33-t2.v33)*11                                    AS  v33        ,
        (t1.v34-t2.v34)*19                                    AS  v34        ,
        (t1.v35-t2.v35)*29                                    AS  v35        ,
        ABS(t1.v41-t2.v41)*0                                  AS  v41        ,
        (t1.v42-t2.v42)*22                                    AS  v42        ,
        ABS(t1.v51-t2.v51)*0                                  AS  v51        ,
        (t1.v52-t2.v52)*4                                     AS  v52        ,
        (t1.v53-t2.v53)*11                                    AS  v53        ,
        (t1.v61-t2.v61)*17                                    AS  v61        ,
        ABS(t1.v62-t2.v62)*0                                  AS  v62        ,
        ABS(t1.v71-t2.v71)*0                                  AS  v71        ,
        (t1.v72-t2.v72)*8                                     AS  v72        ,
        (t1.v73-t2.v73)*21                                    AS  v73        ,
        (t1.v74-t2.v74)*34                                    AS  v74        ,
        (t1.v75-t2.v75)*51                                    AS  v75        ,
        (t1.v81-t2.v81)*10                                    AS  v81        ,
        (t1.v82-t2.v82)*7                                     AS  v82        ,
        (t1.v83-t2.v83)*5                                     AS  v83        ,
        ABS(t1.v84-t2.v84)*0                                  AS  v84        ,
        (t1.v85-t2.v85)*10                                    AS  v85        ,
        (t1.v91-t2.v91)*9                                     AS  v91        ,
        (t1.v92-t2.v92)*7                                     AS  v92        ,
        ABS(t1.v93-t2.v93)*0                                  AS  v93        ,
        (t1.v101-t2.v101)*18                                  AS  v101       ,
        (t1.v102-t2.v102)*13                                  AS  v102       ,
        (t1.v103-t2.v103)*8                                   AS  v103       ,
        ABS(t1.v104-t2.v104)*0                                AS  v104       ,
        ABS(t1.v111-t2.v111)*0                                AS  v111       ,
        (t1.v112-t2.v112)*10                                  AS  v112       ,
        (t1.v113-t2.v113)*17                                  AS  v113       ,
        ABS(t1.v121-t2.v121)*0                                AS  v121       ,
        (t1.v122-t2.v122)*4                                   AS  v122       ,
        (t1.v123-t2.v123)*10                                  AS  v123       ,
        (t1.v124-t2.v124)*16                                  AS  v124       ,
        (t1.v131-t2.v131)*41                                  AS  v131       ,
        (t1.v132-t2.v132)*29                                  AS  v132       ,
        (t1.v133-t2.v133)*19                                  AS  v133       ,
        (t1.v134-t2.v134)*9                                   AS  v134       ,
        ABS(t1.v135-t2.v135)*0                                AS  v135       
FROM  wp_rpt.ios_daily_pct  t1
LEFT JOIN  wp_rpt.ios_csi_benchmark  t2  on 1 = 1
;




-- 生成ios ScoreShift_daily

DROP TABLE IF EXISTS  wp_rpt.ios_scoreshift_daily ;

CREATE TABLE wp_rpt.ios_scoreshift_daily 
AS
SELECT
        biz_date                                    AS  biz_date                                ,
        v11+v12                                     AS  alipay_company_address_matched          , 
        v21+v22                                     AS  gender                                  ,
        v31+v32+v33+v34+v35                         AS  industry_code                           ,
        v41+v42                                     AS  is_good_old_customer                    ,
        v51+v52+v53                                 AS  nfcs_loan_cnt                           ,
        v61+v62                                     AS  pb_match_liainson_update_in_7_days_cnt  ,
        v71+v72+v73+v74+v75                         AS  zmxy_score                              ,
        v81+v82+v83+v84+v85                         AS  call_weekday_leisuretime_quarter        ,
        v91+v92+v93                                 AS  call_weekend_sleeptime_quarter          ,
        v101+v102+v103+v104                         AS  liason_rank_max                         ,
        v111+v112+v113                              AS  py_degree                               ,
        v121+v122+v123+v124                         AS  alipay_ant_available_amount             ,
        v131+v132+v133+v134+v135                    AS  fm_cnid_external_platform_loan_cnt      ,
        v11+v12
        +v21+v22
        +v31+v32+v33+v34+v35
        +v41+v42
        +v51+v52+v53
        +v61+v62
        +v71+v72+v73+v74+v75
        +v81+v82+v83+v84+v85
        +v91+v92+v93
        +v101+v102+v103+v104
        +v111+v112+v113
        +v121+v122+v123+v124
        +v131+v132+v133+v134+v135                    AS sum_shift
FROM wp_rpt.ios_scoreshift_daily_detail
;



-- H5移动
-- H5 移动6.0 需特殊处理
-- 由于，merged_date中只有SV1总分 通过bairongdata 计算得出各个变量的分数

-- 数据插入bairong_data_csi

DROP TABLE IF EXISTS  wp_rpt.bairong_data_csi  ;

CREATE TABLE wp_rpt.bairong_data_csi STORED AS PARQUET
AS
SELECT 
id                                     AS   id       ,
account                                AS   account  ,
cnid                                   AS   cnid     ,
FROM_UTC_TIMESTAMP(TO_TIMESTAMP(cast(logtime as BIGINT)), 'PRC')        AS   log_time ,
score.scorecust                        AS    score_scorecust                      ,
applyloanstr.m12.id.max_monnum         AS    applyloanstr_m12_id_max_monnum       ,
applyloanstr.m3.id.nbank.oth_allnum    AS    applyloanstr_m3_id_nbank_oth_allnum  ,
consumption_c.`continue`               AS    consumption_c_continue               ,         
stability_c.mail.`number`              AS    stability_c_mail_number            
FROM   wp_ods.bairong_data_csi 
;
 


DROP TABLE IF EXISTS wp_rpt.bairong_data_csi_cal ;

CREATE TABLE  wp_rpt.bairong_data_csi_cal  STORED AS PARQUET
AS
SELECT 
        t2.id                                                                                             AS    id               ,
        t2.account                                                                                        AS    account          ,
        t2.cnid                                                                                           AS    cnid             ,
        t2.log_time                                                                                       AS    log_time         ,
        round(CAST(t2.score_scorecust as decimal(10,4)),2)                                                AS    score_scorecust  ,
        CASE WHEN CAST(t2.applyloanstr_m12_id_max_monnum as DOUBLE) < 2 
                    OR  t2.applyloanstr_m12_id_max_monnum   IS NULL 
                    OR  LENGTH(t2.applyloanstr_m12_id_max_monnum) = 0 
                    THEN  28.1 
            WHEN  CAST(t2.applyloanstr_m12_id_max_monnum as DOUBLE) >=2
                        AND CAST(t2.applyloanstr_m12_id_max_monnum as DOUBLE) <3
                        THEN 22.09
            WHEN  CAST(t2.applyloanstr_m12_id_max_monnum as DOUBLE) >=3
                        AND CAST(t2.applyloanstr_m12_id_max_monnum as DOUBLE) <5
                        THEN 18.78
            WHEN  CAST(t2.applyloanstr_m12_id_max_monnum as DOUBLE) >=5
                        AND CAST(t2.applyloanstr_m12_id_max_monnum as DOUBLE) <6
                        THEN 13.78
            WHEN  CAST(t2.applyloanstr_m12_id_max_monnum as DOUBLE) >=6
                        AND CAST(t2.applyloanstr_m12_id_max_monnum as DOUBLE) <8
                        THEN 9.42
            WHEN  CAST(t2.applyloanstr_m12_id_max_monnum as DOUBLE) >=8
                        THEN -0.57
            END                                                                                          AS    applyloanstr_m12_id_max_monnum         ,
                    
        CASE WHEN t2.applyloanstr_m3_id_nbank_oth_allnum  IS NULL       
                OR LENGTH(t2.applyloanstr_m3_id_nbank_oth_allnum) = 0       
                        THEN 22.64      
            WHEN CAST(t2.applyloanstr_m3_id_nbank_oth_allnum AS DOUBLE) <= 1        
                        THEN 15.03      
            WHEN CAST(t2.applyloanstr_m3_id_nbank_oth_allnum AS DOUBLE) > 1     
                        THEN 10.31      
            END                                                                                          AS    applyloanstr_m3_id_nbank_oth_allnum   ,
        CASE WHEN t2.consumption_c_continue = 'M5'      
                OR  t2.consumption_c_continue IS NULL       
                OR  LENGTH(t2.consumption_c_continue) = 0       
                        THEN 15.94      
            WHEN t2.consumption_c_continue IN ('M1','M2','M3')      
                        THEN 21.03      
            WHEN t2.consumption_c_continue = 'M4'       
                        THEN 27.78      
            END                                                                                          AS    consumption_c_continue   ,
        CASE WHEN t2.stability_c_mail_number IS NULL        
                OR LENGTH(t2.stability_c_mail_number) = 0       
                        THEN 17.91      
            WHEN CAST(t2.stability_c_mail_number AS DOUBLE) < 1     
                        THEN 19.83      
            WHEN CAST(t2.stability_c_mail_number AS DOUBLE) >= 1        
                AND CAST(t2.stability_c_mail_number AS DOUBLE) < 2      
                        THEN 22.25      
            WHEN CAST(t2.stability_c_mail_number AS DOUBLE) >= 2        
                        THEN 24.37      
            END                                                                                          AS    stability_c_mail_number  
FROM
(
SELECT TT.* FROM
(SELECT *,Row_Number() over(partition by a1.account,a1.score_scorecust ORDER BY a1.log_time DESC) AS RANK
FROM wp_rpt.bairong_data_csi a1
) TT
 WHERE TT.RANK =1 
) t2 
where t2.score_scorecust is not null  and  to_date(t2.log_time) >= '2018-01-26'  and LENGTH(t2.score_scorecust) > 0
;


-- 形成最终bairong csi 数据表

DROP TABLE IF EXISTS wp_rpt.bairong_data_csi_final ;

CREATE TABLE wp_rpt.bairong_data_csi_final  STORED AS PARQUET
AS
SELECT 
        t1.id                                                                                                    AS    id          ,
        t1.account                                                                                               AS    account     ,
        t1.cnid                                                                                                  AS    cnid        ,
        t1.log_time                                                                                              AS    log_time    ,
        t1.score_scorecust                                                                                       AS    score_scorecust  ,
        t1.applyloanstr_m12_id_max_monnum                                                                        AS    applyloanstr_m12_id_max_monnum  ,
        t1.applyloanstr_m3_id_nbank_oth_allnum                                                                   AS    applyloanstr_m3_id_nbank_oth_allnum ,
        t1.consumption_c_continue                                                                                AS    consumption_c_continue  ,
        round((t1.score_scorecust - t1.applyloanstr_m12_id_max_monnum - t1.applyloanstr_m3_id_nbank_oth_allnum - t1.consumption_c_continue
        - t1.stability_c_mail_number),2)                                                                         AS    netshop_eb_amt_avg   ,
        t1.stability_c_mail_number                                                                               AS    stability_c_mail_number
FROM wp_rpt.bairong_data_csi_cal  t1
;



-- 生成H5移动 变量基础表  weplay_tmp.h5_mobile_daily_val

DROP TABLE IF EXISTS wp_rpt.h5_mobile_daily_val ;

CREATE TABLE wp_rpt.h5_mobile_daily_val 
AS
SELECT
        to_date(t1.application_date)                                                                AS  biz_date ,
        COUNT(CASE WHEN t2.applyloanstr_m12_id_max_monnum = 28.1  THEN t1.loan_id END)              AS  v11      ,
        COUNT(CASE WHEN t2.applyloanstr_m12_id_max_monnum = 22.09 THEN t1.loan_id END)              AS  v12      ,
        COUNT(CASE WHEN t2.applyloanstr_m12_id_max_monnum = 18.78 THEN t1.loan_id END)              AS  v13      ,
        COUNT(CASE WHEN t2.applyloanstr_m12_id_max_monnum = 13.78 THEN t1.loan_id END)              AS  v14      ,
        COUNT(CASE WHEN t2.applyloanstr_m12_id_max_monnum = 9.42  THEN t1.loan_id END)              AS  v15      ,
        COUNT(CASE WHEN t2.applyloanstr_m12_id_max_monnum = -0.57 THEN t1.loan_id END)              AS  v16      ,
        COUNT(CASE WHEN t2.applyloanstr_m3_id_nbank_oth_allnum = 22.64 THEN t1.loan_id END)         AS  v21      ,
        COUNT(CASE WHEN t2.applyloanstr_m3_id_nbank_oth_allnum = 15.03 THEN t1.loan_id END)         AS  v22      ,
        COUNT(CASE WHEN t2.applyloanstr_m3_id_nbank_oth_allnum = 10.31 THEN t1.loan_id END)         AS  v23      ,
        COUNT(CASE WHEN t2.consumption_c_continue = 15.94 THEN t1.loan_id END)                      AS  v31      ,
        COUNT(CASE WHEN t2.consumption_c_continue = 21.03 THEN t1.loan_id END)                      AS  v32      ,
        COUNT(CASE WHEN t2.consumption_c_continue = 27.78 THEN t1.loan_id END)                      AS  v33      ,
        COUNT(CASE WHEN t2.netshop_eb_amt_avg = 14.38 THEN t1.loan_id END)                          AS  v41      ,
        COUNT(CASE WHEN t2.netshop_eb_amt_avg = 18.24 THEN t1.loan_id END)                          AS  v42      ,
        COUNT(CASE WHEN t2.netshop_eb_amt_avg = 21.9  THEN t1.loan_id END)                          AS  v43      ,
        COUNT(CASE WHEN t2.netshop_eb_amt_avg = 26.85 THEN t1.loan_id END)                          AS  v44      ,
        COUNT(CASE WHEN t2.netshop_eb_amt_avg = 33.9  THEN t1.loan_id END)                          AS  v45      ,
        COUNT(CASE WHEN t2.netshop_eb_amt_avg = 40.84 THEN t1.loan_id END)                          AS  v46      ,
        COUNT(CASE WHEN t2.stability_c_mail_number = 17.91 THEN t1.loan_id END)                     AS  v51      ,
        COUNT(CASE WHEN t2.stability_c_mail_number = 19.83 THEN t1.loan_id END)                     AS  v52      ,
        COUNT(CASE WHEN t2.stability_c_mail_number = 22.25 THEN t1.loan_id END)                     AS  v53      ,
        COUNT(CASE WHEN t2.stability_c_mail_number = 24.37 THEN t1.loan_id END)                     AS  v54      ,
        COUNT(CASE WHEN t1.score_card_sv2 = '17.26' THEN t1.loan_id END)                            AS  v61      ,
        COUNT(CASE WHEN t1.score_card_sv2 = '-1.78' THEN t1.loan_id END)                            AS  v62      ,
        COUNT(CASE WHEN t1.score_card_sv2 = '9.09'  THEN t1.loan_id END)                            AS  v63      ,
        COUNT(CASE WHEN t1.score_card_sv2 = '15.41' THEN t1.loan_id END)                            AS  v64      ,
        COUNT(CASE WHEN t1.score_card_sv2 = '20.49' THEN t1.loan_id END)                            AS  v65      ,
        COUNT(CASE WHEN t1.score_card_sv2 = '29.63' THEN t1.loan_id END)                            AS  v66      ,
        COUNT(CASE WHEN t1.score_card_sv2 = '35.7'  THEN t1.loan_id END)                            AS  v67      ,
        COUNT(CASE WHEN t1.score_card_sv2 = '44.3'  THEN t1.loan_id END)                            AS  v68      ,
        COUNT(CASE WHEN t1.score_card_sv2 = '20.0'  THEN t1.loan_id END)                            AS  v69      ,
        COUNT(CASE WHEN t1.score_card_sv3 = '22.81' THEN t1.loan_id END)                            AS  v71      ,
        COUNT(CASE WHEN t1.score_card_sv3 = '32.23' THEN t1.loan_id END)                            AS  v72      ,
        COUNT(CASE WHEN t1.score_card_sv3 = '23.93' THEN t1.loan_id END)                            AS  v73      ,
        COUNT(CASE WHEN t1.score_card_sv3 = '21.31' THEN t1.loan_id END)                            AS  v74      ,
        COUNT(CASE WHEN t1.score_card_sv3 = '19.08' THEN t1.loan_id END)                            AS  v75      ,
        COUNT(CASE WHEN t1.score_card_sv3 = '17.85' THEN t1.loan_id END)                            AS  v76      ,
        COUNT(CASE WHEN t1.score_card_sv3 = '15.35' THEN t1.loan_id END)                            AS  v77      ,
        COUNT(CASE WHEN t1.score_card_sv3 = '12.48' THEN t1.loan_id END)                            AS  v78      ,
        COUNT(CASE WHEN t1.score_card_sv4 = '25.69' THEN t1.loan_id END)                            AS  v81      ,
        COUNT(CASE WHEN t1.score_card_sv4 = '43.87' THEN t1.loan_id END)                            AS  v82      ,
        COUNT(CASE WHEN t1.score_card_sv4 = '26.61' THEN t1.loan_id END)                            AS  v83      ,
        COUNT(CASE WHEN t1.score_card_sv4 = '21.96' THEN t1.loan_id END)                            AS  v84      ,
        COUNT(CASE WHEN t1.score_card_sv4 = '18.13' THEN t1.loan_id END)                            AS  v85      ,
        COUNT(CASE WHEN t1.score_card_sv4 = '15.36' THEN t1.loan_id END)                            AS  v86      ,
        COUNT(CASE WHEN t1.score_card_sv4 = '12.12' THEN t1.loan_id END)                            AS  v87      ,
        COUNT(CASE WHEN t1.score_card_sv4 = '7.81'  THEN t1.loan_id END)                            AS  v88      ,
        COUNT(CASE WHEN t1.score_card_sv4 = '1.57'  THEN t1.loan_id END)                            AS  v89

FROM  weplay.merged_data  t1  
LEFT JOIN  wp_rpt.bairong_data_csi_final t2  ON  t1.account = t2.account  AND  CAST(CAST(t1.score_card_sv1 AS decimal(9,2)) *100 as INT) = CAST(t2.score_scorecust *100  as INT)
WHERE  t1.score_card_version = '6.0'
AND t1.self_score_class in ('S1','S2','S3','S4','S5')
AND to_date(t1.application_date) >= '2018-01-26'
AND to_date(t1.application_date) < to_date(CURRENT_TIMESTAMP())
GROUP BY to_date(t1.application_date)
;




-- 生成H5移动  weplay_tmp.h5_mobile_daily_pct

DROP TABLE IF EXISTS  wp_rpt.h5_mobile_daily_pct ;

CREATE TABLE wp_rpt.h5_mobile_daily_pct
AS
SELECT
        biz_date                                                  AS  biz_date ,
        v11/(v11+v12+v13+v14+v15+v16)                             AS  v11      ,
        v12/(v11+v12+v13+v14+v15+v16)                             AS  v12      ,
        v13/(v11+v12+v13+v14+v15+v16)                             AS  v13      ,
        v14/(v11+v12+v13+v14+v15+v16)                             AS  v14      ,
        v15/(v11+v12+v13+v14+v15+v16)                             AS  v15      ,
        v16/(v11+v12+v13+v14+v15+v16)                             AS  v16      ,
        v21/(v21+v22+v23)                                         AS  v21      ,
        v22/(v21+v22+v23)                                         AS  v22      ,
        v23/(v21+v22+v23)                                         AS  v23      ,
        v31/(v31+v32+v33)                                         AS  v31      ,
        v32/(v31+v32+v33)                                         AS  v32      ,
        v33/(v31+v32+v33)                                         AS  v33      ,
        v41/(v41+v42+v43+v44+v45+v46)                             AS  v41      ,
        v42/(v41+v42+v43+v44+v45+v46)                             AS  v42      ,
        v43/(v41+v42+v43+v44+v45+v46)                             AS  v43      ,
        v44/(v41+v42+v43+v44+v45+v46)                             AS  v44      ,
        v45/(v41+v42+v43+v44+v45+v46)                             AS  v45      ,
        v46/(v41+v42+v43+v44+v45+v46)                             AS  v46      ,
        v51/(v51+v52+v53+v54)                                     AS  v51      ,
        v52/(v51+v52+v53+v54)                                     AS  v52      ,
        v53/(v51+v52+v53+v54)                                     AS  v53      ,
        v54/(v51+v52+v53+v54)                                     AS  v54      ,
        v61/(v61+v62+v63+v64+v65+v66+v67+v68+v69)                 AS  v61      ,
        v62/(v61+v62+v63+v64+v65+v66+v67+v68+v69)                 AS  v62      ,
        v63/(v61+v62+v63+v64+v65+v66+v67+v68+v69)                 AS  v63      ,
        v64/(v61+v62+v63+v64+v65+v66+v67+v68+v69)                 AS  v64      ,
        v65/(v61+v62+v63+v64+v65+v66+v67+v68+v69)                 AS  v65      ,
        v66/(v61+v62+v63+v64+v65+v66+v67+v68+v69)                 AS  v66      ,
        v67/(v61+v62+v63+v64+v65+v66+v67+v68+v69)                 AS  v67      ,
        v68/(v61+v62+v63+v64+v65+v66+v67+v68+v69)                 AS  v68      ,
        v69/(v61+v62+v63+v64+v65+v66+v67+v68+v69)                 AS  v69      ,
        v71/(v71+v72+v73+v74+v75+v76+v77+v78)                     AS  v71      ,
        v72/(v71+v72+v73+v74+v75+v76+v77+v78)                     AS  v72      ,
        v73/(v71+v72+v73+v74+v75+v76+v77+v78)                     AS  v73      ,
        v74/(v71+v72+v73+v74+v75+v76+v77+v78)                     AS  v74      ,
        v75/(v71+v72+v73+v74+v75+v76+v77+v78)                     AS  v75      ,
        v76/(v71+v72+v73+v74+v75+v76+v77+v78)                     AS  v76      ,
        v77/(v71+v72+v73+v74+v75+v76+v77+v78)                     AS  v77      ,
        v78/(v71+v72+v73+v74+v75+v76+v77+v78)                     AS  v78      ,
        v81/(v81+v82+v83+v84+v85+v86+v87+v88+v89)                 AS  v81      ,
        v82/(v81+v82+v83+v84+v85+v86+v87+v88+v89)                 AS  v82      ,
        v83/(v81+v82+v83+v84+v85+v86+v87+v88+v89)                 AS  v83      ,
        v84/(v81+v82+v83+v84+v85+v86+v87+v88+v89)                 AS  v84      ,
        v85/(v81+v82+v83+v84+v85+v86+v87+v88+v89)                 AS  v85      ,
        v86/(v81+v82+v83+v84+v85+v86+v87+v88+v89)                 AS  v86      ,
        v87/(v81+v82+v83+v84+v85+v86+v87+v88+v89)                 AS  v87      ,
        v88/(v81+v82+v83+v84+v85+v86+v87+v88+v89)                 AS  v88      ,
        v89/(v81+v82+v83+v84+v85+v86+v87+v88+v89)                 AS  v89      
FROM wp_rpt.h5_mobile_daily_val
;



-- 生成H5移动 weplay_tmp.h5_mobile_daily_csi_detail

DROP TABLE IF EXISTS wp_rpt.h5_mobile_daily_csi_detail ;

CREATE  TABLE wp_rpt.h5_mobile_daily_csi_detail 
AS
SELECT
        t1.biz_date                                               AS  biz_date  ,
        IF(t1.v11<>0,(t1.v11-t2.v11)*LN(t1.v11/t2.v11),null)      AS  v11       ,
        IF(t1.v12<>0,(t1.v12-t2.v12)*LN(t1.v12/t2.v12),null)      AS  v12       ,
        IF(t1.v13<>0,(t1.v13-t2.v13)*LN(t1.v13/t2.v13),null)      AS  v13       ,
        IF(t1.v14<>0,(t1.v14-t2.v14)*LN(t1.v14/t2.v14),null)      AS  v14       ,
        IF(t1.v15<>0,(t1.v15-t2.v15)*LN(t1.v15/t2.v15),null)      AS  v15       ,
        IF(t1.v16<>0,(t1.v16-t2.v16)*LN(t1.v16/t2.v16),null)      AS  v16       ,
        IF(t1.v21<>0,(t1.v21-t2.v21)*LN(t1.v21/t2.v21),null)      AS  v21       ,
        IF(t1.v22<>0,(t1.v22-t2.v22)*LN(t1.v22/t2.v22),null)      AS  v22       ,
        IF(t1.v23<>0,(t1.v23-t2.v23)*LN(t1.v23/t2.v23),null)      AS  v23       ,
        IF(t1.v31<>0,(t1.v31-t2.v31)*LN(t1.v31/t2.v31),null)      AS  v31       ,
        IF(t1.v32<>0,(t1.v32-t2.v32)*LN(t1.v32/t2.v32),null)      AS  v32       ,
        IF(t1.v33<>0,(t1.v33-t2.v33)*LN(t1.v33/t2.v33),null)      AS  v33       ,
        IF(t1.v41<>0,(t1.v41-t2.v41)*LN(t1.v41/t2.v41),null)      AS  v41       ,
        IF(t1.v42<>0,(t1.v42-t2.v42)*LN(t1.v42/t2.v42),null)      AS  v42       ,
        IF(t1.v43<>0,(t1.v43-t2.v43)*LN(t1.v43/t2.v43),null)      AS  v43       ,
        IF(t1.v44<>0,(t1.v44-t2.v44)*LN(t1.v44/t2.v44),null)      AS  v44       ,
        IF(t1.v45<>0,(t1.v45-t2.v45)*LN(t1.v45/t2.v45),null)      AS  v45       ,
        IF(t1.v46<>0,(t1.v46-t2.v46)*LN(t1.v46/t2.v46),null)      AS  v46       ,
        IF(t1.v51<>0,(t1.v51-t2.v51)*LN(t1.v51/t2.v51),null)      AS  v51       ,
        IF(t1.v52<>0,(t1.v52-t2.v52)*LN(t1.v52/t2.v52),null)      AS  v52       ,
        IF(t1.v53<>0,(t1.v53-t2.v53)*LN(t1.v53/t2.v53),null)      AS  v53       ,
        IF(t1.v54<>0,(t1.v54-t2.v54)*LN(t1.v54/t2.v54),null)      AS  v54       ,
        IF(t1.v61<>0,(t1.v61-t2.v61)*LN(t1.v61/t2.v61),null)      AS  v61       ,
        IF(t1.v62<>0,(t1.v62-t2.v62)*LN(t1.v62/t2.v62),null)      AS  v62       ,
        IF(t1.v63<>0,(t1.v63-t2.v63)*LN(t1.v63/t2.v63),null)      AS  v63       ,
        IF(t1.v64<>0,(t1.v64-t2.v64)*LN(t1.v64/t2.v64),null)      AS  v64       ,
        IF(t1.v65<>0,(t1.v65-t2.v65)*LN(t1.v65/t2.v65),null)      AS  v65       ,
        IF(t1.v66<>0,(t1.v66-t2.v66)*LN(t1.v66/t2.v66),null)      AS  v66       ,
        IF(t1.v67<>0,(t1.v67-t2.v67)*LN(t1.v67/t2.v67),null)      AS  v67       ,
        IF(t1.v68<>0,(t1.v68-t2.v68)*LN(t1.v68/t2.v68),null)      AS  v68       ,
        IF(t1.v69<>0,(t1.v69-0)*LN(t1.v69/0),null)                AS  v69       ,
        IF(t1.v71<>0,(t1.v71-t2.v71)*LN(t1.v71/t2.v71),null)      AS  v71       ,
        IF(t1.v72<>0,(t1.v72-t2.v72)*LN(t1.v72/t2.v72),null)      AS  v72       ,
        IF(t1.v73<>0,(t1.v73-t2.v73)*LN(t1.v73/t2.v73),null)      AS  v73       ,
        IF(t1.v74<>0,(t1.v74-t2.v74)*LN(t1.v74/t2.v74),null)      AS  v74       ,
        IF(t1.v75<>0,(t1.v75-t2.v75)*LN(t1.v75/t2.v75),null)      AS  v75       ,
        IF(t1.v76<>0,(t1.v76-t2.v76)*LN(t1.v76/t2.v76),null)      AS  v76       ,
        IF(t1.v77<>0,(t1.v77-t2.v77)*LN(t1.v77/t2.v77),null)      AS  v77       ,
        IF(t1.v78<>0,(t1.v78-t2.v78)*LN(t1.v78/t2.v78),null)      AS  v78       ,
        IF(t1.v81<>0,(t1.v81-t2.v81)*LN(t1.v81/t2.v81),null)      AS  v81       ,
        IF(t1.v82<>0,(t1.v82-t2.v82)*LN(t1.v82/t2.v82),null)      AS  v82       ,
        IF(t1.v83<>0,(t1.v83-t2.v83)*LN(t1.v83/t2.v83),null)      AS  v83       ,
        IF(t1.v84<>0,(t1.v84-t2.v84)*LN(t1.v84/t2.v84),null)      AS  v84       ,
        IF(t1.v85<>0,(t1.v85-t2.v85)*LN(t1.v85/t2.v85),null)      AS  v85       ,
        IF(t1.v86<>0,(t1.v86-t2.v86)*LN(t1.v86/t2.v86),null)      AS  v86       ,
        IF(t1.v87<>0,(t1.v87-t2.v87)*LN(t1.v87/t2.v87),null)      AS  v87       ,
        IF(t1.v88<>0,(t1.v88-t2.v88)*LN(t1.v88/t2.v88),null)      AS  v88       ,
        IF(t1.v89<>0,(t1.v89-t2.v89)*LN(t1.v89/t2.v89),null)      AS  v89       
FROM  wp_rpt.h5_mobile_daily_pct  t1
LEFT JOIN wp_rpt.h5_mobile_csi_benchmark  t2  ON 1 = 1 
;




-- 生成H5移动 最终CSI  weplay_tmp.h5_mobile_daily_csi 

DROP TABLE IF EXISTS wp_rpt.h5_mobile_daily_csi  ;

CREATE TABLE  wp_rpt.h5_mobile_daily_csi  
AS
SELECT 
        biz_date                                                                                                                                                                                                 AS  biz_date                         ,
        IF(v11 is null,0,v11)+IF(v12 is null,0,v12)+IF(v13 is null,0,v13)+IF(v14 is null,0,v14)+IF(v15 is null,0,v15)+IF(v16 is null,0,v16)                                                                      AS  als_m12_id_max_monnum            ,
        IF(v21 is null,0,v21)+IF(v22 is null,0,v22)+IF(v23 is null,0,v23)                                                                                                                                        AS  als_m3_id_nbank_oth_allnum       ,
        IF(v31 is null,0,v31)+IF(v32 is null,0,v32)+IF(v33 is null,0,v33)                                                                                                                                        AS  cons_cont                        ,
        IF(v41 is null,0,v41)+IF(v42 is null,0,v42)+IF(v43 is null,0,v43)+IF(v44 is null,0,v44)+IF(v45 is null,0,v45)+IF(v46 is null,0,v46)                                                                      AS  netshop_eb_amt_avg               ,
        IF(v51 is null,0,v51)+IF(v52 is null,0,v52)+IF(v53 is null,0,v53)+IF(v54 is null,0,v54)                                                                                                                  AS  stab_mail_num                    ,
        IF(v61 is null,0,v61)+IF(v62 is null,0,v62)+IF(v63 is null,0,v63)+IF(v64 is null,0,v64)+IF(v65 is null,0,v65)+IF(v66 is null,0,v66)+IF(v67 is null,0,v67)+IF(v68 is null,0,v68)+IF(v69 is null,0,v69)    AS   zmxy_score                      ,             
        IF(v71 is null,0,v71)+IF(v72 is null,0,v72)+IF(v73 is null,0,v73)+IF(v74 is null,0,v74)+IF(v75 is null,0,v75)+IF(v76 is null,0,v76)+IF(v77 is null,0,v77)+IF(v78 is null,0,v78)                          AS   call_out_180s_quarter           ,    
        IF(v81 is null,0,v81)+IF(v82 is null,0,v82)+IF(v83 is null,0,v83)+IF(v84 is null,0,v84)+IF(v85 is null,0,v85)+IF(v86 is null,0,v86)+IF(v87 is null,0,v87)+IF(v88 is null,0,v88)+IF(v89 is null,0,v89)    AS  call_weekday_leisuretime_quarter
FROM wp_rpt.h5_mobile_daily_csi_detail ;




-- 生成H5移动   weplay_tmp.h5_mobile_scoreshift_daily_detail

DROP TABLE IF EXISTS wp_rpt.h5_mobile_scoreshift_daily_detail ;

CREATE TABLE wp_rpt.h5_mobile_scoreshift_daily_detail 
AS
SELECT 
        t1.biz_date                                                 AS  biz_date  ,
        (t1.v11-t2.v11)*28.1                                        AS  v11       ,
        (t1.v12-t2.v12)*22.09                                       AS  v12       ,
        (t1.v13-t2.v13)*18.78                                       AS  v13       ,
        (t1.v14-t2.v14)*13.78                                       AS  v14       ,
        (t1.v15-t2.v15)*9.42                                        AS  v15       ,
        (t1.v16-t2.v16)*(-0.57)                                     AS  v16       ,
        (t1.v21-t2.v21)*22.64                                       AS  v21       ,
        (t1.v22-t2.v22)*15.03                                       AS  v22       ,
        (t1.v23-t2.v23)*10.31                                       AS  v23       ,
        (t1.v31-t2.v31)*15.94                                       AS  v31       ,
        (t1.v32-t2.v32)*21.03                                       AS  v32       ,
        (t1.v33-t2.v33)*27.78                                       AS  v33       ,
        (t1.v41-t2.v41)*14.38                                       AS  v41       ,
        (t1.v42-t2.v42)*18.24                                       AS  v42       ,
        (t1.v43-t2.v43)*21.9                                        AS  v43       ,
        (t1.v44-t2.v44)*26.85                                       AS  v44       ,
        (t1.v45-t2.v45)*33.9                                        AS  v45       ,
        (t1.v46-t2.v46)*40.84                                       AS  v46       ,
        (t1.v51-t2.v51)*17.91                                       AS  v51       ,
        (t1.v52-t2.v52)*19.83                                       AS  v52       ,
        (t1.v53-t2.v53)*22.25                                       AS  v53       ,
        (t1.v54-t2.v54)*24.37                                       AS  v54       ,
        (t1.v61-t2.v61)*17.26                                       AS  v61       ,
        (t1.v62-t2.v62)*(-1.78)                                     AS  v62       ,
        (t1.v63-t2.v63)*9.09                                        AS  v63       ,
        (t1.v64-t2.v64)*15.41                                       AS  v64       ,
        (t1.v65-t2.v65)*20.49                                       AS  v65       ,
        (t1.v66-t2.v66)*29.63                                       AS  v66       ,
        (t1.v67-t2.v67)*35.7                                        AS  v67       ,
        (t1.v68-t2.v68)*44.3                                        AS  v68       ,
        (t1.v69-0)*44.3                                             AS  v69       ,
        (t1.v71-t2.v71)*22.81                                       AS  v71       ,
        (t1.v72-t2.v72)*32.23                                       AS  v72       ,
        (t1.v73-t2.v73)*23.93                                       AS  v73       ,
        (t1.v74-t2.v74)*21.31                                       AS  v74       ,
        (t1.v75-t2.v75)*19.08                                       AS  v75       ,
        (t1.v76-t2.v76)*17.85                                       AS  v76       ,
        (t1.v77-t2.v77)*15.35                                       AS  v77       ,
        (t1.v78-t2.v78)*12.48                                       AS  v78       ,
        (t1.v81-t2.v81)*25.69                                       AS  v81       ,
        (t1.v82-t2.v82)*43.87                                       AS  v82       ,
        (t1.v83-t2.v83)*26.61                                       AS  v83       ,
        (t1.v84-t2.v84)*21.96                                       AS  v84       ,
        (t1.v85-t2.v85)*18.13                                       AS  v85       ,
        (t1.v86-t2.v86)*15.36                                       AS  v86       ,
        (t1.v87-t2.v87)*12.12                                       AS  v87       ,
        (t1.v88-t2.v88)*7.81                                        AS  v88       ,
        (t1.v89-t2.v89)*1.57                                        AS  v89       
FROM  wp_rpt.h5_mobile_daily_pct  t1
LEFT JOIN  wp_rpt.h5_mobile_csi_benchmark  t2  ON 1 = 1
;


-- 生成H5移动最终 weplay_tmp.h5_mobile_scoreshift_daily

DROP TABLE IF EXISTS wp_rpt.h5_mobile_scoreshift_daily ;

CREATE TABLE wp_rpt.h5_mobile_scoreshift_daily 
AS
SELECT
        biz_date                                                    AS  biz_date                                ,
        v11+v12+v13+v14+v15+v16                                     AS  als_m12_id_max_monnum                   ,
        v21+v22+v23                                                 AS  als_m3_id_nbank_oth_allnum              ,
        v31+v32+v33                                                 AS  cons_cont                               ,
        v41+v42+v43+v44+v45+v46                                     AS  netshop_eb_amt_avg                      ,
        v51+v52+v53+v54                                             AS  stab_mail_num                           ,
        v61+v62+v63+v64+v65+v66+v67+v68+v69                         AS  zmxy_score                              ,
        v71+v72+v73+v74+v75+v76+v77+v78                             AS  call_out_180s_quarter                   ,
        v81+v82+v83+v84+v85+v86+v87+v88                             AS  call_weekday_leisuretime_quarter        ,
        v11+v12+v13+v14+v15+v16
        +v21+v22+v23
        +v31+v32+v33 
        +v41+v42+v43+v44+v45+v46
        +v51+v52+v53+v54 
        +v61+v62+v63+v64+v65+v66+v67+v68
        +v71+v72+v73+v74+v75+v76+v77+v78
        +v81+v82+v83+v84+v85+v86+v87+v88                           AS sum_shift
FROM  wp_rpt.h5_mobile_scoreshift_daily_detail 
;



-- H5 联通 
-- DB%_daily

DROP TABLE IF EXISTS wp_rpt.h5_unicom_daily_val_new;

CREATE TABLE wp_rpt.h5_unicom_daily_val_new
AS
SELECT  
to_date(t1.application_date) date_time,
    count(case when t1.score_card_sv1 = '3.4' then t1.oid end) as v11 ,
    count(case when t1.score_card_sv1 = '26.13' then t1.oid end) as v12,
    count(case when t1.score_card_sv1 = '45.17' then t1.oid end) as v13,
    count(case when t1.score_card_sv2 = '25.08' then t1.oid end) as v21,
    count(case when t1.score_card_sv2 = '14.41' then t1.oid end) as v22,
    count(case when t1.score_card_sv2 = '2.77' then t1.oid end) as v23,
    count(case when t1.score_card_sv3 = '23.43' then t1.oid end) as v31,
    count(case when t1.score_card_sv3 = '8.08' then t1.oid end) as v32,
    count(case when t1.score_card_sv4 = '23.66' then t1.oid end) as v41,
    count(case when t1.score_card_sv4 = '21.34' then t1.oid end) as v42,
    count(case when t1.score_card_sv4 = '9.72' then t1.oid end) as v43,
    count(case when t1.score_card_sv5 = '12.55' then t1.oid end) as v51,
    count(case when t1.score_card_sv5 = '19.26' then t1.oid end) as v52,
    count(case when t1.score_card_sv5 = '27.07' then t1.oid end) as v53,
    count(case when t1.score_card_sv6 = '20.6' then t1.oid end) as v61,
    count(case when t1.score_card_sv6 = '22.74' then t1.oid end) as v62,
    count(case when t1.score_card_sv6 = '11.36' then t1.oid end) as v63,
    count(case when t1.score_card_sv7 = '17.9' then t1.oid end) as v71,
    count(case when t1.score_card_sv7 = '23.16' then t1.oid end) as v72,
    count(case when t1.score_card_sv7 = '12.61' then t1.oid end) as v73,
    count(case when t1.score_card_sv8 = '24.86' then t1.oid end) as v81,
    count(case when t1.score_card_sv8 = '4.91' then t1.oid end) as v82,
    count(case when t1.score_card_sv8 = '21.71' then t1.oid end) as v83
from
(
    select * 
    from weplay.merged_data aa
    where aa.score_card_version = '4.1' 
          and aa.self_score_class in ('S1','S2','S3','S4','S5')
          and to_date(aa.application_date) >= '2018-07-09' 
          and to_date(application_date) < to_date(CURRENT_TIMESTAMP())
 ) t1 
group by to_date(t1.application_date) order by date_time
;




-- DB%_daily

DROP TABLE IF EXISTS wp_rpt.h5_unicom_daily_pct_new ;

CREATE TABLE wp_rpt.h5_unicom_daily_pct_new
AS
SELECT
    t1.date_time,
    v11/(v11 + v12 + v13) v11,  
    v12/(v11 + v12 + v13) v12,
    v13/(v11 + v12 + v13) v13,
    v21/(v21 + v22 + v23) v21,
    v22/(v21 + v22 + v23) v22,
    v23/(v21 + v22 + v23) v23,
    v31/(v31 + v32) v31,
    v32/(v31 + v32) v32,
    v41/(v41 + v42 + v43)v41,
    v42/(v41 + v42 + v43) v42,
    v43/(v41 + v42 + v43) v43,
    v51/(v51 + v52 + v53) v51,
    v52/(v51 + v52 + v53) v52,
    v53/(v51 + v52 + v53) v53,
    v61/(v61 + v62 + v63) v61,
    v62/(v61 + v62 + v63)  v62,
    v63/(v61 + v62 + v63) v63,
    v71/(v71 + v72 + v73) v71,
    v72/(v71 + v72 + v73) v72,
    v73/(v71 + v72 + v73) v73,
    v81/(v81 + v82 + v83) v81,
    v82/(v81 + v82 + v83) v82, 
    v83/(v81 + v82 + v83) v83
    
FROM wp_rpt.h5_unicom_daily_val_new t1
order by t1.date_time
;




--DB(%)_CSI_Daily

DROP TABLE IF EXISTS wp_rpt.h5_unicom_daily_csi_detail_new;

CREATE TABLE wp_rpt.h5_unicom_daily_csi_detail_new
AS
SELECT 
T1.date_time,
if (t1.v11 <> 0, (t1.v11 - t2.v11) * LN(t1.v11/t2.v11 ),0) csi_v11,
if (t1.v12 <> 0,(t1.v12- t2.v12) * LN(t1.v12/t2.v12 ),0) csi_v12,
if (t1.v13 <> 0,(t1.v13 - t2.v13) * LN(t1.v13/t2.v13 ),0) csi_v13,
if (t1.v21 <> 0,(t1.v21 - t2.v21) * LN(t1.v21/t2.v21 ),0) csi_v21,
if (t1.v22 <> 0,(t1.v22 - t2.v22) * LN(t1.v22/t2.v22 ),0) csi_v22,
if (t1.v23 <> 0,(t1.v23 - t2.v23) * LN(t1.v23/t2.v23 ),0) csi_v23,
if (t1.v31 <> 0,(t1.v31 - t2.v31) * LN(t1.v31/t2.v31 ),0) csi_v31,
if (t1.v32 <> 0,(t1.v32 - t2.v32) * LN(t1.v32/t2.v32),0) csi_v32,
if (t1.v41 <> 0,(t1.v41 - t2.v41) * LN(t1.v41/t2.v41 ),0) csi_v41,
if (t1.v42 <> 0,(t1.v42- t2.v42) * LN(t1.v42/t2.v42 ),0) csi_v42,
if (t1.v43 <> 0,(t1.v43 - t2.v43) * LN(t1.v43/t2.v43 ),0) csi_v43,
if (t1.v51 <> 0,(t1.v51 - t2.v51) * LN(t1.v51/t2.v51 ),0) csi_v51,
if (t1.v52 <> 0,(t1.v52 - t2.v52) * LN(t1.v52/t2.v52 ),0) csi_v52,
if (t1.v53 <> 0,(t1.v53 - t2.v53) * LN(t1.v53/t2.v53 ),0) csi_v53,
if (t1.v61 <> 0,(t1.v61 - t2.v61) * LN(t1.v61/t2.v61 ),0) csi_v61,
if (t1.v62 <> 0,(t1.v62 - t2.v62) * LN(t1.v62/t2.v62 ),0) csi_v62,
if (t1.v63 <> 0,(t1.v63 - t2.v63) * LN(t1.v63/t2.v63 ),0) csi_v63,
if (t1.v71 <> 0,(t1.v71 - t2.v71) * LN(t1.v71/t2.v71 ),0) csi_v71,
if (t1.v72 <> 0,(t1.v72 - t2.v72) * LN(t1.v72/t2.v72 ),0) csi_v72,
if (t1.v73 <> 0,(t1.v73 - t2.v73) * LN(t1.v73/t2.v73 ),0) csi_v73,
if (t1.v81 <> 0,(t1.v81 - t2.v81) * LN(t1.v81/t2.v81 ),0) csi_v81,
if (t1.v82 <> 0,(t1.v82 - t2.v82) * LN(t1.v82/t2.v82 ),0) csi_v82,
if (t1.v83 <> 0,(t1.v83 - t2.v83) * LN(t1.v83/t2.v83 ),0) csi_v83
FROM wp_rpt.h5_unicom_daily_pct_new t1
LEFT JOIN wp_rpt.h5_unicom_grouppst_ac_new t2 ON 1 =1
;




-- DB(%)_ScoreShift_Daily

DROP TABLE IF EXISTS wp_rpt.h5_unicom_scoreshift_daily_detail_new;

CREATE TABLE wp_rpt.h5_unicom_scoreshift_daily_detail_new
AS
SELECT 
T1.date_time,
(t1.v11 - t2.v11) * 3.4 Score_v11,
(t1.v12 - t2.v12) * 26.13 Score_v12,
(t1.v13 - t2.v13) * 45.17 Score_v13,
(t1.v21 - t2.v21) * 25.08 Score_v21,
(t1.v22 - t2.v22) * 14.41 Score_v22,
(t1.v23 - t2.v23) * 2.77 Score_v23,
(t1.v31 - t2.v31) * 23.43 Score_v31,
(t1.v32 - t2.v32) * 8.08 Score_v32,
(t1.v41 - t2.v41) * 23.66 Score_v41,
(t1.v42 - t2.v42) * 21.34 Score_v42,
(t1.v43 - t2.v43) * 9.72 Score_v43,
(t1.v51 - t2.v51) * 12.55 Score_v51,
(t1.v52 - t2.v52) * 19.26 Score_v52,
(t1.v53 - t2.v53) * 27.07 Score_v53,
(t1.v61 - t2.v61) * 20.6 Score_v61,
(t1.v62 - t2.v62) * 22.74 Score_v62,
(t1.v63 - t2.v63) * 11.36 Score_v63,
(t1.v71 - t2.v71) * 17.9 Score_v71,
(t1.v72 - t2.v72) * 23.16 Score_v72,
(t1.v73 - t2.v73) * 12.61 Score_v73,
(t1.v81 - t2.v81) * 24.86 Score_v81,
(t1.v82 - t2.v82) * 4.91 Score_v82,
(t1.v83 - t2.v83) * 21.71 Score_v83
FROM wp_rpt.h5_unicom_daily_pct_new  t1
LEFT JOIN wp_rpt.h5_unicom_grouppst_ac_new t2 ON 1 = 1
;


----(%)csi_detail

DROP TABLE IF EXISTS wp_rpt.h5_unicom_daily_csi_new ;

CREATE TABLE wp_rpt.h5_unicom_daily_csi_new
AS
SELECT 
date_time,
nvl(csi_v11,0) + nvl(csi_v12,0) + nvl(csi_v13,0) csi_sum_v1,
nvl(csi_v21,0) + nvl(csi_v22,0) + nvl(csi_v23,0) csi_sum_v2,
nvl(csi_v31,0) + nvl(csi_v32,0) csi_sum_v3,
nvl(csi_v41,0) + nvl(csi_v42,0) + nvl(csi_v43,0) csi_sum_v4,
nvl(csi_v51,0) + nvl(csi_v52,0) + nvl(csi_v53,0) csi_sum_v5,
nvl(csi_v61,0) + nvl(csi_v62,0) + nvl(csi_v63,0) csi_sum_v6,
nvl(csi_v71,0) + nvl(csi_v72,0) + nvl(csi_v73,0) csi_sum_v7,
nvl(csi_v81,0) + nvl(csi_v82,0) + nvl(csi_v83,0) csi_sum_v8
FROM wp_rpt.h5_unicom_daily_csi_detail_new
;


--(%)scoreshift_Daily

DROP TABLE IF EXISTS wp_rpt.h5_unicom_scoreshift_daily_new ;

CREATE TABLE wp_rpt.h5_unicom_scoreshift_daily_new
AS
SELECT 
  date_time,
  score_v11 + score_v12 + score_v13             AS score_sum_v1,
  score_v21 + score_v22 + score_v23  AS score_sum_v2,
  score_v31 + score_v32  AS score_sum_v3,
  score_v41 + score_v42 + score_v43  AS score_sum_v4,
  score_v51 + score_v52 + score_v53  AS score_sum_v5,
  score_v61 + score_v62 + score_v63  AS score_sum_v6,
  score_v71 + score_v72 + score_v73  AS score_sum_v7,
  score_v81 + score_v82 + score_v83  AS score_sum_v8,
  score_v11 + score_v12 + score_v13 
 +score_v21 + score_v22 + score_v23
 +score_v31 + score_v32
 +score_v41 + score_v42 + score_v43
 +score_v51 + score_v52 + score_v53
 +score_v61 + score_v62 + score_v63
 +score_v71 + score_v72 + score_v73
 +score_v81 + score_v82 + score_v83  AS sum_shift
FROM wp_rpt.h5_unicom_scoreshift_daily_detail_new
;


-- H5 电信
-- by William


-- DB%_daily

DROP TABLE IF EXISTS wp_rpt.h5_telecom_daily_val;

CREATE TABLE wp_rpt.h5_telecom_daily_val
AS
SELECT  
to_date(t1.application_date) date_time,
    count(case when t1.score_card_sv1 = '3.4' then t1.oid end) as v11 ,
    count(case when t1.score_card_sv1 = '26.13' then t1.oid end) as v12,
    count(case when t1.score_card_sv1 = '45.17' then t1.oid end) as v13,
    count(case when t1.score_card_sv2 = '25.08' then t1.oid end) as v21,
    count(case when t1.score_card_sv2 = '14.41' then t1.oid end) as v22,
    count(case when t1.score_card_sv2 = '2.77' then t1.oid end) as v23,
    count(case when t1.score_card_sv3 = '23.43' then t1.oid end) as v31,
    count(case when t1.score_card_sv3 = '8.08' then t1.oid end) as v32,
    count(case when t1.score_card_sv4 = '23.66' then t1.oid end) as v41,
    count(case when t1.score_card_sv4 = '21.34' then t1.oid end) as v42,
    count(case when t1.score_card_sv4 = '9.72' then t1.oid end) as v43,
    count(case when t1.score_card_sv5 = '0' then t1.oid end) as v51,
    count(case when t1.score_card_sv5 = '19.26' then t1.oid end) as v52,
    count(case when t1.score_card_sv5 = '27.07' then t1.oid end) as v53,
    count(case when t1.score_card_sv6 = '20.6' then t1.oid end) as v61,
    count(case when t1.score_card_sv6 = '22.74' then t1.oid end) as v62,
    count(case when t1.score_card_sv6 = '11.36' then t1.oid end) as v63,
    count(case when t1.score_card_sv7 = '17.9' then t1.oid end) as v71,
    count(case when t1.score_card_sv7 = '23.16' then t1.oid end) as v72,
    count(case when t1.score_card_sv7 = '12.61' then t1.oid end) as v73,
    count(case when t1.score_card_sv8 = '24.86' then t1.oid end) as v81,
    count(case when t1.score_card_sv8 = '4.91' then t1.oid end) as v82,
    count(case when t1.score_card_sv8 = '21.71' then t1.oid end) as v83
from
(
    select * 
    from weplay.merged_data aa
    where aa.score_card_version = '7.0' 
          and aa.self_score_class in ('S1','S2','S3','S4','S5')
          and to_date(application_date) < to_date(CURRENT_TIMESTAMP())
 ) t1 
group by to_date(t1.application_date) order by date_time
;




-- DB%_daily

DROP TABLE IF EXISTS wp_rpt.h5_telecom_daily_pct ;

CREATE TABLE wp_rpt.h5_telecom_daily_pct
AS
SELECT
    t1.date_time,
    v11/(v11 + v12 + v13) v11,  
    v12/(v11 + v12 + v13) v12,
    v13/(v11 + v12 + v13) v13,
    v21/(v21 + v22 + v23) v21,
    v22/(v21 + v22 + v23) v22,
    v23/(v21 + v22 + v23) v23,
    v31/(v31 + v32) v31,
    v32/(v31 + v32) v32,
    v41/(v41 + v42 + v43)v41,
    v42/(v41 + v42 + v43) v42,
    v43/(v41 + v42 + v43) v43,
    v51/(v51 + v52 + v53) v51,
    v52/(v51 + v52 + v53) v52,
    v53/(v51 + v52 + v53) v53,
    v61/(v61 + v62 + v63) v61,
    v62/(v61 + v62 + v63)  v62,
    v63/(v61 + v62 + v63) v63,
    v71/(v71 + v72 + v73) v71,
    v72/(v71 + v72 + v73) v72,
    v73/(v71 + v72 + v73) v73,
    v81/(v81 + v82 + v83) v81,
    v82/(v81 + v82 + v83) v82, 
    v83/(v81 + v82 + v83) v83
    
FROM wp_rpt.h5_telecom_daily_val t1
order by t1.date_time
;




--DB(%)_CSI_Daily

DROP TABLE IF EXISTS wp_rpt.h5_telecom_daily_csi_detail;

CREATE TABLE wp_rpt.h5_telecom_daily_csi_detail
AS
SELECT 
T1.date_time,
if (t1.v11 <> 0, (t1.v11 - t2.v11) * LN(t1.v11/t2.v11 ),0) csi_v11,
if (t1.v12 <> 0,(t1.v12- t2.v12) * LN(t1.v12/t2.v12 ),0) csi_v12,
if (t1.v13 <> 0,(t1.v13 - t2.v13) * LN(t1.v13/t2.v13 ),0) csi_v13,
if (t1.v21 <> 0,(t1.v21 - t2.v21) * LN(t1.v21/t2.v21 ),0) csi_v21,
if (t1.v22 <> 0,(t1.v22 - t2.v22) * LN(t1.v22/t2.v22 ),0) csi_v22,
if (t1.v23 <> 0,(t1.v23 - t2.v23) * LN(t1.v23/t2.v23 ),0) csi_v23,
if (t1.v31 <> 0,(t1.v31 - t2.v31) * LN(t1.v31/t2.v31 ),0) csi_v31,
if (t1.v32 <> 0,(t1.v32 - t2.v32) * LN(t1.v32/t2.v32),0) csi_v32,
if (t1.v41 <> 0,(t1.v41 - t2.v41) * LN(t1.v41/t2.v41 ),0) csi_v41,
if (t1.v42 <> 0,(t1.v42- t2.v42) * LN(t1.v42/t2.v42 ),0) csi_v42,
if (t1.v43 <> 0,(t1.v43 - t2.v43) * LN(t1.v43/t2.v43 ),0) csi_v43,
if (t1.v51 <> 0,(t1.v51 - t2.v51) * LN(t1.v51/t2.v51 ),0) csi_v51,
if (t1.v52 <> 0,(t1.v52 - t2.v52) * LN(t1.v52/t2.v52 ),0) csi_v52,
if (t1.v53 <> 0,(t1.v53 - t2.v53) * LN(t1.v53/t2.v53 ),0) csi_v53,
if (t1.v61 <> 0,(t1.v61 - t2.v61) * LN(t1.v61/t2.v61 ),0) csi_v61,
if (t1.v62 <> 0,(t1.v62 - t2.v62) * LN(t1.v62/t2.v62 ),0) csi_v62,
if (t1.v63 <> 0,(t1.v63 - t2.v63) * LN(t1.v63/t2.v63 ),0) csi_v63,
if (t1.v71 <> 0,(t1.v71 - t2.v71) * LN(t1.v71/t2.v71 ),0) csi_v71,
if (t1.v72 <> 0,(t1.v72 - t2.v72) * LN(t1.v72/t2.v72 ),0) csi_v72,
if (t1.v73 <> 0,(t1.v73 - t2.v73) * LN(t1.v73/t2.v73 ),0) csi_v73,
if (t1.v81 <> 0,(t1.v81 - t2.v81) * LN(t1.v81/t2.v81 ),0) csi_v81,
if (t1.v82 <> 0,(t1.v82 - t2.v82) * LN(t1.v82/t2.v82 ),0) csi_v82,
if (t1.v83 <> 0,(t1.v83 - t2.v83) * LN(t1.v83/t2.v83 ),0) csi_v83
FROM wp_rpt.h5_telecom_daily_pct t1
LEFT JOIN wp_rpt.h5_telecom_grouppst_ac t2 ON 1 =1
;




-- DB(%)_ScoreShift_Daily

DROP TABLE IF EXISTS wp_rpt.h5_telecom_scoreshift_daily_detail;

CREATE TABLE wp_rpt.h5_telecom_scoreshift_daily_detail
AS
SELECT 
T1.date_time,
(t1.v11 - t2.v11) * 3.4 Score_v11,
(t1.v12 - t2.v12) * 26.13 Score_v12,
(t1.v13 - t2.v13) * 45.17 Score_v13,
(t1.v21 - t2.v21) * 25.08 Score_v21,
(t1.v22 - t2.v22) * 14.41 Score_v22,
(t1.v23 - t2.v23) * 2.77 Score_v23,
(t1.v31 - t2.v31) * 23.43 Score_v31,
(t1.v32 - t2.v32) * 8.08 Score_v32,
(t1.v41 - t2.v41) * 23.66 Score_v41,
(t1.v42 - t2.v42) * 21.34 Score_v42,
(t1.v43 - t2.v43) * 9.72 Score_v43,
(t1.v51 - t2.v51) * 0 Score_v51,
(t1.v52 - t2.v52) * 19.26 Score_v52,
(t1.v53 - t2.v53) * 27.07 Score_v53,
(t1.v61 - t2.v61) * 20.6 Score_v61,
(t1.v62 - t2.v62) * 22.74 Score_v62,
(t1.v63 - t2.v63) * 11.36 Score_v63,
(t1.v71 - t2.v71) * 17.9 Score_v71,
(t1.v72 - t2.v72) * 23.16 Score_v72,
(t1.v73 - t2.v73) * 12.61 Score_v73,
(t1.v81 - t2.v81) * 24.86 Score_v81,
(t1.v82 - t2.v82) * 4.91 Score_v82,
(t1.v83 - t2.v83) * 21.71 Score_v83
FROM wp_rpt.h5_telecom_daily_pct  t1
LEFT JOIN wp_rpt.h5_telecom_grouppst_ac t2 ON 1 = 1
;


----(%)csi_detail

DROP TABLE IF EXISTS wp_rpt.h5_telecom_daily_csi ;

CREATE TABLE wp_rpt.h5_telecom_daily_csi
AS
SELECT 
date_time,
nvl(csi_v11,0) + nvl(csi_v12,0) + nvl(csi_v13,0) csi_sum_v1,
nvl(csi_v21,0) + nvl(csi_v22,0) + nvl(csi_v23,0) csi_sum_v2,
nvl(csi_v31,0) + nvl(csi_v32,0) csi_sum_v3,
nvl(csi_v41,0) + nvl(csi_v42,0) + nvl(csi_v43,0) csi_sum_v4,
nvl(csi_v51,0) + nvl(csi_v52,0) + nvl(csi_v53,0) csi_sum_v5,
nvl(csi_v61,0) + nvl(csi_v62,0) + nvl(csi_v63,0) csi_sum_v6,
nvl(csi_v71,0) + nvl(csi_v72,0) + nvl(csi_v73,0) csi_sum_v7,
nvl(csi_v81,0) + nvl(csi_v82,0) + nvl(csi_v83,0) csi_sum_v8
FROM wp_rpt.h5_telecom_daily_csi_detail
;


--(%)scoreshift_Daily

DROP TABLE IF EXISTS wp_rpt.h5_telecom_scoreshift_daily ;

CREATE TABLE wp_rpt.h5_telecom_scoreshift_daily
AS
SELECT 
  date_time,
  score_v11 + score_v12 + score_v13             AS score_sum_v1,
  score_v21 + score_v22 + score_v23  AS score_sum_v2,
  score_v31 + score_v32  AS score_sum_v3,
  score_v41 + score_v42 + score_v43  AS score_sum_v4,
  score_v51 + score_v52 + score_v53  AS score_sum_v5,
  score_v61 + score_v62 + score_v63  AS score_sum_v6,
  score_v71 + score_v72 + score_v73  AS score_sum_v7,
  score_v81 + score_v82 + score_v83  AS score_sum_v8,
  score_v11 + score_v12 + score_v13 
 +score_v21 + score_v22 + score_v23
 +score_v31 + score_v32
 +score_v41 + score_v42 + score_v43
 +score_v51 + score_v52 + score_v53
 +score_v61 + score_v62 + score_v63
 +score_v71 + score_v72 + score_v73
 +score_v81 + score_v82 + score_v83  AS sum_shift
FROM wp_rpt.h5_telecom_scoreshift_daily_detail
;


-- 生成 PSI unknowm 监控表

DROP TABLE IF EXISTS wp_rpt.psi_index_daily_unknown ;

CREATE TABLE wp_rpt.psi_index_daily_unknown
AS
SELECT
TO_DATE(CURRENT_TIMESTAMP() - interval 1 day)                                                               AS  biz_date        ,
CASE WHEN  t1.platform_model in ('H5_mob','H5_telecom','H5_unicom') THEN 'H5' ELSE t1.platform_model  END   AS  platform_model  ,
SUM(t1.s_unknown)/SUM(t1.s_unknown+t1.sum_s+t1.s_sn+t1.s_na)                                                AS   unknown_ratio
FROM 
(
SELECT platform_model,sum_s,s_unknown,s_sn,s_na       FROM  wp_rpt.psi_supervisory_android      WHERE  biz_date = TO_DATE(CURRENT_TIMESTAMP() - interval 1 day)
UNION ALL
SELECT platform_model,sum_s,s_unknown,s_sn,s_na       FROM  wp_rpt.psi_supervisory_iOS          WHERE  biz_date = TO_DATE(CURRENT_TIMESTAMP() - interval 1 day)
UNION ALL
SELECT platform_model,sum_s,s_unknown,s_sn,s_na       FROM  wp_rpt.psi_supervisory_h5_unicom    WHERE  biz_date = TO_DATE(CURRENT_TIMESTAMP() - interval 1 day)
UNION ALL
SELECT platform_model,sum_s,s_unknown,s_sn,s_na       FROM  wp_rpt.psi_supervisory_h5_mob       WHERE  biz_date = TO_DATE(CURRENT_TIMESTAMP() - interval 1 day)
UNION ALL
SELECT platform_model,sum_s,s_unknown,s_sn,s_na       FROM  wp_rpt.psi_supervisory_h5_telecom   WHERE  biz_date = TO_DATE(CURRENT_TIMESTAMP() - interval 1 day)
)  t1 
GROUP BY 1,2 ;




--PSI,缺失率，特别区间占比监控
DROP TABLE IF EXISTS wp_rpt.psi_index_daily;

CREATE TABLE wp_rpt.psi_index_daily
AS
SELECT
biz_date                                                                                         AS  biz_date        ,
t1.platform_model                                                                                AS  platform_model  , 
CASE WHEN t1.po = 1 THEN (t1.s_unknown+t1.s_na+t1.s_sn+t1.sum_s)
      ELSE (t1.sum_s)   
      END                                                                                        AS  s_sum           ,
t1.s_psi                                                                                         AS  s_psi           ,
t1.s_unknown+t1.s_na                                                                             AS  na_unknown_num  ,
CASE WHEN t1.po = 1 THEN (t1.s_unknown+t1.s_na)/(t1.s_unknown+t1.s_na+t1.s_sn+t1.sum_s) 
      ELSE (t1.s_unknown+t1.s_na)/(t1.sum_s)   
      END                                                                                        AS  na_unknown_ratio , 
t1.s_sn                                                                                          AS  sn_num  ,
CASE WHEN t1.po = 1 THEN (t1.s_sn)/(t1.s_unknown+t1.s_na+t1.s_sn+t1.sum_s)   
     ELSE  (t1.s_sn)/(t1.sum_s)               
     END                                                                                         AS  sn_ration        
FROM 
(
SELECT 1 AS po,biz_date,platform_model,sum_s,s_unknown,s_sn,s_na,s_psi        FROM  wp_rpt.psi_supervisory_android        WHERE  biz_date >= TO_DATE(CURRENT_TIMESTAMP() - interval 30 day)
UNION ALL
SELECT 1 AS po,biz_date,platform_model,sum_s,s_unknown,s_sn,s_na,s_psi        FROM  wp_rpt.psi_supervisory_iOS            WHERE  biz_date >= TO_DATE(CURRENT_TIMESTAMP() - interval 30 day)
UNION ALL
SELECT 1 AS po,biz_date,platform_model,sum_s,s_unknown,s_sn,s_na,psi          FROM  wp_rpt.psi_supervisory_h5_unicom      WHERE  biz_date >= TO_DATE(CURRENT_TIMESTAMP() - interval 30 day)
UNION ALL
SELECT 1 AS po,biz_date,platform_model,sum_s,s_unknown,s_sn,s_na,psi          FROM  wp_rpt.psi_supervisory_h5_mob         WHERE  biz_date >= TO_DATE(CURRENT_TIMESTAMP() - interval 30 day)
UNION ALL
SELECT 1 AS po,biz_date,platform_model,sum_s,s_unknown,s_sn,s_na,psi          FROM  wp_rpt.psi_supervisory_h5_telecom     WHERE  biz_date >= TO_DATE(CURRENT_TIMESTAMP() - interval 30 day)
UNION ALL 
SELECT 2 AS po,biz_date,'icekredit_huiyan',sum_s,s1,s2,0,s_psi                FROM  wp_rpt.icekredit_huiyan_score         WHERE  biz_date >= TO_DATE(CURRENT_TIMESTAMP() - interval 30 day)
UNION ALL 
SELECT 2 AS po,biz_date,'icekredit_huomou',sum_s,s1,s2,0,s_psi                FROM  wp_rpt.icekredit_huomou_score         WHERE  biz_date >= TO_DATE(CURRENT_TIMESTAMP() - interval 30 day)
UNION ALL 
SELECT 2 AS po,biz_date,'fm_final',sum_s,s1,s2,0,s_psi                        FROM  wp_rpt.fm_final_score                 WHERE  biz_date >= TO_DATE(CURRENT_TIMESTAMP() - interval 30 day)
UNION ALL 
SELECT 2 AS po,biz_date,'baidu',sum_s,s1_1,s1_2,0,s_psi                       FROM  wp_rpt.baidu_score                    WHERE  biz_date >= TO_DATE(CURRENT_TIMESTAMP() - interval 30 day)
)  t1 ;


