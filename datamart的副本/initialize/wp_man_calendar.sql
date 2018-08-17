-- INVALIDATE METADATA;

DROP TABLE IF EXISTS wp_man.calendar;
CREATE TABLE wp_man.calendar AS
SELECT      id,
            CAST(FROM_TIMESTAMP(datet, 'yyyyMMdd') AS INT) datei,
            TO_DATE(datet) dates,
            datet,
            CONCAT(FROM_TIMESTAMP(datet, 'yyyy'), '-Q', CAST(CAST(MONTH(datet)/4+1 AS INT) AS STRING)) quarters,
            FROM_TIMESTAMP(datet, 'yyyy-MM') months,
            FROM_TIMESTAMP(datet, 'MMM') monthname,
            CONCAT(CAST(YEAR(datet) AS STRING),'-', STRRIGHT(CONCAT('00', CAST(CAST((DAYOFYEAR(datet) + DAYOFWEEK(trunc(datet, 'year') - INTERVAL 1 DAY) - 2) / 7 + 1 AS INT) AS STRING)), 2)) weeks,
            CAST((DAYOFYEAR(datet) + DAYOFWEEK(trunc(datet, 'year') - INTERVAL 1 DAY) - 2) / 7 + 1 AS INT) woy,
            DAYOFWEEK(datet - INTERVAL 1 DAY) dow,
            STRLEFT(DAYNAME(datet), 3) dayname,
            SUBSTR('周日周一周二周三周四周五周六', DAYOFWEEK(datet)*6-5, 6) daynamec,
            CONCAT(CAST(YEAR(datet) AS STRING), '-', STRRIGHT(CONCAT('00', CAST(CAST((DAYOFYEAR(datet) + DAYOFWEEK(trunc(datet, 'year') - INTERVAL 1 DAY) - 2) / 7 + 1 AS INT) AS STRING)), 2), '-', FROM_TIMESTAMP(datet, 'MMM')) weekmonth,
            DAYOFWEEK(datet) IN (1,7) is_weekend,
            TRUNC(datet, 'MONTH') start_day,
            LAST_DAY(datet) last_day,
            datet - INTERVAL 1 MONTH mom_day,
            TRUNC(datet, 'DAY') start_day_week,
            TRUNC(datet, 'DAY') + INTERVAL 6 DAY last_day_week,
            datet - INTERVAL 7 DAY wow_day            
FROM        (SELECT id,
                    DATE_ADD('2013-01-01', id - 1) datet
             FROM wp_man.sequence
             ORDER BY 1) t1
;

COMPUTE STATS wp_man.calendar;

DROP VIEW IF EXISTS wp_calc.calendar;
CREATE VIEW wp_calc.calendar AS SELECT * FROM wp_man.calendar;

DROP VIEW IF EXISTS wp_biz.calendar;
CREATE VIEW wp_biz.calendar AS SELECT * FROM wp_man.calendar;

DROP VIEW IF EXISTS weplay.calendar;
CREATE VIEW weplay.calendar AS SELECT * FROM wp_man.calendar;
