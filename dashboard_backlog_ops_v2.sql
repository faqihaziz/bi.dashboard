 -------------------update_query_backlog_ops_25042024-----------------------

WITH
		root_data_lm AS (
WITH lm AS (
SELECT * 
FROM `datamart_idexp.dashboard_productivity_lm`
-- FROM `dev_idexp.dummy_lm_93_days`
-- WHERE th_arrival = last_location
  -- OR (return_regist_time IS NOT NULL AND ) 
)

, return AS (
SELECT * FROM `datamart_idexp.dashboard_return_monitoring`
WHERE latest_scan_activity NOT IN ('Confirm Return Bill', 'Return POD Scan')
	AND return_branch_name != latest_scan_location
  	AND return_confirm_record_time IS NOT NULL
)

SELECT lm.*
FROM lm
LEFT JOIN return ON return.waybill_no = lm.waybill_no
-- WHERE return.waybill_no IS NULL
		),

selected_root_lm AS (

SELECT * FROM (

		SELECT

ww.waybill_no,
ww.aging_day,
kw.kanwil_name,
ww.th_arrival,
kw.branch_no th_arrival_no,
ww.arrival_time,
kw.city_name,
kw.province_name,
ww.pod_time,
ww.return_regist_time,
-- ww.return_confirm_record_time,




		FROM root_data_lm ww
		LEFT JOIN `datamart_idexp.masterdata_facility_to_kanwil` kw ON ww.th_arrival = kw.branch_name

		WHERE ww.aging_day >2
)
	),

get_awb_sending_3pl AS (

SELECT *

FROM (

SELECT
            currenttab.waybill_no,
            DATETIME(currenttab.record_time,'Asia/Jakarta') AS record_time,
            currenttab.operation_branch_name AS operation_branch_name,
            option.option_name AS operation_type,
            currenttab.next_location_name AS next_location_sc,
            
            FROM
                `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab
                LEFT JOIN `datawarehouse_idexp.system_option` AS option ON currenttab.operation_type = option.option_value AND option.type_option = 'operationType'
                                                
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
            AND currenttab.deleted = '0'
            AND currenttab.next_location_name IN ('3PL')

QUALIFY ROW_NUMBER() OVER(PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time DESC)=1
  )
),

first_deliv_attempt AS (

  SELECT
ww.waybill_no,
MIN(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_deliv_attempt,
MIN(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS scan_type,

FROM selected_root_lm ww
LEFT OUTER JOIN `datawarehouse_idexp.waybill_waybill_line` sc ON ww.waybill_no = sc.waybill_no
    AND DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

WHERE 
DATE(ww.arrival_time) >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))

AND sc.operation_type = "09"
AND ww.th_arrival = sc.operation_branch_name
AND DATETIME(sc.record_time,'Asia/Jakarta') > arrival_time

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY sc.record_time ASC)=1

),

return_data AS (

  SELECT

ww.waybill_no,
DATETIME(rr.return_record_time,'Asia/Jakarta') return_regist_time,
DATETIME(rr.return_pod_record_time,'Asia/Jakarta') return_pod_record_time,
DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') return_confirm_record_time,

  FROM selected_root_lm ww
  LEFT JOIN `datawarehouse_idexp.waybill_return_bill` rr ON ww.waybill_no = rr.waybill_no
      AND DATE(rr.return_record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))

WHERE 
DATE(ww.arrival_time) >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))  

  QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY rr.update_time DESC)=1
),

last_pos AS (

SELECT 
      ps.waybill_no,
      MAX(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS last_pos_reason, 
      MAX(DATETIME(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS last_pos_attempt,
      MAX(t4.option_name) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS last_problem_type,
      
      FROM `datawarehouse_idexp.waybill_problem_piece` ps
      LEFT OUTER JOIN `datawarehouse_idexp.system_option` t4 ON t4.option_value = ps.problem_type AND t4.type_option = 'problemType' 
    
      WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
      AND ps.problem_type NOT IN ('02')

QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no ORDER BY operation_time DESC)=1
),

join_all_1 AS (

  SELECT

  ww.*,
  fd.first_deliv_attempt,
  DATE_DIFF(DATE(fd.first_deliv_attempt), DATE(ww.arrival_time), DAY) arrival_to_deliv_day,
  rr.return_confirm_record_time,
  rr.return_pod_record_time,
  a.next_location_sc sending_3pl_status,
  lp.last_pos_reason,
  lp.last_pos_attempt,

  FROM selected_root_lm ww
  LEFT JOIN first_deliv_attempt fd ON ww.waybill_no = fd.waybill_no
  LEFT JOIN return_data rr ON ww.waybill_no = rr.waybill_no
  LEFT JOIN get_awb_sending_3pl a ON ww.waybill_no = a.waybill_no
  LEFT JOIN last_pos lp ON ww.waybill_no = lp.waybill_no

  -- WHERE DATE(ww.arrival_time) BETWEEN '2024-03-19' AND '2024-04-18'
),

join_all_2 AS (

  SELECT

waybill_no,
aging_day,
kanwil_name,
th_arrival,
th_arrival_no,
arrival_time,
city_name,
province_name,
first_deliv_attempt,
arrival_to_deliv_day,
pod_time,
return_regist_time,
return_confirm_record_time,
return_pod_record_time,

CASE
    WHEN aging_day >2 AND first_deliv_attempt IS NULL THEN 1
    WHEN aging_day >2 AND arrival_to_deliv_day >2 THEN 1
    WHEN aging_day >2 AND arrival_to_deliv_day <=2 THEN 0
    ELSE 0
    END AS more_4_days,
CASE
    WHEN aging_day >14 AND return_regist_time IS NULL THEN 1
    ELSE 0 
    END AS more_14_days,

sending_3pl_status,
last_pos_reason,
last_pos_attempt,


FROM join_all_1

QUALIFY ROW_NUMBER() OVER(PARTITION BY waybill_no)=1
)

SELECT * FROM join_all_2

WHERE sending_3pl_status IS NULL
AND SUBSTR(th_arrival,1,6) NOT IN ('TH VIP')
AND last_pos_reason NOT IN ('Paket hilang atau tidak ditemukan')
