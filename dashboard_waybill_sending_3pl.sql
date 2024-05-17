WITH get_awb_sending_3pl AS (

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
                                                
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2024-03-01' AND '2024-03-20' 
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))
            AND currenttab.deleted = '0'
            AND currenttab.next_location_name IN ('3PL')
            -- AND currenttab.waybill_no IN ('IDE701407846707')

QUALIFY ROW_NUMBER() OVER(PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time DESC)=1

-- ORDER BY record_time DESC
            )
),

scan_record_main AS (

  SELECT *

FROM (

SELECT
            currenttab.waybill_no,
            currenttab.vehicle_tag_no,
            currenttab.bag_no,
            option.option_name AS operation_type,
            currenttab.operation_branch_name AS operation_branch_name,
            currenttab.recipient_city_name,
            DATETIME(currenttab.record_time,'Asia/Jakarta') AS record_time,
            LAG(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name,
            LAG(DATETIME(currenttab.record_time,'Asia/Jakarta'),1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_scan_time,
            LEAD(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name,
            LEAD(DATETIME(currenttab.record_time,'Asia/Jakarta')) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_scan_time,
            LEAD(option.option_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_scan_type,
           
            FROM
                `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab
                LEFT JOIN `datawarehouse_idexp.system_option` AS option ON currenttab.operation_type = option.option_value AND option.type_option = 'operationType'
                                                
            -- WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2024-02-10' AND '2024-02-16' 
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))
            AND currenttab.deleted = '0'
            
ORDER BY record_time DESC
            )

),

get_waybill_data AS (

  SELECT 
  
  sc.*,
  ww.order_no,
  ww.pickup_branch_name,
  ww.sender_district_name,
  ww.sender_city_name,
  ww.sender_province_name,
  DATETIME(ww.shipping_time,'Asia/Jakarta') pickup_time,
  ww.recipient_district_name,
  ww.recipient_city_name,
  ww.recipient_province_name,
  DATETIME(ww.pod_record_time,'Asia/Jakarta') pod_record_time,
  ww.item_calculated_weight,


  FROM get_awb_sending_3pl sc
  LEFT JOIN `datawarehouse_idexp.waybill_waybill` ww ON sc.waybill_no = ww.waybill_no
      AND DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -65 DAY))

),

get_arrival_mh_dest AS (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_2,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_dest_arrival,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_dest_arrival_time,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_mh_dest,
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_arr_fm,
  MAX(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_branch_name_arr_fm,
  -- MAX(sc.previous_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_scan_time_arr_fm,


  FROM scan_record_main sc

  WHERE sc.operation_type = 'Arrival scan'
  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('TH','VH','VT','PD')
  -- AND SUBSTR(sc.previous_branch_name,1,2) IN ('MH','DC')
    -- AND DATETIME(sc.record_time,'Asia/Jakarta') < DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1

),

get_unloading_mh_dest AS (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_2,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_dest_unloading,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_dest_unloading_time,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_mh_dest,
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_arr_fm,
  MAX(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_branch_name_arr_fm,
  -- MAX(sc.previous_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) previous_scan_time_arr_fm,


  FROM scan_record_main sc

  WHERE sc.operation_type = 'Unloading scan'
  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('TH','VH','VT','PD')
  -- AND SUBSTR(sc.previous_branch_name,1,2) IN ('MH','DC')
    -- AND DATETIME(sc.record_time,'Asia/Jakarta') < DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1

),

get_sending_mh_dest AS (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_3,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_dest_name,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) sending_time_mh_dest,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_name,
  -- MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_mh_dest,


  FROM scan_record_main sc

  WHERE sc.operation_type = 'Sending scan'

  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  -- AND SUBSTR(sc.next_location_name,1,2) IN ('TH','VH','VT','PD')


QUALIFY ROW_NUMBER() OVER(PARTITION BY waybill_no ORDER BY record_time DESC)=1

),

get_sending_3pl_information AS (

-- SELECT * FROM get_awb_sending_3pl
SELECT

ww.waybill_no,
ww.order_no,
ww.operation_branch_name,
ww.next_location_sc next_location,
a.mh_dest_arrival_time arrival_time_branch,
b.mh_dest_unloading_time unloading_time_branch,
ww.record_time sending_time_to_3pl,
ww.pickup_branch_name,
ww.sender_district_name,
ww.sender_city_name,
ww.sender_province_name,
ww.pickup_time,
ww.recipient_district_name,
ww.recipient_city_name,
ww.recipient_province_name,
ww.pod_record_time,
ww.item_calculated_weight,


FROM get_waybill_data ww
LEFT JOIN get_arrival_mh_dest a ON ww.waybill_no = a.waybill_no
LEFT JOIN get_unloading_mh_dest b ON ww.waybill_no = b.waybill_no
)

SELECT * FROM get_sending_3pl_information
