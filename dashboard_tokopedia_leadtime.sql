WITH tokopedia_data AS (
  
SELECT 

tp.waybill_no,
tp.ecommerce_order_no,
tp.order_no,
t1.option_name order_source,
t2.option_name service_type,
tp.input_time,
tp.request_pickup_time,

-- tp.scheduling_or_pickup_branch,
tp.pickup_record_time,
tp.delivery_type,
tp.role_miles,
oo.sender_name,

tp.origin_province,
tp.origin_city,
tp.scheduling_or_pickup_branch,
tp.destination_province,
tp.destination_city,
tp.delivery_or_pod_branch,
tp.waybill_status,
tp.delivered_time,
tp.route_category,


FROM `datamart_idexp.dashboard_tokopedia` tp
LEFT OUTER JOIN `datawarehouse_idexp.order_order` oo ON tp.waybill_no = oo.waybill_no
AND DATE(oo.input_time,'Asia/Jakarta') >= (DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY)))
LEFT JOIN `datawarehouse_idexp.system_option` t1 ON oo.order_source = t1.option_value AND t1.type_option = 'orderSource'
LEFT JOIN `datawarehouse_idexp.system_option` t2 ON oo.service_type = t2.option_value AND t2.type_option = 'serviceType'

WHERE DATE(tp.input_time) >= (DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))) --BETWEEN '2024-01-01' AND '2024-01-09'

),

scan_record_main AS (

SELECT
            currenttab.waybill_no,
            option.option_name AS operation_type,
            currenttab.operation_branch_name AS operation_branch_name,
            DATETIME(currenttab.record_time,'Asia/Jakarta') AS record_time,
            LAG(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name,
            LAG(DATETIME(currenttab.record_time,'Asia/Jakarta'),1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_scan_time,
            LAG(currenttab.operation_branch_name,2) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name_2,
            LEAD(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_location_name,
            LEAD(DATETIME(currenttab.record_time,'Asia/Jakarta')) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_scan_time,
            LEAD(option.option_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_scan_type,
            MAX(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time DESC) AS last_location,
            
            FROM
                `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab
                LEFT JOIN `datawarehouse_idexp.system_option` AS option ON currenttab.operation_type = option.option_value AND option.type_option = 'operationType'
                LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON currenttab.waybill_no = rr.waybill_no
            AND currenttab.record_time < rr.return_confirm_record_time
                                                
            WHERE DATE(currenttab.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))
            AND currenttab.deleted = '0'
),

get_sending_fm AS (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_1,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) sending_branch,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) sending_time,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  MIN(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_scan_time_sending_fm,


  FROM scan_record_main sc
--   LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no

  WHERE sc.operation_type = 'Sending scan'
--   AND SUBSTR(operation_branch_name,1,2) IN ('TH','VH','VT','PD')
--   AND sc.record_time <= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')
  AND SUBSTR(sc.next_location_name,1,2) IN ('MH','DC')


QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

get_arrival_fm AS (

  SELECT
  sc.waybill_no,
  MIN(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) scan_type_2,
  MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) mh_arrival,
  MIN(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) mh_arrival_time,
  MIN(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_location_name,
  MIN(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) next_scan_time_arr_fm,
  MIN(sc.previous_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) previous_branch_name_arr_fm,
  MIN(sc.previous_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) previous_scan_time_arr_fm,


  FROM scan_record_main sc
  LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
    AND sc.record_time <= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')
  
  WHERE sc.operation_type = 'Arrival scan'
  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  AND SUBSTR(sc.previous_branch_name,1,2) IN ('TH','VH','VT','PD','MH')

QUALIFY ROW_NUMBER() OVER(PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

get_sending_mh_dest AS (

  SELECT
  sc.waybill_no,
  MAX(sc.operation_type) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) scan_type_3,
  MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) mh_sending_branch,
  MAX(sc.record_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) sending_time_to_th_dest,
  MAX(sc.next_location_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_location_name,
  MAX(sc.next_scan_time) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) next_scan_time_mh_dest,


  FROM scan_record_main sc
  LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sc.waybill_no = rr.waybill_no
    AND sc.record_time <= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  WHERE sc.operation_type = 'Sending scan'

  AND SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC')
  AND SUBSTR(sc.next_location_name,1,2) IN ('TH','VH','VT','PD')


QUALIFY ROW_NUMBER() OVER(PARTITION BY waybill_no ORDER BY record_time DESC)=1

),

arrival_dest AS (
  
  SELECT 
b.waybill_no,
b.waybill_source,
b.arrival_time arrival_time_dest,
-- b.arrival_time_dest
b.th_arrival,
b.pod_time,


-- FROM `dev_idexp.temp_table_tokopedia_arrival_dest` b
FROM `datamart_idexp.dashboard_productivity_lm` b
),

gabung_all_1 AS (

SELECT 

waybill_no,
ecommerce_order_no,
order_no,
order_source,
sender_name,
input_time,
request_pickup_time,
service_type,
origin_province,
origin_city,
scheduling_or_pickup_branch,
pickup_record_time,
delivery_type,
role_miles,
destination_province,
destination_city,
route_category,
origin_area,
destination_area,
delivery_or_pod_branch,
waybill_status,
fm_sending_branch,
fm_sending_time,
next_scan_time_sending_fm,
mh_ori_arrival,
mh_ori_arrival_time,
next_scan_time_arr_fm,
previous_branch_name_arr_fm, --kolom bantu
previous_scan_time_arr_fm, --kolom bantu
mh_sending_branch,
sending_time_to_th_dest,
next_scan_time_mh_dest,
arrival_time_dest,
th_arrival,
delivered_time,

FROM (

  SELECT 

tp.waybill_no,
tp.ecommerce_order_no,
tp.order_no,
tp.order_source,
tp.sender_name,
tp.input_time,
tp.request_pickup_time,
tp.service_type,
tp.origin_province,
tp.origin_city,
tp.scheduling_or_pickup_branch,
-- tp.scheduling_or_pickup_branch,
tp.pickup_record_time,
tp.delivery_type,
tp.role_miles,
tp.destination_province,
tp.destination_city,
tp.route_category,

CASE WHEN origin_city IN ("BEKASI","BOGOR","JAKARTA BARAT","JAKARTA PUSAT","JAKARTA SELATAN","JAKARTA TIMUR","JAKARTA UTARA","KEPULAUAN SERIBU","KOTA BEKASI","KOTA BOGOR","KOTA DEPOK","KOTA TANGERANG","TANGERANG","TANGERANG SELATAN") THEN 'Jabodetabek'
      ELSE 'Non Jabodetabek' END AS origin_area,
  CASE WHEN destination_city IN ("BEKASI","BOGOR","JAKARTA BARAT","JAKARTA PUSAT","JAKARTA SELATAN","JAKARTA TIMUR","JAKARTA UTARA","KEPULAUAN SERIBU","KOTA BEKASI","KOTA BOGOR","KOTA DEPOK","KOTA TANGERANG","TANGERANG","TANGERANG SELATAN") THEN 'Jabodetabek'
      ELSE 'Non Jabodetabek' END AS destination_area,

tp.delivery_or_pod_branch,
tp.waybill_status,

CASE
    WHEN c.sending_time > mh_arrival_time THEN d.previous_branch_name_arr_fm
    ELSE c.sending_branch 
    END AS fm_sending_branch,
-- c.sending_branch fm_sending_branch,
CASE
    WHEN c.sending_time > d.mh_arrival_time THEN d.previous_scan_time_arr_fm
    ELSE c.sending_time 
    END AS fm_sending_time,
-- c.sending_time fm_sending_time,
c.next_scan_time_sending_fm,

CASE 
    WHEN tp.scheduling_or_pickup_branch LIKE '%TH VIP%' THEN d.previous_branch_name_arr_fm
    ELSE d.mh_arrival 
    END AS mh_ori_arrival,
-- d.mh_arrival mh_ori_arrival,
CASE 
    WHEN tp.scheduling_or_pickup_branch LIKE '%TH VIP%' THEN d.previous_scan_time_arr_fm
    ELSE d.mh_arrival_time 
    END AS mh_ori_arrival_time,
-- d.mh_arrival_time mh_ori_arrival_time,
d.next_scan_time_arr_fm,
d.previous_branch_name_arr_fm,
d.previous_scan_time_arr_fm,

e.mh_sending_branch,
e.sending_time_to_th_dest,
e.next_scan_time_mh_dest,

b.arrival_time_dest,
b.th_arrival,
tp.delivered_time,



FROM tokopedia_data tp
LEFT OUTER JOIN arrival_dest b ON tp.waybill_no = b.waybill_no
LEFT OUTER JOIN get_sending_fm c ON tp.waybill_no = c.waybill_no
LEFT OUTER JOIN get_arrival_fm d ON tp.waybill_no = d.waybill_no
LEFT OUTER JOIN get_sending_mh_dest e ON tp.waybill_no = e.waybill_no
)
),

gabung_semua AS (

    SELECT 

waybill_no,
ecommerce_order_no,
order_no,
order_source,
sender_name,
input_time,
request_pickup_time,
service_type,
origin_province,
origin_city,
scheduling_or_pickup_branch,
pickup_record_time,
delivery_type,
role_miles,
destination_province,
destination_city,
route_category,
origin_area,
destination_area,
delivery_or_pod_branch,
waybill_status,
fm_sending_branch,
fm_sending_time,
next_scan_time_sending_fm,
mh_ori_arrival,
mh_ori_arrival_time,
next_scan_time_arr_fm,
previous_branch_name_arr_fm, --kolom bantu
previous_scan_time_arr_fm, --kolom bantu
mh_sending_branch,
sending_time_to_th_dest,
next_scan_time_mh_dest,
arrival_time_dest,
th_arrival,
delivered_time,
DATETIME_DIFF(pickup_record_time, request_pickup_time, SECOND) AS leadtime_rpu_to_pickup, --rpu_to_pickup
DATETIME_DIFF(fm_sending_time, pickup_record_time, SECOND) AS leadtime_pickup_to_sending_fm, --leadtime_fm,
DATETIME_DIFF(mh_ori_arrival_time, fm_sending_time, SECOND) AS leadtime_fm_to_arr_mh_ori, --fm_to_mm
DATETIME_DIFF(sending_time_to_th_dest, mh_ori_arrival_time, SECOND) leadtime_arr_mh_ori_to_sending_to_th_dest, --leadtime_mm
DATETIME_DIFF(arrival_time_dest, sending_time_to_th_dest,SECOND) leadtime_sending_mh_dest_to_arr_th_dest, --leadtime_mm_to_lm
DATETIME_DIFF(delivered_time, arrival_time_dest,SECOND) leadtime_arr_dest_to_pod, --leadtime_lm

-------testing new logic---------------

-- CASE 
--     WHEN (DATETIME_DIFF(mh_ori_arrival_time, fm_sending_time, SECOND)) < 0 THEN (DATETIME_DIFF(next_scan_time_arr_fm, fm_sending_time, SECOND))
--     ELSE (DATETIME_DIFF(mh_ori_arrival_time, fm_sending_time, SECOND))
--     END AS check_leadtime_fm_to_mm,

-- CASE 
--     WHEN (DATETIME_DIFF(sending_time_to_th_dest, mh_ori_arrival_time, SECOND)) < 0 THEN (DATETIME_DIFF(next_scan_time_mh_dest, mh_ori_arrival_time, SECOND))
--     ELSE (DATETIME_DIFF(sending_time_to_th_dest, mh_ori_arrival_time, SECOND))
--     END AS check_leadtime_mm,


FROM gabung_all_1
)

SELECT 

waybill_no,
ecommerce_order_no,
order_no,
order_source,
sender_name,
input_time,
request_pickup_time,
service_type,
origin_province,
origin_city,
scheduling_or_pickup_branch,
pickup_record_time,
delivery_type,
role_miles,
destination_province,
destination_city,
route_category,
origin_area,
destination_area,
delivery_or_pod_branch,
waybill_status,
fm_sending_branch,
fm_sending_time,
-- next_scan_time_sending_fm,
mh_ori_arrival,
mh_ori_arrival_time,
-- next_scan_time_arr_fm,
-- previous_branch_name_arr_fm, --kolom bantu
-- previous_scan_time_arr_fm, --kolom bantu
mh_sending_branch,
sending_time_to_th_dest,
-- next_scan_time_mh_dest,
arrival_time_dest,
th_arrival,
delivered_time,
leadtime_rpu_to_pickup, --rpu_to_pickup
leadtime_pickup_to_sending_fm, --leadtime_fm,
CASE 
    WHEN leadtime_fm_to_arr_mh_ori < 0 THEN 0
    ELSE leadtime_fm_to_arr_mh_ori
    END AS leadtime_fm_to_arr_mh_ori, --fm_to_mm
CASE
    WHEN leadtime_arr_mh_ori_to_sending_to_th_dest < 0 THEN 0
    ELSE leadtime_arr_mh_ori_to_sending_to_th_dest
    END AS leadtime_arr_mh_ori_to_sending_to_th_dest, --leadtime_mm
leadtime_sending_mh_dest_to_arr_th_dest, --leadtime_mm_to_lm
leadtime_arr_dest_to_pod, --leadtime_lm


FROM gabung_semua
