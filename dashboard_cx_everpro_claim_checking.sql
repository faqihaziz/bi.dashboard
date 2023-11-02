
WITH cx_everpro_claim_checking AS (
SELECT 

ww.waybill_no,
ww.order_no,
ww.ecommerce_order_no,
DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time ,
DATETIME(ww.pod_record_time,'Asia/Jakarta') pod_record_time,
ww.pod_branch_name,
t0.option_name AS waybill_source,
ww.parent_shipping_cleint vip_username,
ww.vip_customer_name sub_account,
rd16.option_name AS void_status,

MAX(DATETIME(rr.return_record_time,'Asia/Jakarta')) OVER (PARTITION BY rr.waybill_no ORDER BY rr.return_record_time DESC) AS return_regist_time,
MAX(DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')) OVER (PARTITION BY rr.waybill_no ORDER BY rr.return_record_time DESC) AS return_confirm_time,
MAX(rc.option_name) OVER (PARTITION BY rr.waybill_no ORDER BY rr.return_record_time DESC) AS return_confirm_status,
DATETIME(rr.return_pod_record_time,'Asia/Jakarta') return_pod_record_time,
rr.return_pod_photo_url,

ww.sender_city_name,
ww.recipient_city_name,
ww.pod_photo_url,

CASE
      WHEN ww.pod_branch_name IS NOT NULL THEN pod_branch_name
      WHEN ww.pod_branch_name IS NULL THEN mb.branch_name
      END AS th_destination,
ww.pickup_branch_name,

ww.standard_shipping_fee,
ww.total_shipping_fee,
ww.cod_amount,
ww.item_value,
ww.insurance_amount,
ww.handling_fee,
ww.other_fee,


FROM `datawarehouse_idexp.waybill_waybill` ww
LEFT OUTER JOIN `dev_idexp.masterdata_branch_coverage_th` mb ON ww.recipient_district_id = mb.district_id
LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON ww.waybill_no = rr.waybill_no
AND DATE(rr.update_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = ww.void_flag AND rd16.type_option = 'voidFlag'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd1 ON rd1.option_value = ww.return_flag AND rd1.type_option = 'returnFlag'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t0 ON t0.option_value = ww.waybill_source AND t0.type_option = 'waybillSource'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = ww.waybill_status AND t1.type_option = 'waybillStatus'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rc ON rc.option_value = rr.return_confirm_status AND rc.type_option = 'returnConfirmStatus'
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.return_type` t5 ON rr.return_type_id = t5.id AND t5.deleted=0
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu3 ON rr.recipient_city_name = pu3.city and rr.recipient_province_name = pu3.province --Return_area_register, 
LEFT OUTER JOIN `datamart_idexp.masterdata_facility_to_kanwil` fk ON rr.return_branch_name = fk.branch_name
-- LEFT OUTER JOIN `datawarehouse_idexp.system_option` t0 ON t0.option_value = ww.waybill_source AND t0.type_option = 'waybillSource'


WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))
AND t0.option_name IN ('everpro')

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1

),


first_pos_photo as(
  SELECT
        ps.waybill_no,
        MIN(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) first_pos_reason,
        MIN(ps.operation_branch_name) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) first_pos_location,
        MIN(DATETIME(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) AS first_pos_attempt,
        MIN(prt.option_name) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) first_pos_type,

        MIN(sc.photo_url) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_pos_photo_url,
        -- MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_pos_location,

              FROM `datawarehouse_idexp.waybill_problem_piece` ps
              LEFT OUTER JOIN `datawarehouse_idexp.waybill_waybill_line` sc ON ps.waybill_no = sc.waybill_no
              AND DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY)) AND sc.operation_type IN ('18') AND sc.problem_type NOT IN ('02')
              
              LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on sc.problem_code = t4.code and t4.deleted = '0'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = sc.problem_type AND t1.type_option = 'problemType'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` prt ON ps.problem_type  = prt.option_value AND prt.type_option = 'problemType'
              WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))

              AND ps.problem_type NOT IN ('02')

              QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no ORDER BY sc.record_time ASC)=1
        ),

pos_attempt_rank as(

  SELECT
ps.waybill_no,
ps.pos_attempt_1,
ps.pos_reason_1,

ps.pos_attempt_2,
ps.pos_reason_2,

ps.pos_attempt_3,
ps.pos_reason_3,

FROM (
  SELECT
        waybill_no,
        MAX(IF(id = 1, DATETIME(operation_time), NULL)) AS pos_attempt_1,
        MAX(IF(id = 2, DATETIME(operation_time), NULL)) AS pos_attempt_2,
        MAX(IF(id = 3, DATETIME(operation_time), NULL)) AS pos_attempt_3,

        MAX(IF(id = 1, problem_reason, NULL)) AS pos_reason_1,
        MAX(IF(id = 2, problem_reason, NULL)) AS pos_reason_2,
        MAX(IF(id = 3, problem_reason, NULL)) AS pos_reason_3,

        FROM (
              SELECT ps.waybill_no, 
              DATETIME(ps.operation_time,'Asia/Jakarta') operation_time,
              ps.problem_reason, 
                        
              RANK() OVER (PARTITION BY ps.waybill_no ORDER BY DATETIME(ps.operation_time, 'Asia/Jakarta') ASC ) AS id

              FROM `datawarehouse_idexp.waybill_problem_piece` ps

              WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))
              AND ps.problem_type NOT IN ('02')
        ) 

        GROUP BY 1 
) ps
QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no)=1
),

pos_attempt_photo as(

  SELECT
ps.waybill_no,
ps.pos_photo_url_1,
ps.pos_photo_url_2,
ps.pos_photo_url_3,

FROM (
  SELECT
        waybill_no,
        MAX(IF(id = 1, DATETIME(record_time), NULL)) AS pos_attempt_1,
        MAX(IF(id = 2, DATETIME(record_time), NULL)) AS pos_attempt_2,
        MAX(IF(id = 3, DATETIME(record_time), NULL)) AS pos_attempt_3,

        MAX(IF(id = 1, photo_url, NULL)) AS pos_photo_url_1,
        MAX(IF(id = 2, photo_url, NULL)) AS pos_photo_url_2,
        MAX(IF(id = 3, photo_url, NULL)) AS pos_photo_url_3,

        FROM (
              SELECT sc.waybill_no, 
              DATETIME(sc.record_time,'Asia/Jakarta') record_time, 
              sc.photo_url,
                        
              RANK() OVER (PARTITION BY sc.waybill_no ORDER BY DATETIME(sc.record_time, 'Asia/Jakarta') ASC ) AS id
              FROM `datawarehouse_idexp.waybill_waybill_line` sc

              WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))
            AND sc.operation_type IN ('18') AND sc.problem_type NOT IN ('02') 
        ) 

        GROUP BY 1 
) ps
QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no)=1
),


first_deliv_attempt AS (

SELECT
sc.waybill_no,
MIN(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_deliv_attempt,
MIN(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS scan_type,

FROM `datawarehouse_idexp.waybill_waybill_line` sc 
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

WHERE 
DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
AND operation_type = "09"

QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

last_location as(
  SELECT
        sc.waybill_no,
        MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_location, 
        MAX(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_activity, 
        MAX(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_scan_time,

              FROM `datawarehouse_idexp.waybill_waybill_line` sc
              LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on sc.problem_code = t4.code and t4.deleted = '0'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = sc.problem_type AND t1.type_option = 'problemType'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

              WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

              QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1
        )


SELECT 

  waybill_no,
  shipping_time,
  sender_city_name,
  recipient_city_name,
  void_status,

  return_regist_time,
  return_confirm_time,
  return_pod_record_time,
 
  first_pos_attempt,
  pod_record_time,
 
  first_pos_photo_url,
  pod_photo_url,
 
  first_pos_reason,
  first_pos_location,

  waybill_source,
--   return_pod_photo_url,
  vip_username,
  sub_account,

first_deliv_attempt,
pickup_branch_name,
th_destination,

pos_attempt_2,
pos_reason_2,
pos_photo_url_2,

pos_attempt_3,
pos_reason_3,
pos_photo_url_3,

  standard_shipping_fee,
  total_shipping_fee,
  cod_amount,
  item_value,
insurance_amount, 
handling_fee,
other_fee,

last_location, --tambah kolom
last_activity, --tambah kolom
last_scan_time, --tambah kolom

FROM (
  SELECT 

  cx.waybill_no,
  cx.shipping_time,
  cx.order_no,
  cx.ecommerce_order_no,

  cx.pod_branch_name,
  cx.pod_record_time,
  cx.waybill_source,
  cx.vip_username,
  cx.sub_account,
  cx.void_status,
  cx.pod_photo_url,

  cx.return_regist_time,
  cx.return_confirm_time,
  cx.return_pod_photo_url,
  cx.return_pod_record_time,

  fp.first_pos_reason,
  fp.first_pos_type,
  fp.first_pos_attempt,
  fp.first_pos_photo_url,
  fp.first_pos_location,

  cx.sender_city_name,
  cx.recipient_city_name,

  fd.first_deliv_attempt,
  cx.pickup_branch_name,
  cx.th_destination,

  pr.pos_attempt_2,
  pr.pos_reason_2,
  pa.pos_photo_url_2,

  pr.pos_attempt_3,
  pr.pos_reason_3,
  pa.pos_photo_url_3,

  cx.standard_shipping_fee,
  cx.total_shipping_fee,
  cx.cod_amount,
  cx.item_value,
  cx.insurance_amount,
  cx.handling_fee,
  cx.other_fee,

  ll.last_location, --tambah kolom
  ll.last_activity, --tambah kolom
  ll.last_scan_time, --tambah kolom


FROM cx_everpro_claim_checking cx
LEFT OUTER JOIN first_pos_photo fp ON cx.waybill_no = fp.waybill_no
LEFT OUTER JOIN first_deliv_attempt fd ON cx.waybill_no = fd.waybill_no
LEFT OUTER JOIN pos_attempt_rank pr ON cx.waybill_no = pr.waybill_no
LEFT OUTER JOIN pos_attempt_photo pa ON cx.waybill_no = pa.waybill_no
LEFT OUTER JOIN last_location ll ON cx.waybill_no = ll.waybill_no
)
