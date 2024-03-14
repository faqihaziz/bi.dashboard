WITH root_vehicle_tag AS (

  SELECT *

  FROM (

SELECT
  sc.vehicle_tag_no,
  sc.bag_no,
  sc.waybill_no,
  DATETIME(sc.record_time, 'Asia/Jakarta') AS record_time,
  CONCAT(sc.vehicle_tag_no," ","-"," ",sc.bag_no," ","-"," ",sc.waybill_no) vm_bm_awb,


FROM `datawarehouse_idexp.waybill_waybill_line` sc
  LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON sc.operation_type = rd1.option_value AND rd1.type_option = 'operationType'

WHERE DATE(sc.record_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))

-- AND sc.bag_no NOT IN ('')
-- AND sc.vehicle_tag_no IN ("VA0630039536")


QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no)=1
)
-- WHERE vm_bm_awb IS NOT NULL

QUALIFY ROW_NUMBER() OVER (PARTITION BY vm_bm_awb)=1
)

SELECT

sc.record_time,
sc.bag_no,
sc.waybill_no,
ww.kota_asal,
ww.kec_asal,
ww.kota_tujuan,
ww.berat,
ww.biaya_standar_pengiriman standard_shipping_fee,
ww.tipe_ekspress express_type,

sc.vehicle_tag_no, --tambah kolom
ww.tujuan_kec, --tambah kolom
ww.order_no, --tambah kolom
sc.vm_bm_awb, --tambah kolom


FROM root_vehicle_tag sc
LEFT JOIN `datamart_idexp.dashboard_bd_order_all_status` ww ON sc.waybill_no = ww.waybill_no

WHERE DATE(sc.record_time) >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))
