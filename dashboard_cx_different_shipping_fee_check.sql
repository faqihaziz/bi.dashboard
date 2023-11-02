SELECT 

oo.waybill_no, 
t3.option_name AS order_source,
DATETIME(oo.input_time,'Asia/Jakarta') input_time,
DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
oo.sender_city_name AS order_sender_city_name, ww.sender_city_name AS waybill_sender_city_name,
oo.sender_district_name AS order_sender_district_name, ww.sender_district_name AS waybill_sender_district_name,
ww.sender_name AS order_sender_name,
ww.sender_address AS order_sender_address,
oo.recipient_city_name AS order_recipient_city_name, ww.recipient_city_name AS waybill_recipient_city_name,
oo.recipient_district_name AS order_recipient_district_name, ww.recipient_district_name AS waybill_recipient_district_name,
ww.recipient_address AS recipient_address,
ww.pod_photo_url,

CAST(oo.item_calculated_weight AS NUMERIC) AS order_item_calculated_weight,
CAST(ww.item_calculated_weight AS NUMERIC) AS waybill_item_calculated_weight,
-- CAST(oo.item_weight AS NUMERIC) AS order_item_weight,
CAST(oo.item_volume_weight AS NUMERIC) AS order_item_volume_weight,
-- CAST(ww.item_actual_weight AS NUMERIC) AS waybill_item_actual_weight,
CAST(ww.item_volume_weight AS NUMERIC) AS waybill_item_volume_weight,

oo.insurance_amount AS order_insurance,
ww.insurance_amount AS waybill_insurance,

t1.option_name AS order_express_type,
t2.option_name AS waybill_express_type,

oo.standard_shipping_fee AS order_std_shipping_fee,
ww.standard_shipping_fee AS waybill_std_shipping_fee,

oo.handling_fee AS order_handling_fee,
ww.handling_fee AS waybill_handling_fee,

oo.other_fee AS order_other_fee,
ww.other_fee AS waybill_other_fee,

oo.total_shipping_fee AS order_total_shipping_fee,
ww.total_shipping_fee AS waybill_total_shipping_fee,

CASE 
WHEN oo.sender_city_name <> ww.sender_city_name AND oo.sender_district_name <> ww.sender_district_name THEN 'Different Origin City & District'
WHEN oo.sender_district_name <> ww.sender_district_name THEN 'Different Origin District'
WHEN oo.sender_city_name <> ww.sender_city_name THEN 'Different Origin City' ELSE 'No Issue' END AS remark_origin,

CASE 
WHEN oo.recipient_city_name <> ww.recipient_city_name AND oo.recipient_district_name <> oo.recipient_district_name THEN 'Different Destination City & District'
WHEN oo.recipient_city_name <> ww.recipient_city_name THEN 'Different Destination City'
WHEN oo.recipient_district_name <> ww.recipient_district_name THEN 'Different Destination District'
ELSE 'No Issue' END AS remark_destination,

CASE WHEN CAST(oo.item_calculated_weight AS NUMERIC) <> CAST(ww.item_calculated_weight AS NUMERIC) AND CAST(oo.item_weight AS NUMERIC) <> CAST(ww.item_actual_weight AS NUMERIC) THEN 'Change in Weight (re-weigh)'
WHEN CAST(oo.item_calculated_weight AS NUMERIC) <> CAST(ww.item_calculated_weight AS NUMERIC) AND CAST(oo.item_volume_weight AS NUMERIC) <> CAST(ww.item_volume_weight AS NUMERIC) THEN 'Volumetric Weight'
ELSE 'No Issue' END AS remark_weight,

CASE WHEN oo.insurance_amount <> ww.insurance_amount THEN 'Different Insurance Amount' ELSE 'No Issue' END AS remark_insurance,
CASE WHEN t1.option_name <> t2.option_name THEN 'Different Express Type' ELSE 'No Issue' END AS remark_express_type,

FROM `datawarehouse_idexp.order_order` oo
LEFT JOIN `datawarehouse_idexp.waybill_waybill` ww ON ww.waybill_no = oo.waybill_no AND ww.deleted='0'
AND DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))
LEFT JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = oo.express_type AND t1.type_option = 'expressType'
LEFT JOIN `datawarehouse_idexp.system_option` t2 ON t2.option_value = ww.express_type AND t2.type_option = 'expressType'
LEFT JOIN `datawarehouse_idexp.system_option` t3 ON t3.option_value = oo.order_source AND t3.type_option = 'orderSource'

WHERE DATE(oo.input_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))

AND oo.waybill_no LIKE "%IDE%"
