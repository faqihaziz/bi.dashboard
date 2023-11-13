    SELECT 
pickup_date,
month_pickup,
week_pickup,
input_date,
month_order_oo,
month_order,
week_order_oo,
week_order,

waybill_source,
COUNT(waybill_no) total_volume,
SUM(standard_shipping_fee) standard_shipping_fee,

FROM (

      SELECT
      waybill_no,
      origin_city,
      dest_city,
      waybill_source,
      pickup_date,
      month_pickup,
      week_pickup,
      standard_shipping_fee,

      input_date_oo,
      CASE WHEN input_date_oo IS NULL THEN pickup_date ELSE input_date_oo END AS input_date,
      month_order_oo,
      CASE WHEN month_order_oo IS NULL THEN month_pickup ELSE month_order_oo END AS month_order,
      week_order_oo,
      CASE WHEN week_order_oo IS NULL THEN week_pickup ELSE week_order_oo END AS week_order,


FROM (

      SELECT
ww.waybill_no,
ww.sender_city_name AS origin_city,
ww.recipient_city_name AS dest_city,
sr.option_name AS waybill_source,
ww.standard_shipping_fee,
DATE(ww.shipping_time, 'Asia/Jakarta') pickup_date,
FORMAT_DATE("%b %Y", DATE(ww.shipping_time,'Asia/Jakarta')) AS month_pickup,
EXTRACT(WEEK FROM DATETIME(ww.shipping_time, 'Asia/Jakarta')) week_pickup,

DATE(oo.input_time, 'Asia/Jakarta') input_date_oo,
FORMAT_DATE("%b %Y", DATE(oo.input_time,'Asia/Jakarta')) AS month_order_oo,
EXTRACT(WEEK FROM DATETIME(oo.input_time, 'Asia/Jakarta')) week_order_oo,

FROM `datawarehouse_idexp.waybill_waybill` ww
LEFT OUTER JOIN `datawarehouse_idexp.order_order` oo ON ww.waybill_no = oo.waybill_no
AND  DATE(oo.input_time, 'Asia/Jakarta') >= '2022-09-01'
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'


WHERE DATE(ww.shipping_time, 'Asia/Jakarta') BETWEEN '2023-01-01' AND CURRENT_DATE('Asia/Jakarta')
AND ww.void_flag = "0"

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1

)
)

GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY pickup_date ASC, week_pickup ASC, total_volume DESC
