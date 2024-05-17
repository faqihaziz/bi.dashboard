WITH zalora_data AS
                    (
                    SELECT 
                    oo.waybill_no,
                    oo.order_no AS package_number,
                    oo.order_no AS no_order,
                    DATETIME(oo.input_time,'Asia/Jakarta') input_time,
                    DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
                    oo.sender_name AS seller_name,
                    oo.recipient_name AS customer_name,
                    oo.recipient_address AS customer_address,
                    oo.recipient_city_name AS shipping_city,
                    oo.recipient_cellphone AS cust_phone_no,
                    oo.recipient_province_name AS region,

                    oo.sender_city_name AS origin_city,
                    ww.item_calculated_weight AS chargeable_weight_by_3PL,
                    ww.item_calculated_weight AS real_weight,
                    ww.item_calculated_weight AS volumetric_kg,

                    --OnTime_Delivery

                    --Status_POD

                    CASE WHEN DATE(ww.shipping_time, 'Asia/Jakarta') IS NULL AND t2.option_name NOT IN ('Cancel Order','Picked Up') THEN "Not Picked Up Yet"
                        WHEN DATE(ww.shipping_time, 'Asia/Jakarta') IS NOT NULL AND DATE(ww.pod_record_time, 'Asia/Jakarta') IS NOT NULL THEN "Delivered"
                        WHEN DATE(ww.shipping_time, 'Asia/Jakarta') IS NOT NULL AND DATE(ww.pod_record_time, 'Asia/Jakarta') IS NOT NULL AND DATETIME(rr.return_confirm_record_time, 'Asia/Jakarta') IS NULL AND DATETIME(rr.return_pod_record_time, 'Asia/Jakarta') IS NULL THEN "Delivered"
                        WHEN DATE(ww.shipping_time, 'Asia/Jakarta') IS NOT NULL AND DATE(ww.pod_record_time, 'Asia/Jakarta') IS NOT NULL AND DATETIME(rr.return_confirm_record_time, 'Asia/Jakarta') IS NOT NULL AND DATETIME(rr.return_pod_record_time, 'Asia/Jakarta') IS NULL THEN "Delivered"
                        WHEN DATE(ww.shipping_time, 'Asia/Jakarta') IS NULL AND t2.option_name IN ('Cancel Order') THEN "Cancel Order"
                        WHEN DATE(ww.shipping_time, 'Asia/Jakarta') IS NOT NULL AND DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND DATETIME(rr.return_confirm_record_time, 'Asia/Jakarta') IS NULL AND DATE(rr.return_pod_record_time, 'Asia/Jakarta') IS NULL THEN "Delivery Process"
                        WHEN DATE(ww.shipping_time, 'Asia/Jakarta') IS NOT NULL AND DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND DATETIME(rr.return_confirm_record_time, 'Asia/Jakarta') IS NOT NULL AND DATE(rr.return_pod_record_time, 'Asia/Jakarta') IS NULL THEN "Return Process"
                        WHEN DATE(ww.shipping_time, 'Asia/Jakarta') IS NOT NULL AND DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND DATETIME(rr.return_confirm_record_time, 'Asia/Jakarta') IS NOT NULL AND DATE(rr.return_pod_record_time, 'Asia/Jakarta') IS NOT NULL THEN "Returned"
                        END AS pod_status,

                    DATETIME(ww.pod_record_time,'Asia/Jakarta') pod_date,
                    ww.signer AS receiver,

                    MAX(DATETIME(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS POS_attempt,
                    MAX(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS problem_reason_remarks,

                    DATETIME(rr.return_confirm_record_time, 'Asia/Jakarta')return_start_date,
                    DATETIME(rr.return_pod_record_time,'Asia/Jakarta') return_pod_date,
                    rr.return_signer AS penerima_return,

                    sr.option_name AS Order_Source,

                    CASE 
                    WHEN ww.recipient_province_name IN ('DKI JAKARTA') THEN "JABODETABEK"
                    WHEN ww.recipient_province_name NOT IN ('DKI JAKARTA') AND ww.recipient_city_name IN ("BOGOR","KOTA BOGOR","KOTA DEPOK","KOTA TANGERANG",
                    "TANGERANG","TANGERANG SELATAN","BEKASI","KOTA BEKASI") THEN "JABODETABEK"
                    WHEN ww.recipient_province_name NOT IN ('DKI JAKARTA') AND (ww.recipient_city_name NOT IN ("BOGOR","KOTA BOGOR","KOTA DEPOK","KOTA TANGERANG",
                    "TANGERANG","TANGERANG SELATAN","BEKASI","KOTA BEKASI") OR ww.recipient_province_name IN ('JAWA BARAT','JAWA TENGAH','JAWA TIMUR','DI YOGYAKARTA')) THEN "JAWA NON-JABODETABEK" 
                    WHEN ww.recipient_province_name IN ("PAPUA BARAT","MALUKU UTARA","PAPUA","MALUKU","SULAWESI TENGAH","SULAWESI UTARA","SULAWESI TENGGARA","SULAWESI BARAT","SULAWESI SELATAN",
                    "GORONTALO","KALIMANTAN TENGAH","KALIMANTAN TIMUR","KALIMANTAN BARAT","KALIMANTAN UTARA","KALIMANTAN SELATAN","BALI","NTB","NTT","RIAU","SUMATERA BARAT","SUMATERA UTARA",
                    "KEP. RIAU","D.I. ACEH","SUMATERA SELATAN","LAMPUNG","BENGKULU","JAMBI","KEP. BANGKA BELITUNG") THEN "LUAR JAWA"
                    END AS city_category,

                    ww.recipient_city_name AS dest_city,
                    ww.recipient_province_name AS dest_prov,

                    cast(t6.sla as INTEGER) AS SLA_Delivery,
                    DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) AS Due_Date_Delivery,

                    CASE 
                        WHEN cast(t6.sla as INTEGER) >= 999 THEN "No SLA (OoC)"
                        WHEN DATE(ww.pod_record_time,'Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Late"
                        WHEN DATE(ww.pod_record_time,'Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Not Late"
                        WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Not Late"
                        WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Late"
                        END AS Deliv_Performance,

                    FROM `datawarehouse_idexp.order_order`oo
                    LEFT join `grand-sweep-324604.datawarehouse_idexp.waybill_waybill` ww on oo.waybill_no = ww.waybill_no and ww.deleted = '0'
                    AND DATE(ww.shipping_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))
                    LEFT join `grand-sweep-324604.datawarehouse_idexp.waybill_problem_piece` ps on ww.waybill_no = ps.waybill_no AND ps.problem_type NOT IN ('02')
                    AND DATE(ps.operation_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY))
                    LEFT JOIN `grand-sweep-324604.datawarehouse_idexp.waybill_return_bill` rr ON ww.waybill_no = rr.waybill_no AND rr.deleted = '0'

                    left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on oo.order_source  = sr.option_value and sr.type_option = 'orderSource'
                    left join `grand-sweep-324604.datawarehouse_idexp.system_option` t1 on oo.payment_type  = t1.option_value and t1.type_option = 'paymentType'
                    left join `grand-sweep-324604.datawarehouse_idexp.system_option` t0 on oo.service_type  = t0.option_value and t0.type_option = 'serviceType'
                    left join `grand-sweep-324604.datawarehouse_idexp.system_option` t2 on oo.order_status  = t2.option_value and t2.type_option = 'orderStatus'
                    INNER JOIN `datamart_idexp.masterdata_sla_shopee` t6 ON ww.recipient_city_name = t6.destination_city and ww.sender_city_name = t6.origin_city

                    WHERE DATE(oo.input_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -35 DAY)) 

                    --AND sr.option_name IN ('pt fashion eservices indonesia')
                    AND sr.option_name IN ('pt fashion eservices indonesia','pt fashion marketplace indonesia')

                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1

                    ),

                    zalora_report AS (

                    SELECT
                    waybill_no,
                    package_number,
                    no_order,
                    input_time,
                    shipping_time,
                    seller_name,
                    customer_name,
                    customer_address,
                    shipping_city,
                    cust_phone_no,
                    region,
                    origin_city,
                    chargeable_weight_by_3PL,
                    real_weight,
                    volumetric_kg,

                    -------OTD-----------------
                    SLA_Delivery,
                    Due_Date_Delivery,
                    Deliv_Performance,

                    pod_status,
                    pod_date,
                    receiver,
                    POS_attempt,
                    problem_reason_remarks,
                    return_start_date,
                    return_pod_date,
                    penerima_return,
                    CASE
                    WHEN pod_status = 'Delivered' THEN 'POD'
                    ELSE pod_status  
                    END AS status_sistem,
                    order_source,
                    city_category,
                    dest_city,
                    dest_prov,

                    FROM zalora_data


                    )

                    SELECT 

                    waybill_no,
                    package_number,
                    no_order,
                    seller_name,
                    customer_name,
                    customer_address,
                    shipping_city,
                    cust_phone_no,
                    region,
                    origin_city,
                    chargeable_weight_by_3PL,
                    real_weight,
                    volumetric_kg,
                    shipping_time,

                    -------OTD-----------------
                    SLA_Delivery,
                    Due_Date_Delivery,
                    Deliv_Performance,

                    CASE
                    WHEN pod_date IS NOT NULL AND DATE(pod_date) <= DATE(due_date_delivery) THEN 'Not Late'
                    WHEN pod_date IS NOT NULL AND DATE(pod_date) > DATE(due_date_delivery) THEN 'Late'
                    WHEN pod_date IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(due_date_delivery) THEN 'Not Late'
                    WHEN pod_date IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(due_date_delivery) THEN 'Late'
                    END AS OTD_Status,

                    pod_status,
                    pod_date,
                    receiver,
                    POS_attempt,
                    problem_reason_remarks,
                    return_start_date,
                    return_pod_date,
                    penerima_return,
                    status_sistem,
                    order_source,
                    city_category,
                    dest_city,
                    dest_prov,
                    input_time,


                    FROM zalora_report
