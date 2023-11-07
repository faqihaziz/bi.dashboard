
WITH 

zalora_new_sla AS (
  SELECT 
    sender_location_id,
    recipient_location_id,
    express_type,
    shipping_client_id,
    discount_rate,
    min_sla,
    max_sla

 FROM `grand-sweep-324604.datawarehouse_idexp.standard_shipping_fee` 
 WHERE DATE(end_expire_time, 'Asia/Jakarta') > CURRENT_DATE('Asia/Jakarta')
      AND deleted = '0'

QUALIFY ROW_NUMBER() OVER (PARTITION BY search_code ORDER BY created_at DESC)=1
),

zalora_data AS
(
SELECT 
oo.waybill_no,
oo.order_no AS package_number,
oo.order_no AS no_order,

oo.input_time Waktu_Input,
oo.Start_Pickup_Time,
oo.Pickup_Time,
oo.End_pickup_time,
oo.Order_Status,
oo.Pickup_Failure_Reason,
oo.pickup_failure_time Pickup_Failure_Attempt,

DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
oo.Sender_Name AS seller_name,
-- oo.recipient_name AS customer_name,
-- oo.recipient_address AS customer_address,
ww.recipient_city_name AS dest_city,
-- oo.recipient_cellphone AS cust_phone_no,
ww.recipient_province_name AS dest_province,
ww.recipient_district_name dest_district,
CONCAT(ww.recipient_city_name,' ','-',' ',ww.recipient_district_name) AS Dest_City_District,

Order_Source,

oo.scheduling_or_pickup_branch Origin_Branch,
oo.sender_district_name Origin_District,
oo.sender_city_name Origin_City,
oo.sender_province_name Origin_Province,
CONCAT(oo.sender_city_name,' ','-',' ',oo.sender_district_name) AS Origin_City_District,
oo.Kanwil_Area_pickup,

oo.order_past_1500,
oo.Durasi_Pickup,
oo.Pickup_Category_everpro Pickup_Category,
CASE WHEN oo.Pickup_Performance_everpro = "On Time" THEN "Not Late"
WHEN oo.Pickup_Performance_everpro = "Not Picked Up" THEN "Not Late" 
WHEN oo.Pickup_Performance_everpro = "Late" THEN "Late" END AS Pickup_Performance,
oo.Pickup_Rate,
oo.Pickup_Status,
-- FM_Performance,
oo.Late_Pickup_Factor,
oo.Late_Pickup_Category,


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

MIN(DATETIME(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) AS POS_attempt,
MIN(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) AS problem_reason_remarks, 

DATETIME(rr.return_confirm_record_time, 'Asia/Jakarta')return_start_date,
DATETIME(rr.return_pod_record_time,'Asia/Jakarta') return_pod_date,
rr.return_signer AS penerima_return,

-- sr.option_name AS Order_Source,

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

zs.max_sla SLA_Delivery,
cast(t6.sla as INTEGER) AS SLA_Delivery_old,
zs.min_sla min_SLA_Delivery, 

DATE(DATE_ADD(ww.shipping_time, INTERVAL (zs.max_sla) day)) AS Due_Date_Delivery,
DATE(DATE_ADD(ww.shipping_time, INTERVAL (zs.max_sla) day)) AS Due_Date_Delivery_old,

---------- zalora deliv performance old ----------------------------
CASE 
    WHEN cast(t6.sla as INTEGER) >= 999 THEN "No SLA (OoC)"
    WHEN DATE(ww.pod_record_time,'Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Late"
    WHEN DATE(ww.pod_record_time,'Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Not Late"
    WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Not Late"
    WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Late"
    END AS Deliv_Performance_old,

---------- zalora deliv performance old ----------------------------
CASE 
    WHEN zs.max_sla >= 999 THEN "No SLA (OoC)"
    WHEN DATE(ww.pod_record_time,'Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (zs.max_sla) day)) THEN "Late"
    WHEN DATE(ww.pod_record_time,'Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (zs.max_sla) day)) THEN "Not Late"
    WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (zs.max_sla) day)) THEN "Not Late"
    WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (zs.max_sla) day)) THEN "Late"
    END AS Deliv_Performance,



FROM `datamart_idexp.dashboard_bd_kpi_monthly` oo
LEFT join `grand-sweep-324604.datawarehouse_idexp.waybill_waybill` ww on oo.waybill_no = ww.waybill_no and ww.deleted = '0'
AND DATE(ww.shipping_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -65 DAY))
LEFT join `grand-sweep-324604.datawarehouse_idexp.waybill_problem_piece` ps on ww.waybill_no = ps.waybill_no AND ps.problem_type NOT IN ('02')
AND DATE(ps.operation_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -65 DAY))
LEFT JOIN `grand-sweep-324604.datawarehouse_idexp.waybill_return_bill` rr ON ww.waybill_no = rr.waybill_no AND rr.deleted = '0'
INNER JOIN `datamart_idexp.masterdata_sla_shopee` t6 ON ww.recipient_city_name = t6.destination_city and ww.sender_city_name = t6.origin_city
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on oo.order_source  = sr.option_value and sr.type_option = 'orderSource'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` t2 on oo.order_status  = t2.option_value and t2.type_option = 'orderStatus' 
LEFT JOIN zalora_new_sla zs ON zs.shipping_client_id = ww.vip_customer_id 
              AND ww.sender_city_id = zs.sender_location_id 
              AND ww.recipient_district_id  = zs.recipient_location_id 
              AND ww.express_type = zs.express_type


WHERE DATE(oo.input_time) >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -65 DAY)) 

AND Order_Source IN ('pt fashion eservices indonesia','pt fashion marketplace indonesia')


-- AND oo.waybill_no IN ()

QUALIFY ROW_NUMBER() OVER (PARTITION BY oo.waybill_no ORDER BY oo.update_time_oo DESC)=1

),

zalora_report AS (

SELECT
ww.waybill_no,
ww.package_number,
ww.no_order,
ww.Waktu_Input,
ww.shipping_time,
ww.seller_name,
ww.Start_Pickup_Time,
ww.Pickup_Time,
ww.End_pickup_time,
ww.Order_Status,
ww.Pickup_Failure_Reason,
ww.Pickup_Failure_Attempt,
ww.Origin_Branch,
ww.Origin_District,
ww.Origin_City,
ww.Origin_Province,
ww.Origin_City_District,
ww.Dest_City_District,
-- Kanwil_Area_pickup,
ww.order_past_1500,
ww.Durasi_Pickup,
ww.Pickup_Category,
ww.Pickup_Performance,
ww.Pickup_Rate,
ww.Pickup_Status,
-- FM_Performance,
ww.Late_Pickup_Factor,
ww.Late_Pickup_Category,

ww.dest_city,
ww.dest_province,
ww.dest_district,


-------OTD-----------------
ww.min_SLA_Delivery,
ww.SLA_Delivery,
ww.SLA_Delivery_old,
ww.due_date_delivery,
ww.Deliv_Performance,
ww.Due_Date_Delivery_old,
ww.Deliv_Performance_old,

ww.pod_status,
ww.pod_date,
ww.receiver,
ww.POS_attempt,
CASE WHEN ww.problem_reason_remarks IS NULL THEN ldr.late_delivery_reason
ELSE ww.problem_reason_remarks END AS problem_reason_remarks,
-- return_start_date,
ww.return_pod_date,
ww.penerima_return,
CASE
  WHEN ww.pod_status = 'Delivered' THEN 'POD'
  ELSE ww.pod_status  
  END AS status_sistem,
ww.order_source,
ww.city_category,
-- dest_city,
-- dest_prov,

FROM zalora_data ww
LEFT JOIN `datamart_idexp.mitra_late_reason_delivery` ldr ON ww.Waybill_No = ldr.waybill_no

)

SELECT 

waybill_no,
package_number,
no_order,
Waktu_Input,
shipping_time,
order_source,
seller_name,
-- Start_Pickup_Time,
Pickup_Time,
-- End_pickup_time,
Order_Status,
Pickup_Failure_Reason,
Pickup_Failure_Attempt,
Origin_Branch,
Origin_District,
Origin_City,
Origin_Province,
Origin_City_District,
Dest_City_District,
-- Kanwil_Area_pickup,
-- order_past_1500,
-- Durasi_Pickup,
Pickup_Category,
Pickup_Performance,
-- Pickup_Rate,
Pickup_Status,
-- FM_Performance,
Late_Pickup_Factor,
Late_Pickup_Category,

-------OTD-----------------
min_SLA_Delivery,
SLA_Delivery,
SLA_Delivery_old,
due_date_delivery,
Deliv_Performance,
Due_Date_Delivery_old,
Deliv_Performance_old,


pod_status,
pod_date,
-- receiver,
POS_attempt,
-- CASE WHEN problem_reason_remarks IS NULL THEN "Late Schedule to Courier"
CASE WHEN Deliv_Performance = 'Late' AND problem_reason_remarks IS NULL THEN NULL
ELSE problem_reason_remarks END AS problem_reason_remarks,

-- return_start_date,
-- return_pod_date,
-- penerima_return,
status_sistem,
city_category,
dest_city,
dest_province,
dest_district,

CASE
WHEN Deliv_Performance = 'Late' AND problem_reason_remarks IN ('Paket akan diproses dengan nomor resi yang baru','Paket salah dalam proses penyortiran','Paket rusak/pecah','Paket hilang atau tidak ditemukan','Data alamat tidak sesuai dengan kode sortir','Paket hilang ditemukan','Pengemasan paket dengan kemasan rusak','Paket crosslabel','Melewati jam operasional cabang','Kerusakan pada resi / informasi resi tidak jelas','Kemasan paket rusak','Paket akan dikembalikan ke cabang asal','Kerusakan pada label pengiriman','Paket salah sortir/ salah rute','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain','Paket yang diterima dalam keadaan rusak','Late Arrival','Kurir tidak available','Late scan POD') THEN 'IDE'
WHEN Deliv_Performance = 'Late' AND problem_reason_remarks IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat',
'Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') THEN 'Penerima'
WHEN Deliv_Performance = 'Late' AND problem_reason_remarks IN ('Paket ditolak oleh bea cukai (red line)','Terdapat barang berbahaya (Dangerous Goods)','Cuaca buruk / bencana alam','Penundaan jadwal armada pengiriman','Indikasi kecurangan pengiriman','Berat paket tidak sesuai','Paket makanan, disimpan hingga waktu pengiriman yang tepat','Food parcels, kept until proper delivery time','Pengirim tidak dapat dihubungi','bencana alam','Cuaca buruk / Hujan') THEN 'External'
WHEN Deliv_Performance = 'Late' AND problem_reason_remarks IS NULL THEN 'IDE'
END AS Late_Deliv_Factor,

CASE
WHEN Deliv_Performance = 'Late' AND problem_reason_remarks IN ('Paket akan diproses dengan nomor resi yang baru','Paket salah dalam proses penyortiran','Paket rusak/pecah','Paket hilang atau tidak ditemukan','Data alamat tidak sesuai dengan kode sortir','Paket hilang ditemukan','Pengemasan paket dengan kemasan rusak','Paket crosslabel','Melewati jam operasional cabang','Kerusakan pada resi / informasi resi tidak jelas','Kemasan paket rusak','Paket akan dikembalikan ke cabang asal','Kerusakan pada label pengiriman','Paket salah sortir/ salah rute','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain','Paket yang diterima dalam keadaan rusak','Late Arrival','Late scan POD') THEN 'Controllable'
WHEN Deliv_Performance = 'Late' AND problem_reason_remarks IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat',
'Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') THEN 'Uncontrollable'
WHEN Deliv_Performance = 'Late' AND problem_reason_remarks IN ('Paket ditolak oleh bea cukai (red line)','Terdapat barang berbahaya (Dangerous Goods)','Cuaca buruk / bencana alam','Penundaan jadwal armada pengiriman','Indikasi kecurangan pengiriman','Berat paket tidak sesuai','Paket makanan, disimpan hingga waktu pengiriman yang tepat','Food parcels, kept until proper delivery time','Pengirim tidak dapat dihubungi','bencana alam','Cuaca buruk / Hujan') THEN 'Uncontrollable'
WHEN Deliv_Performance = 'Late' AND problem_reason_remarks IS NULL THEN 'Controllable'
END AS Late_Deliv_Category

FROM zalora_report
