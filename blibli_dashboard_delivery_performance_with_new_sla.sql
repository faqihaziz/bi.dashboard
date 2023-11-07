------------------------Dashboard Blibli Deliv---------------------------------------

WITH 

blibli_new_sla AS (
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

blibli_delivery_performance AS (

      SELECT 
   Waybill_No,
   Origin_Dest_City,
   Dest_City_District,
   Shipping_Time,
   Destination_Province,
   Destination_City,
   Destination,
   Delivery_Area,
   Kanwil_Area_Deliv,
   City_Category,
   POD_Date,
   POD_Time,
   POD_Branch,
   Order_Source,
   SLA_Delivery,
   Due_Date_Delivery,
   Performance,
   min_SLA_Delivery, --tambah kolom
   SLA_Delivery_old, --tambah kolom
   Due_Date_Delivery_Old, --tambah kolom
   Performance_Old, --tambah kolom


    CASE 
    WHEN problem_reason IN ('Paket dikirim via ekspedisi lain','Pengirim mengantarkan paket ke Drop Point','Pengirim tidak di tempat','Paket sedang disiapkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain') THEN NULL
    WHEN problem_reason IN ('Paket dikirim via ekspedisi lain','Pengirim mengantarkan paket ke Drop Point','Pengirim tidak di tempat','Paket sedang disiapkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain') THEN late_delivery_reason
    WHEN problem_reason IS NULL THEN late_delivery_reason  
    WHEN problem_reason IS NOT NULL THEN problem_reason
    END AS Late_Reason,

    FROM (

SELECT 
ww.waybill_no AS Waybill_No,
CONCAT(ww.sender_city_name,' ','-',' ',ww.recipient_city_name) AS Origin_Dest_City,
CONCAT(ww.recipient_city_name,' ','-',' ',ww.recipient_district_name) AS Dest_City_District,
DATETIME(ww.shipping_time,'Asia/Jakarta') AS Shipping_Time,
ww.recipient_province_name AS Destination_Province,
ww.recipient_city_name AS Destination_City,
ww.recipient_district_name AS Destination,
ma1.mitra_by_area AS Delivery_Area,
kw1.kanwil_name AS Kanwil_Area_Deliv,
DATE(ww.pod_record_time,'Asia/Jakarta') AS POD_Date,
DATETIME(ww.pod_record_time,'Asia/Jakarta') AS POD_Time,
ww.pod_branch_name AS POD_Branch,
sr.option_name as Order_Source,

-------------mapping_city_category------------------------------
CASE 
WHEN ww.recipient_province_name IN ('DKI JAKARTA') THEN "JAKARTA"
WHEN ww.recipient_province_name NOT IN ('DKI JAKARTA') AND ww.recipient_city_name IN ("BOGOR","KOTA BOGOR","KOTA DEPOK","KOTA TANGERANG",
"TANGERANG","TANGERANG SELATAN","BEKASI","KOTA BEKASI") THEN "BODETABEK"
WHEN ww.recipient_province_name NOT IN ('DKI JAKARTA') AND ww.recipient_city_name NOT IN ("BOGOR","KOTA BOGOR","KOTA DEPOK","KOTA TANGERANG",
"TANGERANG","TANGERANG SELATAN","BEKASI","KOTA BEKASI") THEN "NON-JABODETABEK" 
END AS City_Category,

-----------------blibli_delivery_performance------------------------
t6.SLAinternal AS SLA_Delivery_old,
bns.min_sla min_SLA_Delivery, 
bns.max_sla SLA_Delivery,

DATE(DATE_ADD(ww.shipping_time, INTERVAL (bns.max_sla) DAY)) AS Due_Date_Delivery,
DATE(DATE_ADD(ww.shipping_time, INTERVAL (t6.SLAinternal) DAY)) AS Due_Date_Delivery_Old,

------------------- performance by new sla -------------------------
CASE 
    WHEN bns.max_sla >= 999 THEN "No SLA (OoC)"
    WHEN bns.max_sla IS NULL THEN "Not Late"
    WHEN DATE(ww.pod_record_time,'Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (bns.max_sla*1) day)) THEN "Late"
    WHEN DATE(ww.pod_record_time,'Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (bns.max_sla*1) day)) THEN "Not Late"
    WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (bns.max_sla*1) day)) THEN NULL
    WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (bns.max_sla*1) day)) THEN "Late"
    END AS Performance,

-------------------- performance by old sla -----------------------------------------
CASE 
    WHEN t6.SLAinternal >= 999 THEN "No SLA (OoC)"
    WHEN t6.SLAinternal IS NULL THEN "Not Late"
    WHEN DATE(ww.pod_record_time,'Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (t6.SLAinternal*1) day)) THEN "Late"
    WHEN DATE(ww.pod_record_time,'Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (t6.SLAinternal*1) day)) THEN "Not Late"
    WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (t6.SLAinternal*1) day)) THEN NULL
    WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (t6.SLAinternal*1) day)) THEN "Late"
    END AS Performance_Old,

   -----------------------mapping_late_delivery_reason---------------------------------
MAX(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS problem_reason,
MAX(DATE(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS POS_time,
ldr.late_delivery_reason as late_delivery_reason,

CASE 
WHEN ps.problem_reason IN ('Paket dikirim via ekspedisi lain','Pengirim mengantarkan paket ke Drop Point','Pengirim tidak di tempat','Paket sedang disiapkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order') THEN NULL
WHEN ps.problem_reason IS NULL THEN pf2.problem_factor
ELSE pf1.problem_factor
END AS Late_Delivery_Factor,

CASE 
WHEN ps.problem_reason IN ('Paket dikirim via ekspedisi lain','Pengirim mengantarkan paket ke Drop Point','Pengirim tidak di tempat','Paket sedang disiapkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order') THEN NULL
WHEN ps.problem_reason IS NULL THEN pf2.problem_category
ELSE pf1.problem_category
END AS Late_Delivery_Category,



FROM `grand-sweep-324604.datawarehouse_idexp.waybill_waybill` ww
    LEFT JOIN `grand-sweep-324604.datawarehouse_idexp.waybill_problem_piece` ps ON ww.waybill_no = ps.waybill_no AND ps.deleted = '0'
    LEFT join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'orderSource'
    LEFT JOIN `datamart_idexp.masterdata_backlog_city_to_ma` ma1 ON ww.recipient_district_name = ma1.destination AND ww.recipient_city_name = ma1.destination_city 
    LEFT JOIN `datamart_idexp.mitra_late_reason_delivery` ldr ON ww.waybill_no = ldr.waybill_no
    INNER JOIN `grand-sweep-324604.datamart_idexp.sla_internal` t6 ON ww.recipient_city_name = t6.Destination_City and ww.sender_city_name = t6.Origin_City and ww.recipient_district_name = t6.Destination
    LEFT JOIN `datamart_idexp.mapping_kanwil_area` kw1 ON ww.recipient_province_name = kw1.province_name
    LEFT JOIN `datamart_idexp.masterdata_mapping_problem_factor` pf1 ON ps.problem_reason = pf1.code
    LEFT JOIN `datamart_idexp.masterdata_mapping_problem_factor` pf2 ON ldr.late_delivery_reason = pf2.register_reason_bahasa
    LEFT JOIN blibli_new_sla bns ON bns.shipping_client_id = ww.vip_customer_id 
              AND ww.sender_city_id = bns.sender_location_id 
              AND ww.recipient_district_id  = bns.recipient_location_id 
              AND ww.express_type = bns.express_type

WHERE DATE(ww.shipping_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -76 DAY))

AND sr.option_name IN ('Blibli','Blibli API')

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1

))

SELECT
Waybill_No,
Origin_Dest_City,
Dest_City_District,
Shipping_Time,
Destination_Province,
Destination_City,
Destination,
Delivery_Area,
Kanwil_Area_Deliv,
Order_Source,
POD_Date,
POD_Time,
POD_Branch,
City_Category,
CASE 
    WHEN DATE(Shipping_Time) >= '2023-11-01' THEN SLA_Delivery
    ELSE SLA_Delivery_Old END AS SLA_Delivery,
CASE
    WHEN DATE(Shipping_Time) >= '2023-11-01' THEN Due_Date_Delivery
    ELSE Due_Date_Delivery_Old END AS Due_Date_Delivery,
CASE
    WHEN DATE(Shipping_Time) >= '2023-11-01' THEN Performance
    ELSE Performance_Old END AS Performance,

CASE 
    WHEN Performance = 'Late' AND Late_Reason IS NULL THEN "Kurir tidak available"
    WHEN Late_Reason IS NOT NULL THEN Late_Reason
    END AS Late_Delivery_Reason,

CASE
WHEN Performance = 'Late' AND Late_Reason IN ('Paket akan diproses dengan nomor resi yang baru','Paket salah dalam proses penyortiran','Paket rusak/pecah','Paket hilang atau tidak ditemukan','Data alamat tidak sesuai dengan kode sortir','Paket hilang ditemukan','Pengemasan paket dengan kemasan rusak','Paket crosslabel','Melewati jam operasional cabang','Kerusakan pada resi / informasi resi tidak jelas','Kemasan paket rusak','Paket akan dikembalikan ke cabang asal','Kerusakan pada label pengiriman','Paket salah sortir/ salah rute','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain','Paket yang diterima dalam keadaan rusak','Late Delivery','Late Arrival','Kurir tidak available','Late scan POD') THEN 'IDE'
WHEN Performance = 'Late' AND Late_Reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat',
'Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') THEN 'Penerima'
WHEN Performance = 'Late' AND Late_Reason IN ('Paket ditolak oleh bea cukai (red line)','Terdapat barang berbahaya (Dangerous Goods)','Cuaca buruk / bencana alam','Penundaan jadwal armada pengiriman','Indikasi kecurangan pengiriman','Berat paket tidak sesuai','Paket makanan, disimpan hingga waktu pengiriman yang tepat','Food parcels, kept until proper delivery time','Pengirim tidak dapat dihubungi','bencana alam','Cuaca buruk / Hujan') THEN 'External'
WHEN Performance = 'Late' AND Late_Reason IS NULL THEN 'IDE'
END AS Late_Delivery_Factor,

CASE
WHEN Performance = 'Late' AND Late_Reason IN ('Paket akan diproses dengan nomor resi yang baru','Paket salah dalam proses penyortiran','Paket rusak/pecah','Paket hilang atau tidak ditemukan','Data alamat tidak sesuai dengan kode sortir','Paket hilang ditemukan','Pengemasan paket dengan kemasan rusak','Paket crosslabel','Melewati jam operasional cabang','Kerusakan pada resi / informasi resi tidak jelas','Kemasan paket rusak','Paket akan dikembalikan ke cabang asal','Kerusakan pada label pengiriman','Paket salah sortir/ salah rute','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain','Paket yang diterima dalam keadaan rusak','Late Delivery','Late Arrival','Kurir tidak available','Late scan POD') THEN 'Controllable'
WHEN Performance = 'Late' AND Late_Reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat',
'Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') THEN 'Uncontrollable'
WHEN Performance = 'Late' AND Late_Reason IN ('Paket ditolak oleh bea cukai (red line)','Terdapat barang berbahaya (Dangerous Goods)','Cuaca buruk / bencana alam','Penundaan jadwal armada pengiriman','Indikasi kecurangan pengiriman','Berat paket tidak sesuai','Paket makanan, disimpan hingga waktu pengiriman yang tepat','Food parcels, kept until proper delivery time','Pengirim tidak dapat dihubungi','bencana alam','Cuaca buruk / Hujan') THEN 'Uncontrollable'
WHEN Performance = 'Late' AND Late_Reason IS NULL THEN 'Controllable'
END AS Late_Delivery_Category,

CASE 
    WHEN Performance_Old = 'Late' AND Late_Reason IS NULL THEN "Kurir tidak available"
    WHEN Late_Reason IS NOT NULL THEN Late_Reason
    END AS Late_Delivery_Reason_old, --tambah kolom
min_SLA_Delivery, --tambah kolom
SLA_Delivery_Old, --tambah kolom
Due_Date_Delivery_Old, --tambah kolom
Performance_Old, --tambah kolom


FROM blibli_delivery_performance

QUALIFY ROW_NUMBER() OVER (PARTITION BY Waybill_No)=1

