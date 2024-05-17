WITH cx_mengantar_claim_checking AS (
                    SELECT 

                    ww.waybill_no,
                    ww.order_no,
                    ww.ecommerce_order_no,
                    DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time ,
                    DATETIME(ww.pod_record_time,'Asia/Jakarta') pod_record_time,
                    mb.branch_name destination_branch_name,
                    ww.delivery_branch_name,
                    ww.pod_branch_name,
                    ww.pod_photo_url,
                    t0.option_name AS waybill_source,
                    ww.parent_shipping_cleint vip_username,
                    ww.vip_customer_name sub_account,
                    rd16.option_name AS void_status,
                    ww.recipient_address,


                    FROM `datawarehouse_idexp.waybill_waybill` ww
                    LEFT OUTER JOIN `dev_idexp.masterdata_branch_coverage_th` mb ON ww.recipient_district_id = mb.district_id
                    LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = ww.void_flag AND rd16.type_option = 'voidFlag'
                    -- LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd1 ON rd1.option_value = ww.return_flag AND rd1.type_option = 'returnFlag'
                    LEFT OUTER JOIN `datawarehouse_idexp.system_option` t0 ON t0.option_value = ww.waybill_source AND t0.type_option = 'waybillSource'
                    -- LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = ww.waybill_status AND t1.type_option = 'waybillStatus'
                    -- LEFT OUTER JOIN `datawarehouse_idexp.system_option` et ON et.option_value = ww.express_type AND et.type_option = 'expressType'
                    -- LEFT OUTER JOIN `datawarehouse_idexp.system_option` st ON st.option_value = ww.service_type AND st.type_option = 'serviceType'


                    WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
                    --AND t0.option_name IN ('Mengantar')
                    AND ww.waybill_source IN ('753','796')
                    -- AND ww.waybill_no IN ('IDE702412752006')

                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1

                    ),

                    return_waybill AS (

                    SELECT

                    rr.waybill_no,
                    DATETIME(rr.return_record_time,'Asia/Jakarta') return_record_time,
                    DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') return_confirm_record_time,
                    DATETIME(rr.return_pod_record_time,'Asia/Jakarta') return_pod_record_time,


                    FROM `datawarehouse_idexp.waybill_return_bill` rr
                    ),

                    first_deliv_attempt AS (

                    SELECT
                    sc.waybill_no,
                    MIN(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_deliv_attempt,
                    MIN(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS scan_type,
                    MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) first_deliv_branch,

                    FROM `datawarehouse_idexp.waybill_waybill_line` sc 
                    LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

                    WHERE 
                    DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
                    AND operation_type = "09"

                    QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

                    ),

                    first_return_regist_time AS (

                    SELECT
                    sc.waybill_no,
                    MIN(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_regist_return_time,
                    MIN(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS scan_type,
                    MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) first_regist_return_branch,

                    FROM `datawarehouse_idexp.waybill_waybill_line` sc 
                    LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

                    WHERE 
                    DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
                    AND operation_type = "19"

                    QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

                    ),

                    get_bad_address_pos AS (

                    SELECT
                            ps.waybill_no,
                            ps.problem_reason problem_reason_bad_address,
                            DATETIME(ps.operation_time,'Asia/Jakarta') pos_attempt_time,
                            prt.option_name problem_type,

                                FROM `datawarehouse_idexp.waybill_problem_piece` ps
                                LEFT OUTER JOIN `datawarehouse_idexp.system_option` prt ON ps.problem_type  = prt.option_value AND prt.type_option = 'problemType'
                                WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

                                AND ps.problem_type NOT IN ('02')
                                AND ps.problem_reason IN ('Alamat pelanggan salah/sudah pindah alamat','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas')

                                QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no)=1
                    
                    ),

                    get_package_lost_pos AS (

                    SELECT
                            ps.waybill_no,
                            ps.problem_reason problem_reason_bad_address,
                            DATETIME(ps.operation_time,'Asia/Jakarta') pos_attempt_time,
                            prt.option_name problem_type,

                                FROM `datawarehouse_idexp.waybill_problem_piece` ps
                                LEFT OUTER JOIN `datawarehouse_idexp.system_option` prt ON ps.problem_type  = prt.option_value AND prt.type_option = 'problemType'
                                WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

                                AND ps.problem_type NOT IN ('02')
                                AND ps.problem_reason IN ('Paket hilang atau tidak ditemukan')

                                QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no)=1
                    
                    ),

                    get_lost_package_found_pos AS (

                    SELECT
                            ps.waybill_no,
                            ps.problem_reason problem_reason_bad_address,
                            DATETIME(ps.operation_time,'Asia/Jakarta') pos_attempt_time,
                            prt.option_name problem_type,

                                FROM `datawarehouse_idexp.waybill_problem_piece` ps
                                LEFT OUTER JOIN `datawarehouse_idexp.system_option` prt ON ps.problem_type  = prt.option_value AND prt.type_option = 'problemType'
                                WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

                                AND ps.problem_type NOT IN ('02')
                                AND ps.problem_reason IN ('Paket hilang ditemukan')

                                QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no)=1
                    
                    ),

                    get_pos_1_to_4 as(

                    SELECT
                    ps.waybill_no,
                    ps.pos_photo_url_1,
                    ps.pos_photo_url_2,
                    ps.pos_photo_url_3,
                    ps.pos_photo_url_4,
                    ps.pos_reason_1,
                    ps.pos_reason_2,
                    ps.pos_reason_3,
                    ps.pos_reason_4,

                    FROM (
                    SELECT
                            waybill_no,
                            MAX(IF(id = 1, DATETIME(record_time), NULL)) AS pos_attempt_1,
                            MAX(IF(id = 2, DATETIME(record_time), NULL)) AS pos_attempt_2,
                            MAX(IF(id = 3, DATETIME(record_time), NULL)) AS pos_attempt_3,
                            MAX(IF(id = 4, DATETIME(record_time), NULL)) AS pos_attempt_4,

                            MAX(IF(id = 1, pos_reason, NULL)) AS pos_reason_1,
                            MAX(IF(id = 2, pos_reason, NULL)) AS pos_reason_2,
                            MAX(IF(id = 3, pos_reason, NULL)) AS pos_reason_3,
                            MAX(IF(id = 4, pos_reason, NULL)) AS pos_reason_4,

                            MAX(IF(id = 1, photo_url, NULL)) AS pos_photo_url_1,
                            MAX(IF(id = 2, photo_url, NULL)) AS pos_photo_url_2,
                            MAX(IF(id = 3, photo_url, NULL)) AS pos_photo_url_3,
                            MAX(IF(id = 4, photo_url, NULL)) AS pos_photo_url_4,

                            FROM (
                                SELECT sc.waybill_no, 
                                DATETIME(sc.record_time,'Asia/Jakarta') record_time,
                                sc.register_reason_bahasa pos_reason,
                                sc.photo_url,
                                            
                                RANK() OVER (PARTITION BY sc.waybill_no ORDER BY DATETIME(sc.record_time, 'Asia/Jakarta') ASC ) AS id
                                FROM `datawarehouse_idexp.waybill_waybill_line` sc

                                WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
                                AND sc.operation_type IN ('18') AND sc.problem_type NOT IN ('02') 
                            ) 

                            GROUP BY 1 
                    ) ps
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no)=1
                    ),

                    waybill_to_deliv_pos_and_return AS (

                    SELECT

                    cx.waybill_no,
                    cx.order_no,
                    cx.shipping_time,
                    cx.waybill_source,
                    cx.void_status,
                    cx.recipient_address,
                    CASE
                        WHEN cx.pod_record_time IS NOT NULL THEN cx.pod_branch_name
                        WHEN cx.pod_record_time IS NULL AND fd.first_deliv_attempt IS NOT NULL THEN fd.first_deliv_branch
                        WHEN cx.pod_record_time IS NULL AND fd.first_deliv_attempt IS NULL THEN cx.destination_branch_name
                        END AS destination_branch_name,
                    
                    fd.first_deliv_attempt,
                    fd.first_deliv_branch,
                    fr.first_regist_return_time,
                    fr.first_regist_return_branch,
                    DATE_DIFF(DATE(fr.first_regist_return_time),DATE(fd.first_deliv_attempt),DAY) AS diff_first_regist_and_deliv_day,
                    
                    CASE
                        WHEN ps1.waybill_no IS NOT NULL THEN "Yes"
                        ELSE "No" END AS apakah_ada_pos_alamat_penerima_tidak_jelas_tidak_dapat_ditemukan,
                    CASE
                        WHEN ps2.waybill_no IS NOT NULL THEN "Yes"
                        ELSE "No" END AS apakah_ada_POS_paket_hilang,
                    CASE
                        WHEN ps3.waybill_no IS NOT NULL THEN "Yes"
                        ELSE "No" END AS apakah_ada_POS_paket_hilang_ditemukan,
                    CASE
                        WHEN pod_record_time IS NOT NULL THEN "Yes"
                        ELSE "No" END AS apakah_paket_sudah_pod,

                    ps4.pos_reason_1,
                    ps4.pos_photo_url_1,
                    ps4.pos_reason_2,
                    ps4.pos_photo_url_2,
                    ps4.pos_reason_3,
                    ps4.pos_photo_url_3,
                    ps4.pos_reason_4,
                    ps4.pos_photo_url_4,

                    cx.pod_record_time,
                    cx.pod_photo_url,

                    FROM cx_mengantar_claim_checking cx
                    LEFT OUTER JOIN first_deliv_attempt fd ON cx.waybill_no = fd.waybill_no
                    LEFT OUTER JOIN first_return_regist_time fr ON cx.waybill_no = fr.waybill_no
                    LEFT OUTER JOIN get_bad_address_pos ps1 ON cx.waybill_no = ps1.waybill_no
                    LEFT OUTER JOIN get_package_lost_pos ps2 ON cx.waybill_no = ps2.waybill_no
                    LEFT OUTER JOIN get_lost_package_found_pos ps3 ON cx.waybill_no = ps3.waybill_no
                    LEFT OUTER JOIN get_pos_1_to_4 ps4 ON cx.waybill_no = ps4.waybill_no
                    )

                    SELECT * FROM waybill_to_deliv_pos_and_return
