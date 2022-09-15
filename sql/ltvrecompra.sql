with base as (
    SELECT 
        pur.hub_index_m10_user_buyer_id, 
        pur.hub_index_m10_product_item_id,
        pur.hub_index_m10_purchase_id,
        pur.hub_index_m10_affiliation_id,
        pur.user_buyer_id,
        pur.product_item_id,
        pur.purchase_id,
        pur.affiliation_id,
        pur.recurrency_id,
        pur.purchase_transaction,
        pur.purchase_release_datetime,
        pur.purchase_release_date,
        pur.purchase_order_datetime,
        pur.purchase_order_date,
        pur.purchase_status,
        pur.purchase_payment_type,
        pur.purchase_installment_number,
        pur.purchase_total_value, 
        pur.purchase_value,
        pur.purchase_currency_code_to

    FROM dhm_core_business.f_purchase pur   
 	WHERE (pur.purchase_release_datetime BETWEEN '2021-07-01' AND '2022-06-30')
 	AND pur.purchase_order_datetime >= '2021-07-01'

), pure_buyers AS (
    -- get users with only paper = 1 
    SELECT DISTINCT 
        ur.user_id,
        ur.hub_index_m10_user_id,
        TRUE as pure_buyer
    FROM dhm_core_business.d_user_role ur 
    WHERE user_id IN (SELECT user_id FROM (SELECT user_id, COUNT(*) as counts FROM dhm_core_business.d_user_role 
    										GROUP BY 1 HAVING COUNT(*) = 1))
    and ur.user_role = 1 
    
), commission_base AS (
    -- LFT commission made per user type
    SELECT 
        cla.user_id AS user_id,
        SUM(cla.commission_coproducer_value_brl + cla.commission_producer_value_brl + cla.commission_affiliate_value_brl) AS commission_total,
        TRUE as has_commission
    FROM core_business.dhmv_commission_lifetime_accumulated cla 
    GROUP BY user_id
    HAVING COALESCE(commission_total) > 0

), user_papers as (
    -- check papers of each user
    SELECT
        base.*,
        COALESCE(p.pure_buyer, FALSE) as pure_buyer,
        COALESCE(cb.has_commission, FALSE) as has_commission,
        COALESCE(us.user_role_creation_datetime, NULL) as prod_signup_datetime,
        b.has_1,
        b.has_144,
        b.has_2,
        b.has_3,
        b.has_177,
        b.has_186
    FROM base 
    LEFT JOIN (SELECT
            user_id,
            COUNT(CASE WHEN user_role = 1 THEN 1 ELSE NULL END) as has_1,
            COUNT(CASE WHEN user_role = 144 THEN 1 ELSE NULL END) as has_144,
            COUNT(CASE WHEN user_role = 2 THEN 1 ELSE NULL END) as has_2,
            COUNT(CASE WHEN user_role = 3 THEN 1 ELSE NULL END) as has_3,
            COUNT(CASE WHEN user_role = 177 THEN 1 ELSE NULL END) as has_177,
            COUNT(CASE WHEN user_role = 186 THEN 1 ELSE NULL END) as has_186
        FROM dhm_core_business.d_user_role u
        GROUP BY 1) b on base.user_buyer_id = b.user_id
    LEFT JOIN pure_buyers p on base.hub_index_m10_user_buyer_id  = p.hub_index_m10_user_id AND base.user_buyer_id = p.user_id
    LEFT JOIN commission_base cb on base.user_buyer_id = cb.user_id
    LEFT JOIN (SELECT  
                user_id, user_role_creation_datetime 
                FROM dhm_core_business.d_user_role where user_role = 186) us ON us.user_id = base.user_buyer_id
), subcategory_unique AS (
    -- get unique subcategory/category
	SELECT
		product_subcategory_name,
		product_topification_category_name
	FROM (SELECT 	
			product_subcategory_name,
			product_topification_category_name,	
			cts,
			ROW_NUMBER() OVER (PARTITION BY product_subcategory_name ORDER BY cts DESC) AS rn 
			FROM (SELECT 
					product_subcategory_name,
					product_topification_category_name,
					COUNT(product_id) AS cts
			      FROM dhm_core_business.d_product 
				  WHERE product_topification_name IS NOT NULL AND product_subcategory_name IS NOT NULL AND product_topification_category_name IS NOT NULL 
				  GROUP BY 1,2))
	WHERE rn = 1
), category_unique AS ( 
    -- get unique category/topic
	SELECT DISTINCT 
		product_topification_category_name,
		product_topification_name
	FROM dhm_core_business.d_product 
	WHERE product_topification_category_name IS NOT NULL AND product_topification_name IS NOT NULL
), impute_category AS (
   -- this subquery imputes null categories at producer level. It uses the most frequent topic
	SELECT 
	  user_producer_id,
	  product_topification_category_name
    FROM (SELECT 
			user_producer_id,
			product_topification_category_name,
			ROW_NUMBER() OVER (PARTITION BY user_producer_id ORDER BY cts DESC) AS rn 
          FROM (SELECT 
				  user_producer_id,
				  product_topification_category_name,
				  COUNT(*) AS cts
			   FROM dhm_core_business.d_product dp 
			   WHERE product_topification_category_name IS NOT NULL 
			   GROUP BY 1,2)) 
	WHERE rn = 1
), products AS ( 
   SELECT 
        dp.hub_index_m10_product_id,
        dp.hub_index_m10_user_producer_id,
        dp.product_id,
        dp.user_producer_id,
        CASE 
            WHEN dp.category_id = 1 THEN 'E-books, Documents'
            WHEN dp.category_id = 2 THEN 'Software'
            WHEN dp.category_id = 3 THEN 'Apps'
            WHEN dp.category_id = 4 THEN 'Video Lessons, Screencasts, Movies'
            WHEN dp.category_id = 5 THEN 'Audios, Musics, Ringtones'
            WHEN dp.category_id = 6 THEN 'Templates, Source Code'
            WHEN dp.category_id = 7 THEN 'Images'
            WHEN dp.category_id = 8 THEN 'Online Courses, Members Site, Signature Services'
            WHEN dp.category_id = 9 THEN 'Serial Codes, Discount Coupons'
            WHEN dp.category_id = 10 THEN 'E-tickets'
            WHEN dp.category_id = 11 THEN 'Online Services'
            WHEN dp.category_id = 12 THEN 'Online Events'
            WHEN dp.category_id = 13 THEN 'Bundle'
        END AS product_category,
        CASE WHEN dp.category_id = 8 and dp.product_membership_user_activation_form = 4 and dp.product_distribution_form = 2
        	THEN 'club' ELSE 'not_club' END AS is_club,
        CASE WHEN r.total_answers IS NULL THEN 'no_rating' ELSE 'has_rating' END AS has_rating,
        r.total_answers as total_answers_rating,
        r.average as avg_rating,
		CASE 
			WHEN dp.product_id IN (1110516,868003,260816, 1849589, 1494525, 478392, 1777580, 619157) THEN 'FINANÇAS E NEGÓCIOS'
			WHEN dp.product_id IN (1303073,700015,219755, 309107, 274264, 329451, 291557) THEN 'ENSINO E ESTUDO ACADÊMICO'
			WHEN dp.product_id = 1079400 THEN 'CARREIRA E DESENVOLVIMENTO PESSOAL'
			WHEN dp.product_id = 1760277 THEN 'MARKETING E VENDAS'
			WHEN dp.product_id = 974641 THEN 'DESIGN E FOTOGRAFIA'
			WHEN dp.product_id = 1616310 THEN 'AUTOCONHECIMENTO E ESPIRITUALIDADE'
			WHEN dp.product_id = 1217611 THEN 'PLANTAS E ECOLOGIA'
			WHEN dp.product_id IN (220019,615006,260516, 431794, 1348819, 403411, 291169, 141566, 836966, 1363004) THEN 'SAÚDE E ESPORTES'
			WHEN dp.product_id IN (483079,1844916) THEN 'MANUTENÇÃO DE EQUIPAMENTOS'
			WHEN dp.product_id IN (1689407,1223886) THEN 'RELACIONAMENTOS'
			ELSE COALESCE(dp.product_topification_category_name, cu.product_topification_category_name, ic.product_topification_category_name, su.product_topification_category_name) END AS category       
	FROM dhm_core_business.d_product dp
    LEFT JOIN category_unique cu ON cu.product_topification_name = dp.product_topification_name
    LEFT JOIN impute_category ic on ic.user_producer_id = dp.user_producer_id
    LEFT JOIN subcategory_unique su ON su.product_subcategory_name = dp.product_subcategory_name 
    LEFT JOIN dhm_api_ask.d_survey_rating r ON r.product_id = dp.product_id

)
SELECT 
    up.purchase_id,
    up.user_buyer_id,
    CASE 
        WHEN pure_buyer THEN 'pure_buyer'
        WHEN pure_buyer IS FALSE AND has_commission THEN 'buyer_with_sales'
        WHEN pure_buyer IS FALSE AND has_commission IS FALSE AND has_186 = 1 THEN 'buyer_signup_no_sales'
        WHEN pure_buyer IS FALSE AND has_commission IS FALSE AND has_186 = 0 THEN 'pure_buyer'
        ELSE NULL END AS user_type,
-- COALESCE(d.name, 'Others') AS device_origin,
    u.user_country,
    u2.user_office_name,
    u.user_creation_datetime,
    u.user_creation_date,
    up.prod_signup_datetime,
    up.purchase_release_datetime,
    up.purchase_release_date,
    up.purchase_order_datetime,
    up.purchase_order_date,
    up.purchase_status,
    up.purchase_payment_type,
    up.purchase_installment_number,
    up.purchase_transaction,
    p.product_id,
    p.user_producer_id AS producer_id,
    p.product_category,
    p.is_club,
    p.has_rating,
    p.total_answers_rating,
    p.avg_rating,
    p.category,
    COALESCE(up.purchase_total_value, up.purchase_value + ISNULL(otc.order_transaction_cost_vat_calculated_cost_value,0) +ISNULL(otc.order_transaction_cost_installment_interest_total_value,0) ) *(CASE WHEN  up.purchase_currency_code_to = 'BRL' THEN 1 ELSE ex1.exchange_rate_value  END) AS gmv_value_brl,       
    subs.id AS subscription_id,
    CASE WHEN subs.plano_assinatura IS NOT NULL THEN 'subscription' ELSE 'single-payment' END AS is_subs,
	subs.assinatura_status as subs_status,
    subs.valor_de_recorrencia_no_momento_da_adesao as subs_value,
	subs.periodicidade_da_recorrencia_no_momento_da_adesao AS subs_type,
	subs.data_vencimento as subs_due_day,
	subs.data_adesao as subs_start_datetime,
	subs.data_cancelamento as subs_cancellation_datetime,
	subs.ultimo_pagamento_realizado as subs_last_payment_datetime,
	subs.ultima_tentativa_cobranca as subs_last_payment_tentative_date,
	sf.subscription_feature_type,
    fr.id AS recurrency_id,
	fr.num_recorrencia AS recurrency_number,
	CASE WHEN c.coupon_id IS NOT NULL THEN 'has_coupon' ELSE 'no_coupon' END AS has_coupon,
	c.coupon_discount_value,
	s.segmentation_final_name AS segment,
	s_final.segmentation_final_name AS segmentation_final_name

--	agg.user1,
--	agg.user2,
--	agg.user3,
--	agg.user4,
--	agg.user1_date,
--	agg.user2_date,
--	agg.user4_date,
--	agg.user3_date

FROM user_papers up 
JOIN dhm_core_business.f_product_item pi ON up.hub_index_m10_product_item_id = pi.hub_index_m10_product_item_id AND up.product_item_id = pi.product_item_id
JOIN dhm_core_business.d_user u ON up.hub_index_m10_user_buyer_id = u.hub_index_m10_user_id AND up.user_buyer_id = u.user_id                       
--JOIN dhm_core_business.d_affiliation aff ON aff.hub_index_m10_affiliation_id = up.hub_index_m10_affiliation_id  AND up.affiliation_id = aff.affiliation_id 
JOIN products p ON pi.hub_index_m10_product_id = p.hub_index_m10_product_id  AND pi.product_id = p.product_id 
JOIN dhm_core_business.d_purchase_extra_info dpei ON dpei.hub_index_m10_purchase_id = up.hub_index_m10_purchase_id AND dpei.purchase_id = up.purchase_id
-- JOIN analytics.f_customer_event fce on fce.id = pur.purchase_id
-- LEFT JOIN d_device d ON fce.d_device_id = d.id
JOIN dhm_core_business.d_user u2 ON p.hub_index_m10_user_producer_id = u2.hub_index_m10_user_id AND p.user_producer_id = u2.user_id                       
LEFT JOIN dhm_core_business.d_coupon c ON c.hub_index_m10_coupon_id = dpei.hub_index_m10_coupon_id AND c.coupon_id = dpei.coupon_id
LEFT JOIN dhm_core_business.d_order_transaction_cost otc ON otc.hub_index_m10_purchase_id = up.hub_index_m10_purchase_id AND otc.purchase_id = up.purchase_id
LEFT JOIN dhm_core_business.d_exchange_rate ex1 ON ex1.exchange_rate_date = up.purchase_release_date 
    AND ex1.exchange_rate_currency_code_to IN ('BRL') AND up.purchase_currency_code_to = ex1.exchange_rate_currency_code_from    
LEFT JOIN datamart.d_subscription subs ON MOD(subs.id, 10) = pi.hub_index_m10_subscription_id AND subs.id = pi.subscription_id
LEFT JOIN datamart.d_subscription_feature sf on sf.subscription = subs.id
LEFT JOIN datamart.f_recurrency fr on fr.id = up.recurrency_id -- se quiser pegar o número da recorrência
LEFT JOIN (SELECT reference_month, hub_index_m10_user_id, user_id, segmentation_final_name FROM core_business.dhv_user_segmentation_hist WHERE reference_month >= '2021-07') s ON s.hub_index_m10_user_id = p.hub_index_m10_user_producer_id AND s.user_id = p.user_producer_id AND s.reference_month = TO_CHAR(up.purchase_order_date,'YYYY-MM')
LEFT JOIN (SELECT hub_index_m10_user_id, user_id, segmentation_final_name FROM core_business.dhv_user_segmentation_hist WHERE is_segmentation_active = 1) s_final ON s_final.hub_index_m10_user_id = p.hub_index_m10_user_producer_id AND s_final.user_id = p.user_producer_id 
WHERE u.user_creation_datetime >= '2021-07-01'

--LEFT JOIN 
--	(
--SELECT
--    purchase_transaction,
--    MAX(CASE WHEN rn = 1 THEN user_creation_datetime END) AS user1_date,
--    MAX(CASE WHEN rn = 2 THEN user_creation_datetime END) AS user2_date,
--    MAX(CASE WHEN rn = 3 THEN user_creation_datetime END) AS user3_date,
--    MAX(CASE WHEN rn = 4 THEN user_creation_datetime END) AS user4_date,
--    MAX(CASE WHEN rn = 5 THEN user_creation_datetime END) AS user5_date,
--    MAX(CASE WHEN rn = 6 THEN user_creation_datetime END) AS user6_date,
--    MAX(CASE WHEN rn = 1 THEN user_buyer_id END) AS user1,
--    MAX(CASE WHEN rn = 2 THEN user_buyer_id END) AS user2,
--    MAX(CASE WHEN rn = 3 THEN user_buyer_id END) AS user3,
--    MAX(CASE WHEN rn = 4 THEN user_buyer_id END) AS user4,
--    MAX(CASE WHEN rn = 5 THEN user_buyer_id END) AS user5,
--    MAX(CASE WHEN rn = 6 THEN user_buyer_id END) AS user6
--FROM (
--SELECT purchase_transaction, user_buyer_id, user_creation_datetime, ROW_NUMBER() OVER (PARTITION BY purchase_transaction ORDER BY user_creation_datetime ASC) rn 
--FROM (SELECT DISTINCT purchase_transaction, user_buyer_id, user_creation_datetime FROM dhm_core_business.f_purchase_hist hist
--        JOIN dhm_core_business.d_user u ON hist.hub_index_m10_user_buyer_id = u.hub_index_m10_user_id AND hist.user_buyer_id = u.user_id
--        	        WHERE (purchase_release_datetime BETWEEN '2021-07-01' AND '2022-06-30') AND purchase_order_datetime >= '2021-07-01')
--)
--GROUP BY 1 ) agg ON agg.purchase_transaction = up.purchase_transaction


--AND u.user_creation_date > pur.purchase_order_date









--
--SELECT DISTINCT 
--	fce.origin_datetime,
--    fce.purchase_code as purchase_transaction,
--    d.name as device
--FROM analytics.f_customer_event fce on 
--JOIN (SELECT purchase_transaction FROM  dhm_core_business.f_purchase pur 
--        WHERE (pur.purchase_release_datetime BETWEEN '2021-07-01' AND '2022-06-30')
--     AND pur.purchase_order_datetime >= '2021-07-01') pur on fce.purchase_code = pur.purchase_transaction
--LEFT JOIN analytics.d_device d ON fce.d_device_id = d.id 
--WHERE fce.d_event_type_id = 4
--and origin_datetime BETWEEN '2021-06-01' and '2022-07-05'

--
--SELECT DISTINCT 
--    fce.purchase_code as purchase_transaction,
--    d.name as device
--FROM analytics.f_customer_event fce 
--JOIN (SELECT purchase_transaction FROM  dhm_core_business.f_purchase pur 
--        WHERE (pur.purchase_release_datetime BETWEEN '2021-07-01' AND '2022-06-30')
-- 	AND pur.purchase_order_datetime >= '2021-07-01') pur on fce.purchase_code = pur.purchase_transaction
--LEFT JOIN analytics.d_device d ON fce.d_device_id = d.id 
--WHERE fce.d_event_type_id = 4
--and origin_datetime BETWEEN '2022-02-01' and '2022-03-31' 
---- and origin_datetime BETWEEN '2021-12-01' and '2022-01-31'  
---- and origin_datetime BETWEEN '2021-10-01' and '2021-11-30'  
---- and origin_datetime BETWEEN '2021-08-01' and '2022-09-30'  
---- and origin_datetime BETWEEN '2021-07-01' and '2022-07-31'   

---- CLUB
--SELECT DISTINCT
--  u.marketplace_id as user_buyer_id, 
--  a.marketplace_id as product_id,
--  IF(ua.is_course_complete = TRUE, True, False) AS is_course_complete,
--  DATE_FORMAT(ua.completion_course_date, '%Y-%m-%d %T') AS completion_course_date,
--    DATE_FORMAT(ua.join_date, '%Y-%m-%d %T') AS join_course_date,
--  ua.status AS membership_status,
--  DATE_FORMAT(ua.last_access, '%Y-%m-%d %T') AS last_access
--FROM user_area ua
--  INNER JOIN user u ON u.id = ua.user
--  INNER JOIN membership a ON a.id = ua.area
--WHERE u.marketplace_id  = 47757265
--    -- AND a.marketplace_id  = :product_id
