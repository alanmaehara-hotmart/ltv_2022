SELECT
    pur.purchase_id, 
    pur.payment_method_description, 
    pur.purchase_payment_engine, 
    pur.purchase_sale_type, 
    pur.order_bump_type, 
    pur.purchase_parent_id 
FROM core_business.dhv_finance_audit pur
JOIN dhm_core_business.d_user u ON pur.hub_index_m10_user_buyer_id = u.hub_index_m10_user_id AND pur.user_buyer_id = u.user_id                       
WHERE u.user_creation_datetime >= '2021-07-01' AND
(pur.purchase_release_datetime BETWEEN '2021-07-01' AND '2022-06-30')
 AND pur.purchase_order_datetime >= '2021-07-01'