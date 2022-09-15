WITH base as (

SELECT DISTINCT 
    up.purchase_id,
    up.purchase_order_datetime, 
    p.product_id,
    ROW_NUMBER() OVER (PARTITION BY p.product_id ORDER BY up.purchase_order_datetime ASC) AS rn 
FROM dhm_core_business.f_purchase up 
JOIN dhm_core_business.f_product_item pi ON up.hub_index_m10_product_item_id = pi.hub_index_m10_product_item_id AND up.product_item_id = pi.product_item_id
JOIN dhm_core_business.d_product p ON pi.hub_index_m10_product_id = p.hub_index_m10_product_id  AND pi.product_id = p.product_id 

), final as (

SELECT DISTINCT
purchase_id,
product_id,
purchase_order_datetime
FROM base
WHERE rn = 1
)
SELECT * from final

