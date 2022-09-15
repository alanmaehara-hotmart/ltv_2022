
SELECT
    purchase_transaction,
    MAX(CASE WHEN rn = 1 THEN user_creation_datetime END) AS user1_date,
    MAX(CASE WHEN rn = 2 THEN user_creation_datetime END) AS user2_date,
    MAX(CASE WHEN rn = 3 THEN user_creation_datetime END) AS user3_date,
    MAX(CASE WHEN rn = 4 THEN user_creation_datetime END) AS user4_date,
    MAX(CASE WHEN rn = 5 THEN user_creation_datetime END) AS user5_date,
    MAX(CASE WHEN rn = 6 THEN user_creation_datetime END) AS user6_date,
    MAX(CASE WHEN rn = 1 THEN user_buyer_id END) AS user1,
    MAX(CASE WHEN rn = 2 THEN user_buyer_id END) AS user2,
    MAX(CASE WHEN rn = 3 THEN user_buyer_id END) AS user3,
    MAX(CASE WHEN rn = 4 THEN user_buyer_id END) AS user4,
    MAX(CASE WHEN rn = 5 THEN user_buyer_id END) AS user5,
    MAX(CASE WHEN rn = 6 THEN user_buyer_id END) AS user6
FROM (
SELECT purchase_transaction, user_buyer_id, user_creation_datetime, ROW_NUMBER() OVER (PARTITION BY purchase_transaction ORDER BY user_creation_datetime ASC) rn 
FROM (SELECT DISTINCT purchase_transaction, user_buyer_id, user_creation_datetime FROM dhm_core_business.f_purchase_hist hist
        JOIN dhm_core_business.d_user u ON hist.hub_index_m10_user_buyer_id = u.hub_index_m10_user_id AND hist.user_buyer_id = u.user_id
        	        WHERE (purchase_release_datetime BETWEEN '2021-07-01' AND '2022-06-30') AND purchase_order_datetime >= '2021-07-01')
)
GROUP BY 1 