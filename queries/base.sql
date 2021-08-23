WITH transactions AS (
    SELECT 
        trans_num AS id,
        cc_num AS user_id,
        amt AS price,
        merchant,
        trans_date_trans_time AS made_at,
        SUBSTR(zip, 1, 3) AS location,
        category,
        is_fraud
    FROM df
),

past AS (
    SELECT
        transactions.user_id,
        COUNT(DISTINCT past_transactions.id) AS num_transactions,
        COUNT(DISTINCT past_transactions.id)=0 AS no_past_transactions,
        AVG(past_transactions.price) AS mean_price
    FROM transactions 
    JOIN transactions past_transactions
        ON transactions.user_id = past_transactions.user_id
        AND past_transactions.made_at < DATE(transactions.made_at, '-90 DAYS') 
    GROUP BY 1
),

fraudulent_past AS (
    SELECT
        transactions.user_id,
        COUNT(DISTINCT fraudulent_transactions.id) AS num_transactions,
        COUNT(DISTINCT fraudulent_transactions.id)=0 AS no_past_transactions,
        AVG(fraudulent_transactions.price) AS mean_price
    FROM transactions 
    JOIN transactions fraudulent_transactions
        ON fraudulent_transactions.user_id = transactions.user_id
        AND fraudulent_transactions.made_at < DATE(transactions.made_at, '-90 DAYS') 
        AND fraudulent_transactions.is_fraud = 1
        -- Won't know if transaction is fraud till 90 days later
    GROUP BY 1
),

past_merchant_stats AS (
    SELECT
        transactions.merchant,
        COUNT(DISTINCT past_transactions.id) AS num_transactions,
        COUNT(DISTINCT past_transactions.id)=0 AS no_past_transactions,
        AVG(past_transactions.price) AS mean_price
    FROM transactions 
    JOIN transactions past_transactions
        ON transactions.merchant = past_transactions.merchant
        AND past_transactions.made_at < DATE(transactions.made_at, '-90 DAYS')
    GROUP BY 1
),

fraudulent_merchant_past AS (
    SELECT
        transactions.merchant,
        COUNT(DISTINCT fraudulent_transactions.id) AS num_transactions,
        COUNT(DISTINCT fraudulent_transactions.id)=0 AS no_past_transactions,
        AVG(fraudulent_transactions.price) AS mean_price
    FROM transactions 
    JOIN transactions fraudulent_transactions
        ON fraudulent_transactions.merchant = transactions.merchant
        AND fraudulent_transactions.made_at < DATE(transactions.made_at, '-90 DAYS') 
        AND fraudulent_transactions.is_fraud = 1
        -- Won't know if transaction is fraud till 90 days later
    GROUP BY 1
)

SELECT
    transactions.is_fraud AS label,
    transactions.id AS transaction_id,
    transactions.user_id,
    transactions.price,
    CAST(COALESCE(transactions.location, 'UNK') AS VARCHAR) AS location,
    CAST(COALESCE(transactions.merchant, 'UNK') AS VARCHAR) AS merchant,
    CAST(COALESCE(transactions.category, 'UNK') AS VARCHAR) AS category,
    past.num_transactions,
    COALESCE(past.no_past_transactions, True) AS no_past_transactions,
    past.mean_price,
    fraudulent_past.num_transactions AS num_fraudulent_transactions,
    COALESCE(fraudulent_past.no_past_transactions, True) AS no_fraudulent_past_transactions,
    fraudulent_past.mean_price AS mean_price_fraudulent,
    past_merchant_stats.num_transactions AS num_merchant_transactions,
    COALESCE(past_merchant_stats.no_past_transactions, True) AS no_merchant_past_transactions,
    past_merchant_stats.mean_price AS mean_price_merchant,
    fraudulent_merchant_past.num_transactions AS num_fraudulent_merchant_transactions,
    COALESCE(fraudulent_merchant_past.no_past_transactions, True) AS no_merchant_fraudulent_past_transactions,
    fraudulent_merchant_past.mean_price AS mean_price_merchant_fraudulent,
    CASE WHEN past.num_transactions > 0 
         THEN COALESCE(fraudulent_past.num_transactions, 0) / past.num_transactions END AS fraud_rate_user,
    CASE WHEN past_merchant_stats.num_transactions > 0 
         THEN COALESCE(fraudulent_merchant_past.num_transactions, 0) / past_merchant_stats.num_transactions END AS fraud_rate_merchant
FROM transactions
LEFT JOIN past ON past.user_id=transactions.user_id
LEFT JOIN fraudulent_past ON fraudulent_past.user_id=transactions.user_id
LEFT JOIN past_merchant_stats ON past_merchant_stats.merchant=transactions.merchant
LEFT JOIN fraudulent_merchant_past ON fraudulent_merchant_past.merchant=transactions.merchant
