SET mapreduce.framework.name=local;
SET mapreduce.cluster.local.dir=/tmp/hive-mapred-local;
SET hadoop.tmp.dir=/tmp/hadoop-hive;

SHOW TABLES;

DESCRIBE gold_daily_transaction_metrics;

SELECT * FROM gold_daily_transaction_metrics LIMIT 5;

SELECT COUNT(*) AS gold_row_count
FROM gold_daily_transaction_metrics;

SELECT currency,
       SUM(transaction_count) AS transaction_count
FROM gold_daily_transaction_metrics
GROUP BY currency
ORDER BY transaction_count DESC;

SELECT status,
       COUNT(*) AS silver_row_count
FROM silver_transactions
GROUP BY status
ORDER BY status;
