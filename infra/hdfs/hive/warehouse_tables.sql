CREATE DATABASE IF NOT EXISTS realtime_warehouse;

CREATE EXTERNAL TABLE IF NOT EXISTS realtime_warehouse.bronze_transactions (
  transaction_id STRING,
  customer_id STRING,
  payment_method_id STRING,
  country_id STRING,
  transaction_type STRING,
  amount DECIMAL(12, 2),
  currency STRING,
  status STRING,
  merchant_id STRING,
  device_type STRING,
  producer_timestamp_ms BIGINT,
  transaction_reference STRING,
  created_at TIMESTAMP
)
PARTITIONED BY (created_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/bronze/transactions';

CREATE EXTERNAL TABLE IF NOT EXISTS realtime_warehouse.silver_transactions (
  transaction_id STRING,
  customer_id STRING,
  payment_method_id STRING,
  country_id STRING,
  transaction_type STRING,
  amount DECIMAL(12, 2),
  currency STRING,
  status STRING,
  merchant_id STRING,
  device_type STRING,
  producer_timestamp_ms BIGINT,
  transaction_reference STRING,
  created_at TIMESTAMP
)
PARTITIONED BY (created_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/silver/transactions';

CREATE EXTERNAL TABLE IF NOT EXISTS realtime_warehouse.silver_quarantine (
  transaction_id STRING,
  customer_id STRING,
  payment_method_id STRING,
  country_id STRING,
  transaction_type STRING,
  amount DECIMAL(12, 2),
  currency STRING,
  status STRING,
  merchant_id STRING,
  device_type STRING,
  producer_timestamp_ms BIGINT,
  transaction_reference STRING,
  created_at TIMESTAMP,
  created_date STRING,
  `_rejection_reason` STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/silver/quarantine';

CREATE EXTERNAL TABLE IF NOT EXISTS realtime_warehouse.gold_daily_transaction_metrics (
  transaction_date DATE,
  currency STRING,
  status STRING,
  transaction_type STRING,
  transaction_count BIGINT,
  gross_amount DECIMAL(22, 2),
  average_amount DECIMAL(16, 6)
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/gold/daily_transaction_metrics';

CREATE EXTERNAL TABLE IF NOT EXISTS realtime_warehouse.gold_enriched_transactions (
  transaction_id STRING,
  transaction_reference STRING,
  transaction_type STRING,
  transaction_category STRING,
  amount DOUBLE,
  currency STRING,
  status STRING,
  country_id STRING,
  merchant_id STRING,
  device_type STRING,
  customer_id STRING,
  customer_name STRING,
  customer_tier STRING,
  customer_region STRING,
  payment_method_id STRING,
  payment_method_name STRING,
  payment_method_type STRING,
  payment_method_category STRING,
  payment_processing_fee_percent DOUBLE,
  processing_fee_amount DOUBLE,
  is_high_value BOOLEAN,
  is_premium_customer BOOLEAN,
  risk_score STRING,
  producer_timestamp_ms BIGINT,
  processing_timestamp_ms BIGINT,
  e2e_latency_ms BIGINT,
  processing_date TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/gold/enriched_transactions';
