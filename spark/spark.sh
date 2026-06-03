uv run spark-submit \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8,software.amazon.awssdk:bundle:2.29.52,software.amazon.awssdk:url-connection-client:2.29.52 \
  write_sales_records_2gb.py
