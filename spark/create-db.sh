aws s3tables create-table \
  --region ap-northeast-1 \
  --table-bucket-arn arn:aws:s3tables:ap-northeast-1:123456789012:bucket/my-table-bucket \
  --namespace sales_ns \
  --name sales_records \
  --format ICEBERG \
  --metadata '{
    "iceberg": {
      "schema": {
        "fields": [
          {"name": "sale_id", "type": "long", "required": true},
          {"name": "product_name", "type": "string"},
          {"name": "quantity", "type": "int"},
          {"name": "amount", "type": "double"},
          {"name": "sale_time", "type": "timestamp"}
        ]
      }
    }
  }'
