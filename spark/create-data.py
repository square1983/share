from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    expr,
    current_timestamp
)


# =========================
# 修正が必要なパラメータ
# =========================

REGION = "ap-northeast-1"

TABLE_BUCKET_ARN = "arn:aws:s3tables:ap-northeast-1:123456789012:bucket/my-table-bucket"

CATALOG_NAME = "s3tablesbucket"
NAMESPACE = "sales_ns"
TABLE_NAME = "sales_records"

FULL_TABLE_NAME = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}"


# =========================
# データ量の制御
# =========================
# 目標：約 2GB の論理データ量
#
# 1 行あたりの概算：
# sale_id        long      8 bytes
# product_name   string    100〜150 bytes
# quantity       int       4 bytes
# amount         double    8 bytes
# sale_time      timestamp 8 bytes
#
# Parquet / Iceberg のオーバーヘッドも考慮すると、
# 2,000万行程度が 2GB 前後のテストデータとして扱いやすい。
# ただし、S3 上の実際の物理サイズは Parquet の圧縮により 2GB 未満になる可能性がある。
ROW_COUNT = 20_000_000

# パーティション数が多いほど出力ファイル数が増える。
# 少なすぎると 1 ファイルあたりのサイズが大きくなりすぎる可能性がある。
# Glue / EMR の Worker 数に応じて調整する。
PARTITIONS = 64


def main():
    spark = (
        SparkSession.builder
        .appName("WriteTestDataToS3Tables")

        # Iceberg の Spark 拡張機能を有効化
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        # S3 Tables Catalog の設定
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl",
            "software.amazon.s3tables.iceberg.S3TablesCatalog"
        )
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.warehouse",
            TABLE_BUCKET_ARN
        )

        # 書き込み時のファイルサイズを制御
        .config("spark.sql.shuffle.partitions", str(PARTITIONS))
        .config("spark.sql.files.maxRecordsPerFile", "500000")

        # Parquet の圧縮方式。
        # S3 上の物理サイズを 2GB に近づけたい場合は uncompressed に変更する。
        .config("spark.sql.parquet.compression.codec", "snappy")

        .getOrCreate()
    )

    print(f"Target table: {FULL_TABLE_NAME}")
    print(f"Generating rows: {ROW_COUNT}")

    # テストデータを生成
    #
    # Parquet の圧縮率が高くなりすぎないように、
    # product_name には uuid を組み合わせたランダム性の高い文字列を使用する。
    # これにより、単純な繰り返し文字列よりも実データに近いサイズ感になりやすい。
    df = (
        spark.range(0, ROW_COUNT)
        .repartition(PARTITIONS)
        .select(
            col("id").cast("long").alias("sale_id"),

            concat(
                lit("product_"),
                expr("uuid()"),
                lit("_"),
                expr("uuid()"),
                lit("_"),
                expr("uuid()")
            ).alias("product_name"),

            ((col("id") % 100) + 1).cast("int").alias("quantity"),

            (
                ((col("id") % 10000) + 100) / 10.0
            ).cast("double").alias("amount"),

            (
                current_timestamp() - expr("INTERVAL 1 DAYS") * (col("id") % 365)
            ).cast("timestamp").alias("sale_time")
        )
    )

    # S3 Tables / Iceberg テーブルへ書き込む
    #
    # append は既存テーブルへデータを追加するモード。
    df.writeTo(FULL_TABLE_NAME).append()

    print("Write completed.")

    # 書き込み結果を簡易確認
    count_df = spark.sql(f"SELECT COUNT(*) AS cnt FROM {FULL_TABLE_NAME}")
    count_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
