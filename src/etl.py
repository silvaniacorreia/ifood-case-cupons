# Funções de ETL: ingestão, limpeza, normalização (timezone/PII), joins e agregações
from __future__ import annotations
import os
from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, functions as F, types as T

# ---------- Leitura dos insumos ----------

def load_raw(spark, raw_dir: str) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Lê os 4 insumos do case a partir de data/raw/.
    Retorna: (orders, consumers, restaurants, abmap)
    """
    orders_path = os.path.join(raw_dir, "order.json.gz")
    consumers_path = os.path.join(raw_dir, "consumer.csv.gz")
    restaurants_path = os.path.join(raw_dir, "restaurant.csv.gz")
    ab_dir = os.path.join(raw_dir, "ab_test_ref_extracted")

    orders = (
        spark.read
        .option("multiLine", True) 
        .json(orders_path)
    )

    consumers = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(consumers_path)
    )

    restaurants = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(restaurants_path)
    )

    ab_csv = None
    for root, _, files in os.walk(ab_dir):
        for f in files:
            if f.lower().endswith(".csv"):
                ab_csv = os.path.join(root, f)
                break
        if ab_csv:
            break
    if not ab_csv:
        raise FileNotFoundError(f"CSV do ab_test_ref não encontrado em {ab_dir}")

    abmap = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ab_csv)
    )
    return orders, consumers, restaurants, abmap


# ---------- Conformizações por tabela ----------

DEFAULT_TZ = "America/Sao_Paulo"  # para relatórios/datas de negócio

def conform_orders(orders: DataFrame, business_tz: str = DEFAULT_TZ) -> DataFrame:
    """
    Tipagem, normalização de timezone, remoção de PII e derivadas úteis para ORDERS.
    - Escolhe 'event_ts_utc' = created_utc (ou scheduled_utc se agendado)
    - Cria 'event_date_brt' = dia do evento em BRT
    - Normaliza lat/long, platform, valores monetários
    - Remove PII de endereço/nome; hasheia CPF
    """
    o = (
        orders
        # tipos
        .withColumn("order_created_at", F.to_timestamp("order_created_at"))
        .withColumn("order_scheduled_date", F.to_timestamp("order_scheduled_date"))
        .withColumn("order_total_amount", F.col("order_total_amount").cast("double"))
        .withColumn("order_scheduled", F.col("order_scheduled").cast("boolean"))
        .withColumn("origin_platform", F.lower(F.col("origin_platform")))
        # timezone: usar merchant_timezone quando existir; senão, default
        .withColumn("tz", F.coalesce(F.col("merchant_timezone"), F.lit(business_tz)))
        .withColumn("created_utc", F.to_utc_timestamp(F.col("order_created_at"), F.col("tz")))
        .withColumn("scheduled_utc", F.to_utc_timestamp(F.col("order_scheduled_date"), F.col("tz")))
        .withColumn(
            "event_ts_utc",
            F.when(F.col("order_scheduled") == True, F.col("scheduled_utc")).otherwise(F.col("created_utc"))
        )
        .withColumn("event_date_brt", F.to_date(F.from_utc_timestamp(F.col("event_ts_utc"), business_tz)))
        # derivados
        .withColumn("basket_size", F.when(F.col("items").isNotNull(), F.size("items")).otherwise(F.lit(0)))
    )

    # qualidade básica e limites razoáveis
    o = (
        o
        .dropna(subset=["order_id", "customer_id"])        
        .dropDuplicates(["order_id"])
        .filter(F.col("order_total_amount") >= 0)          
        .withColumn(
            "merchant_latitude",
            F.when((F.col("merchant_latitude") >= -90) & (F.col("merchant_latitude") <= 90),
                   F.col("merchant_latitude")).otherwise(F.lit(None).cast("double"))
        )
        .withColumn(
            "merchant_longitude",
            F.when((F.col("merchant_longitude") >= -180) & (F.col("merchant_longitude") <= 180),
                   F.col("merchant_longitude")).otherwise(F.lit(None).cast("double"))
        )
    )

    # PII → hasheia CPF e remove campos sensíveis de endereço e nome
    o = (
        o
        .withColumn("cpf_hash", F.sha2(F.col("cpf").cast("string"), 256))
        .drop(
            "cpf", "customer_name", "delivery_address_external_id",
            "delivery_address_city", "delivery_address_country", "delivery_address_district",
            "delivery_address_latitude", "delivery_address_longitude", "delivery_address_state",
            "delivery_address_zip_code"
        )
    )
    return o


def conform_consumers(consumers: DataFrame) -> DataFrame:
    """
    Tipagem/normalização para CONSUMERS.
    - created_at -> timestamp
    - active -> boolean
    - language -> lower
    - remove/hashea PII de nome/telefone
    """
    c = (
        consumers
        .dropna(subset=["customer_id"])
        .dropDuplicates(["customer_id"])
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("active", F.col("active").cast("boolean"))
        .withColumn("language", F.lower(F.col("language")))
        .withColumn("phone_hash", F.sha2(F.concat_ws("-", F.col("customer_phone_area"), F.col("customer_phone_number")), 256))
        .drop("customer_name", "customer_phone_area", "customer_phone_number")  
    )
    c = (
        c
        .withColumnRenamed("created_at", "consumer_created_at")
        .select("customer_id", "language", "active", "phone_hash", "consumer_created_at")
    )
    return c


def conform_restaurants(restaurants: DataFrame) -> DataFrame:
    """
    Tipagem/normalização para RESTAURANTS.
    - id -> merchant_id
    - created_at -> timestamp
    - enabled -> boolean
    - price_range -> int
    - average_ticket, delivery_time, minimum_order_value -> double (>=0)
    """
    r = (
        restaurants
        .withColumnRenamed("id", "merchant_id")
        .dropna(subset=["merchant_id"])
        .dropDuplicates(["merchant_id"])
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("enabled", F.col("enabled").cast("boolean"))
        .withColumn("price_range", F.col("price_range").cast("int"))
        .withColumn("average_ticket", F.col("average_ticket").cast("double"))
        .withColumn("delivery_time", F.col("delivery_time").cast("double"))
        .withColumn("minimum_order_value", F.col("minimum_order_value").cast("double"))
        .withColumn("average_ticket", F.when(F.col("average_ticket") >= 0, F.col("average_ticket")).otherwise(F.lit(None)))
        .withColumn("delivery_time", F.when(F.col("delivery_time") >= 0, F.col("delivery_time")).otherwise(F.lit(None)))
        .withColumn("minimum_order_value", F.when(F.col("minimum_order_value") >= 0, F.col("minimum_order_value")).otherwise(F.lit(None)))
    )
    r = (
        r.withColumnRenamed("created_at", "merchant_created_at")
         .select(
             "merchant_id", "enabled", "price_range",
             "average_ticket", "delivery_time", "minimum_order_value",
             "merchant_zip_code", "merchant_city", "merchant_state", "merchant_country",
             "merchant_created_at"
         )
    )
    return r


# ---------- Montagem do dataset unificado ----------

def clean_and_conform(
    orders: DataFrame,
    consumers: DataFrame,
    restaurants: DataFrame,
    abmap: DataFrame,
    *,
    business_tz: str = DEFAULT_TZ,
    treat_is_target_null_as_control: bool = False,
    experiment_start: str | None = None,
    experiment_end: str | None = None,
) -> DataFrame:
    """
    Normaliza ordens/consumidores/restaurantes, aplica joins e janela do experimento.
    - business_tz: timezone de referência para 'event_date_brt'
    - treat_is_target_null_as_control: se True, nulos -> 0; senão, filtra fora
    - experiment_start/end: strings "YYYY-MM-DD" (inclusivo/exclusivo no filtro UTC)
    """
    o = conform_orders(orders, business_tz=business_tz)
    c = conform_consumers(consumers)
    r = conform_restaurants(restaurants)

    a = (
        abmap
        .select("customer_id", "is_target")
        .dropDuplicates(["customer_id"])
        .withColumn("is_target", F.col("is_target").cast("int"))
    )

    df = (
        o.join(c, on="customer_id", how="left")
         .join(r, on="merchant_id", how="left")
         .join(a, on="customer_id", how="left")
    )

    if treat_is_target_null_as_control:
        df = df.withColumn("is_target", F.coalesce(F.col("is_target"), F.lit(0)))
    else:
        df = df.filter(F.col("is_target").isNotNull())

    # filtro de janela do experimento (UTC)
    if experiment_start:
        df = df.filter(F.col("event_ts_utc") >= F.to_timestamp(F.lit(experiment_start)))
    if experiment_end:
        df = df.filter(F.col("event_ts_utc") < F.to_timestamp(F.lit(experiment_end)))

    return df


# ---------- "Silvers" e agregações ----------

def build_orders_silver(df: DataFrame) -> DataFrame:
    """
    Seleciona o conjunto mínimo de colunas normalizadas ao nível de pedido (fato).
    """
    cols = [
        "order_id", "customer_id", "merchant_id",
        "event_ts_utc", "event_date_brt",
        "order_created_at", "order_scheduled", "order_total_amount",
        "origin_platform", "basket_size",
        "is_target",
        "language", "active", "consumer_created_at",
        "enabled", "price_range", "average_ticket", "delivery_time",
        "minimum_order_value", "merchant_city", "merchant_state", "merchant_country",
        "merchant_created_at",
    ]
    keep = [c for c in cols if c in df.columns]
    return df.select(*keep)


def build_user_aggregates(df_orders_silver: DataFrame) -> DataFrame:
    """
    Agregações por usuário para RFM e A/B.
    - last_order: UTC do último evento
    - frequency: nº pedidos
    - monetary: soma de valor
    - is_target: flag do experimento (primeira ocorrência)
    """
    agg = (
        df_orders_silver.groupBy("customer_id")
        .agg(
            F.max("event_ts_utc").alias("last_order"),
            F.count("*").alias("frequency"),
            F.sum("order_total_amount").alias("monetary"),
            F.first("is_target").alias("is_target"),
        )
    )
    return agg
