# Funções de ETL: ingestão, limpeza, normalização (timezone/PII), joins e agregações
"""
Funções principais para o pipeline ETL, incluindo:
- Leitura de dados brutos (JSON, CSV)
- Limpeza e conformidade de dados
- Joins e agregações
- Normalização de timestamps
"""

from __future__ import annotations
import os
import datetime as dt
from pathlib import Path
from typing import Tuple
import gzip, json

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, MapType, StringType
from pyspark.sql.functions import broadcast


from src.checks import preflight, sniff_orders_format, list_valid_ab_csvs

def _parse_ts_any(colname: str) -> F.Column:
    """
    Parseia timestamps em diferentes formatos para um formato unificado.

    Parâmetros:
        colname (str): Nome da coluna contendo os timestamps.

    Retorna:
        F.Column: Coluna Spark com timestamps normalizados.
    """
    c = F.col(colname).cast("string")
    return F.coalesce(
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(c, "yyyy-MM-dd"),
        F.to_timestamp(c)  
    )

# ---------- Leitura dos insumos ----------

def load_raw(spark, raw_dir: str) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Lê os insumos brutos do diretório especificado e retorna DataFrames Spark.

    Parâmetros:
        spark (SparkSession): Sessão Spark ativa.
        raw_dir (str): Caminho para o diretório contendo os dados brutos.

    Retorna:
        Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
            DataFrames para pedidos, consumidores, restaurantes e mapa A/B.
    """
    raw_dir = os.path.abspath(raw_dir)
    rep = preflight(raw_dir, strict=True)
    print("[ETL] preflight OK:", {
        "orders_fmt": rep.get("orders_format_guess"),
        "ab_csv_candidates": len(rep.get("ab_csv_candidates", []))
    })

    orders_path      = os.path.join(raw_dir, "order.json.gz")
    consumers_path   = os.path.join(raw_dir, "consumer.csv.gz")
    restaurants_path = os.path.join(raw_dir, "restaurant.csv.gz")
    ab_dir           = os.path.join(raw_dir, "ab_test_ref_extracted")

    orders_sharded = Path(raw_dir) / "orders_sharded"
    if orders_sharded.exists() and any(orders_sharded.glob("part-*.json")):
        print("[ETL] Lendo orders a partir de shards:", orders_sharded)
        orders = spark.read.json(str(orders_sharded / "part-*.json"))
    else:
        print("[ETL] Lendo orders do único .gz:", orders_path)
        fmt = rep.get("orders_format_guess", "ndjson_or_objects")
        if fmt == "json_array":
            with gzip.open(orders_path, "rt", encoding="utf-8") as f:
                data = json.load(f)
            orders = spark.createDataFrame(data)
        else:
            orders = spark.read.json(orders_path)

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

    csv_paths = [str(p) for p in list_valid_ab_csvs(Path(ab_dir))]
    if not csv_paths:
        raise FileNotFoundError(f"Nenhum CSV válido encontrado em {ab_dir}")
    abmap = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_paths)
    )

    return orders, consumers, restaurants, abmap

# ---------- Conformizações por tabela ----------

DEFAULT_TZ = "America/Sao_Paulo"  

def conform_orders(orders: DataFrame, business_tz: str = DEFAULT_TZ) -> DataFrame:
    cols = set(orders.columns)

    def col_or_null(name, cast_type=None, lower=False):
        if name in cols:
            c = F.col(name)
            if lower: c = F.lower(c.cast("string"))
            if cast_type: c = c.cast(cast_type)
            return c
        return F.lit(None).cast(cast_type) if cast_type else F.lit(None)

    o = (orders
         .withColumn("order_created_at", _parse_ts_any("order_created_at") if "order_created_at" in cols else F.lit(None).cast("timestamp"))
         .withColumn("order_scheduled_date", _parse_ts_any("order_scheduled_date") if "order_scheduled_date" in cols else F.lit(None).cast("timestamp"))
         .withColumn("order_total_amount", col_or_null("order_total_amount", "double"))
         .withColumn("order_scheduled", col_or_null("order_scheduled", "boolean"))
         .withColumn("origin_platform", col_or_null("origin_platform", lower=True))
         .withColumn("merchant_latitude", col_or_null("merchant_latitude", "double"))
         .withColumn("merchant_longitude", col_or_null("merchant_longitude", "double"))
    )

    tz_col = F.coalesce(col_or_null("merchant_timezone", "string"), F.lit(business_tz))
    o = (o
        .withColumn("created_utc",   F.to_utc_timestamp(F.col("order_created_at"), tz_col))
        .withColumn("scheduled_utc", F.to_utc_timestamp(F.col("order_scheduled_date"), tz_col))
        .withColumn(
            "event_ts_utc",
            F.when(
                (F.col("order_scheduled") == True) &
                F.col("scheduled_utc").isNotNull() &
                (F.col("scheduled_utc") >= F.col("created_utc")),  
                F.col("scheduled_utc")
            ).otherwise(F.col("created_utc"))
        )
        .withColumn("event_date_brt", F.to_date(F.from_utc_timestamp(F.col("event_ts_utc"), business_tz)))
    )

    if "items" in cols:
        if isinstance(orders.schema["items"].dataType, ArrayType):
            o = o.withColumn("basket_size", F.size(F.col("items")))
        else:
            o = (
                o.withColumn(
                    "items_parsed",
                    F.when(
                        F.col("items").cast("string").rlike(r"^\s*\["),
                        F.from_json(
                            F.col("items").cast("string"),
                            ArrayType(MapType(StringType(), StringType()))
                        )
                    )
                )
                .withColumn(
                    "basket_size",
                    F.when(F.col("items_parsed").isNotNull(), F.size("items_parsed"))
                     .otherwise(F.lit(None).cast("int"))
                )
                .drop("items_parsed")
            )
    else:
        o = o.withColumn("basket_size", F.lit(None).cast("int"))

    o = (o
         .dropna(subset=["order_id", "customer_id"])
         .dropDuplicates(["order_id"])
         .filter(F.col("order_total_amount").isNull() | (F.col("order_total_amount") >= 0))
         .withColumn("merchant_latitude",
                     F.when((F.col("merchant_latitude")>=-90) & (F.col("merchant_latitude")<=90),
                            F.col("merchant_latitude")).otherwise(F.lit(None).cast("double")))
         .withColumn("merchant_longitude",
                     F.when((F.col("merchant_longitude")>=-180) & (F.col("merchant_longitude")<=180),
                            F.col("merchant_longitude")).otherwise(F.lit(None).cast("double")))
    )

    if "cpf" in cols:
        o = o.withColumn("cpf_hash", F.sha2(F.col("cpf").cast("string"), 256))
    drop_cols = [c for c in [
        "cpf","customer_name","delivery_address_external_id",
        "delivery_address_city","delivery_address_country","delivery_address_district",
        "delivery_address_latitude","delivery_address_longitude","delivery_address_state",
        "delivery_address_zip_code"
    ] if c in o.columns]
    return o.drop(*drop_cols)

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
    r.withColumn("merchant_zip_code", F.col("merchant_zip_code").cast("string"))
     .withColumnRenamed("created_at", "merchant_created_at")
     .select(
         "merchant_id", "enabled", "price_range",
         "average_ticket", "delivery_time", "minimum_order_value",
         "merchant_zip_code", "merchant_city", "merchant_state", "merchant_country",
         "merchant_created_at"
     )
    )
    return r

def conform_abmap(abmap: DataFrame) -> DataFrame:
    """
    Conformação do DataFrame de AB Test.
    """
    df = abmap
    for c in abmap.columns:
        df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))

    cols = set(df.columns)
    candidates = ["is_target","target","treatment","group","ab_group","variant","bucket"]
    group_col = next((c for c in candidates if c in cols), None)
    if group_col is None:
        raise ValueError(f"Não encontrei a coluna de grupo no ab_test_ref. Colunas: {sorted(cols)}")

    df = (df
          .withColumn("customer_id", F.col("customer_id").cast("string"))
          .withColumn("grp_raw", F.col(group_col).cast("string"))
          .withColumn(
              "is_target",
              F.when(F.lower(F.col("grp_raw")).isin("treatment","test","teste","target","variant_b","b","1","true"), F.lit(1))
               .when(F.lower(F.col("grp_raw")).isin("control","controle","variant_a","a","0","false"), F.lit(0))
               .otherwise(F.col(group_col).cast("int"))
          )
          .select("customer_id","is_target")
          .dropDuplicates(["customer_id"])
    )
    return df

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
    auto_infer_window: bool = True,
    use_quantile_window: bool = False,    
    cache_intermediates: bool = False,    
    verbose: bool = False,                
) -> DataFrame:
    o = conform_orders(orders, business_tz=business_tz)
    c = conform_consumers(consumers)
    r = conform_restaurants(restaurants)
    a = conform_abmap(abmap)

    if cache_intermediates:
        o = o.persist()
        c = c.persist()
        r = r.persist()
        a = a.persist()

    if verbose:
        pass

    start_str, end_str = experiment_start, experiment_end
    if auto_infer_window and not (start_str or end_str):
        if use_quantile_window:
            q = o.select(F.col("event_ts_utc").cast("long").alias("t")).na.drop()
            q01, q99 = q.approxQuantile("t", [0.01, 0.99], 0.001)
            if q01 and q99:
                start_dt = dt.datetime.utcfromtimestamp(int(q01))
                end_dt   = dt.datetime.utcfromtimestamp(int(q99)) + dt.timedelta(days=1)
                start_str, end_str = start_dt.date().isoformat(), end_dt.date().isoformat()
                if verbose:
                    print(f"[ETL] Janela INFERIDA (quantis 1–99%): start={start_str} end={end_str} (end exclusivo)")
        else:
            mm = (o.agg(F.min("event_ts_utc").alias("min_ts"),
                        F.max("event_ts_utc").alias("max_ts")).first())
            if mm and mm["min_ts"] and mm["max_ts"]:
                start_str = mm["min_ts"].date().isoformat()
                end_str   = (mm["max_ts"].date() + dt.timedelta(days=1)).isoformat()
                if verbose:
                    print(f"[ETL] Janela INFERIDA a partir dos dados (UTC): start={start_str} end={end_str} (end exclusivo)")

    if start_str or end_str:
        if start_str:
            o = o.filter(F.col("event_ts_utc") >= F.to_timestamp(F.lit(start_str)))
        if end_str:
            o = o.filter(F.col("event_ts_utc") < F.to_timestamp(F.lit(end_str)))
        if verbose:
            pass

    parts = int(o.sparkSession.conf.get("spark.sql.shuffle.partitions"))
    o = o.repartition(parts, "customer_id")

    c = c.select("customer_id", "language", "active", "phone_hash", "consumer_created_at")
    r = r.select("merchant_id", "enabled", "price_range", "average_ticket", "delivery_time",
                 "minimum_order_value", "merchant_city", "merchant_state", "merchant_country", "merchant_created_at")
    a = a.select("customer_id", "is_target")

    # ===== Broadcast em dimensões pequenas =====
    r = F.broadcast(r)

    # ===== Joins =====
    df = (o.join(c, on="customer_id", how="left")
            .join(r, on="merchant_id", how="left")
            .join(a, on="customer_id", how="left"))

    if not treat_is_target_null_as_control:
        if verbose:
            pass
        df = df.filter(F.col("is_target").isNotNull())
        if verbose:
            pass
    else:
        df = df.withColumn("is_target", F.coalesce(F.col("is_target"), F.lit(0)))

    if cache_intermediates:
        df = df.persist()

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

def enrich_orders_for_analysis(df_orders_silver: DataFrame) -> DataFrame:
    """
    Cria colunas derivadas para facilitar a análise, sem alterar as originais:
      - origin_platform_clean  : preenche nulos com 'unknown'
      - language_clean / active_clean : categorias para cortes sem perder linhas
      - minimum_order_value_imputed / delivery_time_imputed : imputação por média em nível de price_range

    OBS: as colunas originais permanecem intactas para auditoria.
    """
    base = (
        df_orders_silver
        .withColumn(
            "origin_platform_clean",
            F.coalesce(F.col("origin_platform").cast("string"), F.lit("unknown"))
        )
        .withColumn(
            "language_clean",
            F.coalesce(F.lower(F.col("language").cast("string")), F.lit("unknown"))
        )
        .withColumn(
            "active_clean",
            F.when(F.col("active").isNull(), F.lit("unknown"))
             .when(F.col("active") == True, F.lit("true"))
             .otherwise(F.lit("false"))
        )
    )

    # ===== MOV imputado por média em nível de price_range =====
    mov_range = (
        base.groupBy("price_range")
            .agg(F.avg("minimum_order_value").alias("mov_mean_range"))
    )
    base = (
        base
        .join(mov_range, ["price_range"], "left")
        .withColumn(
            "minimum_order_value_imputed",
            F.coalesce(F.col("minimum_order_value"), F.col("mov_mean_range"))
        )
        .drop("mov_mean_range")
    )

    # ===== delivery_time imputado por média em nível de price_range =====
    deliv_range = (
        base.groupBy("price_range")
            .agg(F.avg("delivery_time").alias("del_mean_range"))
    )
    base = (
        base
        .join(deliv_range, ["price_range"], "left")
        .withColumn(
            "delivery_time_imputed",
            F.coalesce(F.col("delivery_time"), F.col("del_mean_range"))
        )
        .drop("del_mean_range")
    )

    return base

def build_user_aggregates(df_orders_silver: DataFrame, start_utc: str, end_utc: str) -> DataFrame:
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
        F.first("is_target", ignorenulls=True).alias("is_target"),
        F.min("consumer_created_at").alias("consumer_created_at"),  
        F.last("origin_platform", ignorenulls=True).alias("origin_platform"),
        F.first("active", ignorenulls=True).alias("active")  
        )
        .withColumn("recency", F.datediff(end_utc.cast("timestamp"), F.col("last_order")))
        .withColumn(
            "is_new_customer",
            (F.col("consumer_created_at").cast("timestamp") >= start_utc.cast("timestamp")) &
            (F.col("consumer_created_at").cast("timestamp") <  end_utc.cast("timestamp"))
        )
        .withColumn("heavy_user", (F.col("frequency") >= F.lit(3)))
    )
    return agg
