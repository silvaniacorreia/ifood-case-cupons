# Funções de ETL: ingestão, limpeza, normalização (timezone/PII), joins e agregações
from __future__ import annotations
import os
import datetime as dt
from pathlib import Path
from typing import Tuple
import gzip, json

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType
from pyspark.sql.types import ArrayType
import datetime as dt

from src.checks import preflight, sniff_orders_format, list_valid_ab_csvs

def _parse_ts_any(colname: str) -> F.Column:
    """
    Função para parsear timestamps em diferentes formatos.
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
    Lê os 4 insumos do case a partir de data/raw/.
    Retorna: (orders, consumers, restaurants, abmap)
    """
    raw_dir = os.path.abspath(raw_dir)
    # 0) pré-checagens (fail-fast)
    rep = preflight(raw_dir, strict=True)
    print("[ETL] preflight OK:", {
        "orders_fmt": rep.get("orders_format_guess"),
        "ab_csv_candidates": len(rep.get("ab_csv_candidates", []))
    })

    orders_path      = os.path.join(raw_dir, "order.json.gz")
    consumers_path   = os.path.join(raw_dir, "consumer.csv.gz")
    restaurants_path = os.path.join(raw_dir, "restaurant.csv.gz")
    ab_dir           = os.path.join(raw_dir, "ab_test_ref_extracted")

    # --- ORDERS: leitura robusta (NDJSON ou JSON array) ---
    def read_orders_auto(path: str) -> DataFrame:
        df_try = spark.read.json(path)    # NDJSON/objetos por linha (distribuída)
        if df_try.count() > 5:
            return df_try
        # fallback: cheirar o arquivo
        with gzip.open(path, "rt", encoding="utf-8") as f:
            first_non_ws = None
            while True:
                ch = f.read(1)
                if not ch:
                    break
                if not ch.isspace():
                    first_non_ws = ch
                    break
            f.seek(0)
            if first_non_ws == "[":
                data = json.load(f)  # JSON array (ok para tamanho do case)
                return spark.createDataFrame(data)
            else:
                data = [json.loads(line) for line in f if line.strip()]
                return spark.createDataFrame(data)

    orders = read_orders_auto(orders_path)

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

    # --- A/B: ler TODOS os CSVs válidos (ignora ._* e minúsculos) ---
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
             F.when((F.col("order_scheduled") == True) & F.col("scheduled_utc").isNotNull(),
                    F.col("scheduled_utc")).otherwise(F.col("created_utc"))
         )
         .withColumn("event_date_brt", F.to_date(F.from_utc_timestamp(F.col("event_ts_utc"), business_tz)))
    )

    if "items" in cols and isinstance(orders.schema["items"].dataType, ArrayType):
        o = o.withColumn("basket_size", F.size("items"))
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
        r.withColumnRenamed("created_at", "merchant_created_at")
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
) -> DataFrame:
    o = conform_orders(orders, business_tz=business_tz)
    c = conform_consumers(consumers)
    r = conform_restaurants(restaurants)
    a = conform_abmap(abmap)

    print("[ETL] counts: orders_raw =", orders.count())
    print("[ETL] counts: orders_conformed (event_ts_utc not null) =", o.filter(F.col("event_ts_utc").isNotNull()).count())
    print("[ETL] counts: abmap =", a.count())

    df = (o.join(c, on="customer_id", how="left")
           .join(r, on="merchant_id", how="left")
           .join(a, on="customer_id", how="left"))

    print("[ETL] after join (total) =", df.count())

    if treat_is_target_null_as_control:
        df = df.withColumn("is_target", F.coalesce(F.col("is_target"), F.lit(0)))
    else:
        before = df.count()
        df = df.filter(F.col("is_target").isNotNull())
        print("[ETL] drop is_target null ->", before, "→", df.count())

    start_str, end_str = experiment_start, experiment_end
    if auto_infer_window and not (start_str or end_str):
        mm = (o.agg(F.min("event_ts_utc").alias("min_ts"),
                    F.max("event_ts_utc").alias("max_ts"))
                .first())
        if mm and mm["min_ts"] and mm["max_ts"]:
            start_str = mm["min_ts"].date().isoformat()
            end_str   = (mm["max_ts"].date() + dt.timedelta(days=1)).isoformat()
            print(f"[ETL] Janela INFERIDA a partir dos dados (UTC): start={start_str} end={end_str} (end exclusivo)")
        else:
            print("[ETL] Não foi possível inferir janela — sem filtro.")

    before = df.count()
    if start_str:
        df = df.filter(F.col("event_ts_utc") >= F.to_timestamp(F.lit(start_str)))
    if end_str:
        df = df.filter(F.col("event_ts_utc") < F.to_timestamp(F.lit(end_str)))
    if (start_str or end_str):
        print("[ETL] filtro janela ->", before, "→", df.count())

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
