from __future__ import annotations
import random
from typing import Dict
import time

import yaml
from pydantic import BaseModel
from pyspark.sql import SparkSession
import numpy as np

# --------- Modelos de configuração ---------
class SparkConfig(BaseModel):
    app_name: str = "ifood-case-cupons"
    shuffle_partitions: int = 64
    driver_memory: str = "8g"

class RuntimeConfig(BaseModel):
    seed: int = 2025
    spark: SparkConfig = SparkConfig()

class DataPaths(BaseModel):
    raw_dir: str = "data/raw"
    processed_dir: str = "data/processed"
    files: Dict[str, str] = {
        "orders_json_gz": "order.json.gz",
        "consumers_csv_gz": "consumer.csv.gz",
        "restaurants_csv_gz": "restaurant.csv.gz",
        "ab_test_tar_gz": "ab_test_ref.tar.gz",
    }

class AnalysisConfig(BaseModel):
    use_cuped: bool = True
    winsorize: float = 0.02
    business_tz: str = "America/Sao_Paulo"
    treat_is_target_null_as_control: bool = False
    experiment_window: dict[str, str] | None = None
    auto_infer_window: bool = True
class RFMConfig(BaseModel):
    quantiles: list[float] = [0.2, 0.4, 0.6, 0.8]

class FinanceConfig(BaseModel):
    take_rate: float = 0.15
    coupon_cost_default: float = 6.0
    coupon_cost_grid: list[float] = [3, 6, 9, 12, 15]

class Settings(BaseModel):
    runtime: RuntimeConfig = RuntimeConfig()
    data: DataPaths = DataPaths()
    analysis: AnalysisConfig = AnalysisConfig()
    rfm: RFMConfig = RFMConfig()
    finance: FinanceConfig = FinanceConfig()

def load_settings(path: str = "config/settings.yaml") -> Settings:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return Settings(**raw)

def set_seeds(seed: int = 42):
    random.seed(seed)
    np.random.seed(seed)

def get_spark(app_name: str, shuffle_partitions: int = 64, extra_conf: dict | None = None):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    )
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    if extra_conf:
        for k, v in extra_conf.items():
            spark.conf.set(k, v)

    return spark

def stop_spark(spark: SparkSession):
    spark.stop()

def benchmark_shuffle(spark: SparkSession, df, shuffle_partitions_list: list[int]) -> dict[int, float]:
    results = {}
    for partitions in shuffle_partitions_list:
        spark.conf.set("spark.sql.shuffle.partitions", partitions)
        print(f"Testing shuffle_partitions={partitions}...")
        start_time = time.time()
        # Exemplo de operação de shuffle (groupBy + count)
        df.groupBy("customer_id").count().collect()
        elapsed_time = time.time() - start_time
        results[partitions] = elapsed_time
        print(f"Elapsed time: {elapsed_time:.2f} seconds")
    return results

