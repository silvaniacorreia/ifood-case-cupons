from __future__ import annotations
import os
import random
from typing import Dict

import yaml
from pydantic import BaseModel
from pyspark.sql import SparkSession


# --------- Modelos de configuração ---------
class SparkConfig(BaseModel):
    app_name: str = "ifood-case"
    shuffle_partitions: int = 64

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


# --------- Leitura de parâmetros ---------
def load_settings(path: str = "config/settings.yaml") -> Settings:
    """
    Lê o YAML de configuração.
    """
    if not os.path.exists(path):
        path = "config/settings.yaml"
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return Settings(**data)


# --------- Reprodutibilidade ---------
def set_seeds(seed: int = 2025) -> None:
    random.seed(seed)
    try:
        import numpy as np
        np.random.seed(seed)
    except Exception:
        pass


# --------- Sessão Spark ---------
def get_spark(app_name: str = "ifood-case", shuffle_partitions: int = 64) -> SparkSession:
    """
    Cria/retorna uma SparkSession.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
    )
    builder = builder.master("local[*]")

    spark = builder.getOrCreate()
    return spark

def stop_spark(spark: SparkSession | None) -> None:
    if spark is not None:
        spark.stop()
