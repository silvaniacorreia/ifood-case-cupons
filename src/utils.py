from __future__ import annotations
import random
from typing import Dict

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

def get_spark(app_name: str, shuffle_partitions: int = 64, driver_memory: str = "12g") -> SparkSession:
    builder = (
        SparkSession.builder
        .master("local[*]")            
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", driver_memory)
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def stop_spark(spark: SparkSession):
    spark.stop()

