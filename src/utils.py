from __future__ import annotations
import random
from typing import Dict
import time

import yaml
from pydantic import BaseModel
from pyspark.sql import SparkSession
import numpy as np

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
    take_rate: float = 0.23
    coupon_cost_default: float = 10.0
    coupon_cost_grid: list[float] = [5, 10, 15, 20, 25]

class Settings(BaseModel):
    runtime: RuntimeConfig = RuntimeConfig()
    data: DataPaths = DataPaths()
    analysis: AnalysisConfig = AnalysisConfig()
    rfm: RFMConfig = RFMConfig()
    finance: FinanceConfig = FinanceConfig()

def load_settings(path: str = "config/settings.yaml") -> Settings:
    """
    Carrega as configurações do arquivo YAML especificado.

    Parâmetros:
        path (str): Caminho para o arquivo de configurações YAML.

    Retorna:
        Settings: Objeto contendo as configurações carregadas.
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return Settings(**raw)

def set_seeds(seed: int = 42):
    """
    Define a semente para a geração de números aleatórios.
    """
    random.seed(seed)
    np.random.seed(seed)

def get_spark(app_name: str, shuffle_partitions: int = 64, extra_conf: dict | None = None):
    """
    Inicializa uma SparkSession com configurações padrão e adicionais.

    Parâmetros:
        app_name (str): Nome do aplicativo Spark.
        shuffle_partitions (int): Número de partições para operações de shuffle.
        extra_conf (dict | None): Configurações adicionais para a SparkSession.

    Retorna:
        SparkSession: Instância configurada da SparkSession.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false") 
        .config("spark.driver.extraJavaOptions", "-Xss8m")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    if extra_conf:
        for k, v in extra_conf.items():
            spark.conf.set(k, v)

    spark.sparkContext.setLogLevel("ERROR")

    return spark

def stop_spark(spark: SparkSession):
    spark.stop()

def benchmark_shuffle(spark: SparkSession, df, shuffle_partitions_list: list[int]) -> dict[int, float]:
    results = {}
    for partitions in shuffle_partitions_list:
        spark.conf.set("spark.sql.shuffle.partitions", partitions)
        print(f"Testing shuffle_partitions={partitions}...")
        start_time = time.time()
        df.groupBy("customer_id").count().collect()
        elapsed_time = time.time() - start_time
        results[partitions] = elapsed_time
        print(f"Elapsed time: {elapsed_time:.2f} seconds")
    return results
