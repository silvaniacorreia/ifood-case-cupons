from __future__ import annotations
import os, gzip, json, tarfile
from pathlib import Path
from typing import List, Dict, Any
from pyspark.sql import functions as F

REQUIRED_FILES = {
    "orders":      "order.json.gz",
    "consumers":   "consumer.csv.gz",
    "restaurants": "restaurant.csv.gz",
    "ab_tar":      "ab_test_ref.tar.gz",
}

def _exists_and_size(p: Path) -> tuple[bool, int]:
    return p.exists(), (p.stat().st_size if p.exists() else 0)

def validate_gzip(p: Path) -> bool:
    """
    Valida a integridade de um arquivo gzip.

    Parâmetros:
        p (Path): Caminho para o arquivo gzip.

    Retorna:
        bool: True se o arquivo for válido, False caso contrário.
    """
    try:
        with gzip.open(p, "rb") as g:
            g.peek(1)
        return True
    except Exception:
        return False

def preview_tar_members(p: Path, limit: int = 5) -> list[str]:
    """
    Retorna os nomes dos primeiros arquivos em um arquivo tar.gz.

    Parâmetros:
        p (Path): Caminho para o arquivo tar.gz.
        limit (int): Número máximo de arquivos a serem retornados.

    Retorna:
        list[str]: Lista com os nomes dos arquivos.
    """
    names = []
    with tarfile.open(p, "r:gz") as tar:
        for m in tar.getmembers():
            if m.isfile():
                names.append(m.name)
            if len(names) >= limit:
                break
    return names

def list_valid_ab_csvs(ab_dir: Path, min_bytes: int = 1024) -> list[Path]:
    """
    Lista arquivos CSV válidos no diretório de A/B.

    Parâmetros:
        ab_dir (Path): Caminho para o diretório de A/B.
        min_bytes (int): Tamanho mínimo em bytes para considerar um arquivo válido.

    Retorna:
        list[Path]: Lista de caminhos para arquivos CSV válidos.
    """
    if not ab_dir.exists():
        return []
    out = []
    for p in ab_dir.rglob("*.csv"):
        if p.name.startswith("._") or p.name.startswith("."):
            continue  
        try:
            if p.stat().st_size >= min_bytes:
                out.append(p)
        except Exception:
            pass
    out.sort(key=lambda x: x.stat().st_size if x.exists() else 0, reverse=True)
    return out

def sniff_orders_format(orders_gz: Path) -> str:
    """
    Retorna:
      - "json_array" se o arquivo começa com '['
      - "ndjson_or_objects" caso contrário (1 JSON por linha ou objetos por linha)
      - "missing" se o arquivo não existir
      - "error:<mensagem>" em caso de erro
    """
    if not orders_gz.exists():
        return "missing"
    try:
        with gzip.open(orders_gz, "rt", encoding="utf-8") as f:
            first_non_ws = None
            while True:
                ch = f.read(1)
                if not ch:
                    break
                if not ch.isspace():
                    first_non_ws = ch
                    break
        if first_non_ws == "[":
            return "json_array"
        return "ndjson_or_objects"
    except Exception as e:
        return f"error:{e}"

def preflight(raw_dir: str | Path, strict: bool = True) -> Dict[str, Any]:
    """
    Executa checagens de presença, tamanho e integridade dos arquivos necessários.

    Parâmetros:
        raw_dir (str | Path): Caminho para o diretório contendo os arquivos brutos.
        strict (bool): Se True, levanta erros críticos em caso de problemas.

    Retorna:
        Dict[str, Any]: Relatório detalhado das checagens realizadas.
    """
    raw = Path(raw_dir)
    report: Dict[str, Any] = {"raw_dir": str(raw), "files": {}, "ab_csv_candidates": []}

    for key, fname in REQUIRED_FILES.items():
        p = raw / fname
        exists, size = _exists_and_size(p)
        item = {"path": str(p), "exists": exists, "size_bytes": size}
        if fname.endswith(".gz") and exists:
            item["gzip_ok"] = validate_gzip(p)
        if fname.endswith(".tar.gz") and exists:
            try:
                item["tar_preview"] = preview_tar_members(p, 5)
                item["tar_ok"] = True
            except Exception as e:
                item["tar_ok"] = False
                item["tar_error"] = str(e)
        report["files"][key] = item

    ab_dir = raw / "ab_test_ref_extracted"
    valids = list_valid_ab_csvs(ab_dir)
    report["ab_csv_candidates"] = [str(p) for p in valids]

    orders_fmt = sniff_orders_format(raw / REQUIRED_FILES["orders"])
    report["orders_format_guess"] = orders_fmt

    criticals = []
    for k in ["orders", "consumers", "restaurants", "ab_tar"]:
        f = report["files"].get(k, {})
        if not f.get("exists"):
            criticals.append(f"arquivo ausente: {k} -> {f.get('path')}")
    if report["files"].get("orders", {}).get("exists") and not report["files"]["orders"].get("gzip_ok", False):
        criticals.append("orders gzip inválido")
    if report["files"].get("ab_tar", {}).get("exists") and not report["files"]["ab_tar"].get("tar_ok", False):
        criticals.append("ab_test_ref.tar.gz inválido")
    if not report["ab_csv_candidates"]:
        criticals.append("nenhum CSV válido encontrado em data/raw/ab_test_ref_extracted/")

    if strict and criticals:
        msg = "Preflight falhou:\n - " + "\n - ".join(criticals)
        raise RuntimeError(msg)

    return report

def profile_loaded(orders, consumers, restaurants, abmap, n: int = 5, light: bool = True):
    """Resumo rápido dos 4 DataFrames já carregados.
       light=True evita operações pesadas (count/agg/shuffle)."""

    def _nulls(df):
        return None  

    print("\n=== PROFILE: ORDERS ===")
    if not light:
        print("rows:", orders.count())
        orders.printSchema()
    else:
        orders.printSchema()
    try:
        orders.select("order_id","customer_id","merchant_id","order_total_amount","order_created_at").limit(n).show(n, truncate=False)
    except Exception:
        orders.limit(n).show(n, truncate=False)

    if not light:
        for ts in ["order_created_at","event_ts_utc"]:
            if ts in orders.columns:
                orders.agg(F.min(ts).alias("min_"+ts), F.max(ts).alias("max_"+ts)).show()

    print("\n=== PROFILE: CONSUMERS ===")
    if not light:
        print("rows:", consumers.count())
        consumers.printSchema()
        consumers.limit(n).show(n, truncate=False)
    else:
        consumers.printSchema()
        consumers.limit(n).show(n, truncate=False)

    print("\n=== PROFILE: RESTAURANTS ===")
    if not light:
        print("rows:", restaurants.count())
        restaurants.printSchema()
        restaurants.limit(n).show(n, truncate=False)
    else:
        restaurants.printSchema()
        restaurants.limit(n).show(n, truncate=False)

    print("\n=== PROFILE: ABMAP ===")
    if not light:
        print("rows:", abmap.count())
        abmap.printSchema()
        abmap.limit(n).show(n, truncate=False)
        for g in ["is_target","target","treatment","group","ab_group","variant","bucket"]:
            if g in abmap.columns:
                abmap.groupBy(g).count().show()
                break
    else:
        abmap.printSchema()
        abmap.limit(n).show(n, truncate=False)

def check_post_etl(
    orders_silver,
    users_silver,
    *,
    light: bool = True,
    key_cols: list[str] | None = None,
    sample_frac: float = 0.001,
    preview_rows: int = 5,
    use_pandas_preview: bool = False,
    check_semantic_dups: bool = True,
):
    """
    Executa checagens leves pós-ETL para registro no Colab.
    - light=True: nulos apenas em colunas-chave e previews por sample.
    - light=False: nulos em todas as colunas (lento).
    - check_semantic_dups: investiga duplicatas semânticas na fato (lento moderado).
    """
    if key_cols is None:
        key_cols = [
            "order_id", "customer_id", "merchant_id",
            "event_ts_utc", "order_total_amount",
            "is_target", "price_range", "language", "active",
            "delivery_time_imputed", "minimum_order_value_imputed",
        ]
    key_cols = [c for c in key_cols if c in orders_silver.columns]

    print("Faixa de datas (UTC) em orders_silver:")
    orders_silver.agg(
        F.min("event_ts_utc").alias("min_utc"),
        F.max("event_ts_utc").alias("max_utc"),
    ).show(truncate=False)

    print("Split A/B (users):")
    users_silver.groupBy("is_target").count().orderBy("is_target").show()

    def nulls_by_col(df, cols):
        exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]
        return df.select(exprs)

    if light:
        print(f"Nulos (colunas-chave): {key_cols}")
        nulls_by_col(orders_silver, key_cols).show(truncate=False)
    else:
        print("Nulos (todas as colunas) — operação pesada:")
        nulls_by_col(orders_silver, orders_silver.columns).show(truncate=False)

    if check_semantic_dups:
        print("\nPossíveis duplicatas sistêmicas (mesmo cliente/restaurante/ts/valor, order_id distinto):")
        dups = (
            orders_silver
            .groupBy("customer_id", "merchant_id", "event_ts_utc", "order_total_amount")
            .agg(
                F.countDistinct("order_id").alias("n_orders"),
                F.collect_set("order_id").alias("order_ids"),
            )
            .filter(F.col("n_orders") > 1)
        )
        total_dups = dups.count()
        print(f"Total de combinações com múltiplos order_id: {total_dups}")
        if total_dups > 0:
            dups.select("customer_id","merchant_id","event_ts_utc","order_total_amount","n_orders","order_ids")\
                .orderBy(F.col("n_orders").desc())\
                .show(10, truncate=False)

    print("\nPreview orders_silver (sample leve):")
    orders_preview_cols = [c for c in [
        "price_range","order_id","customer_id","merchant_id",
        "event_ts_utc","order_total_amount","origin_platform",
        "is_target","language","active"
    ] if c in orders_silver.columns]
    preview_df = orders_silver.sample(False, sample_frac, seed=42).select(*orders_preview_cols)
    if preview_df.rdd.isEmpty():
        preview_df = orders_silver.select(*orders_preview_cols).limit(preview_rows)
    preview_df.show(preview_rows, truncate=False)

    print("\nPreview users_silver (primeiras linhas):")
    users_preview_cols = [c for c in [
        "customer_id","last_order","frequency","monetary","is_target","recency"
    ] if c in users_silver.columns]
    users_silver.select(*users_preview_cols).show(preview_rows, truncate=False)

    if use_pandas_preview:
        try:
            from IPython.display import display
            display(orders_silver.limit(preview_rows).toPandas())
            display(users_silver.limit(preview_rows).toPandas())
        except Exception:
            pass
