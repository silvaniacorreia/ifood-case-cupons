
from __future__ import annotations

from typing import Optional, List
import pandas as pd

try:
    import pyspark.sql.functions as F
except Exception:  
    F = None  

def break_even_needed(take_rate: float, coupon_cost: float, redemption_rate: float) -> float:
    """
    Valor mínimo de vendas extras por usuário tratado necessário para pagar o cupom.
    Fórmula: (custo_do_cupom * taxa_de_resgate) / take_rate
    Ex.: 10 * 0.30 / 0.23 ≈ 13.04
    """
    if take_rate <= 0:
        raise ValueError("take_rate deve ser > 0")
    return float(coupon_cost) * float(redemption_rate) / float(take_rate)


def break_even_table_spark(
    users_silver,
    *,
    take_rate: float,
    coupon_cost: float,
    redemption_rate: float,
    segment_col: Optional[str] = None,
    id_col: str = "customer_id",
    group_col: str = "is_target",
    monetary_col: str = "monetary",
) -> pd.DataFrame:
    """
    Calcula, no Spark, o uplift de GMV por usuário tratado e compara com o break-even.
    Se segment_col=None, retorna 1 linha (overall). Caso contrário, 1 linha por segmento.

    Parâmetros esperados no `users_silver` (Spark DataFrame):
      - `id_col` (default: customer_id)
      - `group_col` (0 = controle, 1 = tratamento)
      - `monetary_col` (valor total por usuário no período)
      - opcionalmente `segment_col` (ex.: heavy_user, origin_platform)

    Retorna um pandas.DataFrame com as colunas:
      segment, usuarios_trat, gmv_ctrl, gmv_trat, uplift_gmv_user, uplift_needed,
      gap_uplift, receita_total, custo_total, lucro_total, lucro_por_usuario,
      roi_percent, status
    """
    if F is None:
        raise RuntimeError("pyspark não está disponível para executar break_even_table_spark.")

    needed = [id_col, group_col, monetary_col]
    if segment_col:
        needed.append(segment_col)
    missing = [c for c in needed if c not in users_silver.columns]
    if missing:
        raise KeyError(f"Colunas ausentes no users_silver: {missing}")

    by: List[str] = [segment_col, group_col] if segment_col else [group_col]

    ab = (
        users_silver
        .groupBy(*by)
        .agg(
            F.countDistinct(F.col(id_col)).alias("usuarios"),
            F.avg(F.col(monetary_col)).alias("gmv_user")
        )
    )

    cond_ctrl = (F.col(group_col) == 0)
    cond_trat = (F.col(group_col) == 1)

    if segment_col:
        ctrl = ab.filter(cond_ctrl).select(
            F.col(segment_col).alias("segment"),
            F.col("usuarios").alias("usuarios_ctrl"),
            F.col("gmv_user").alias("gmv_ctrl")
        )
        trat = ab.filter(cond_trat).select(
            F.col(segment_col).alias("segment"),
            F.col("usuarios").alias("usuarios_trat"),
            F.col("gmv_user").alias("gmv_trat")
        )
        joined = trat.join(ctrl, on="segment", how="inner")
    else:
        ctrl = ab.filter(cond_ctrl).select(
            F.lit("ALL").alias("segment"),
            F.col("usuarios").alias("usuarios_ctrl"),
            F.col("gmv_user").alias("gmv_ctrl")
        )
        trat = ab.filter(cond_trat).select(
            F.lit("ALL").alias("segment"),
            F.col("usuarios").alias("usuarios_trat"),
            F.col("gmv_user").alias("gmv_trat")
        )
        joined = trat.join(ctrl, on="segment", how="inner")

    uplift_needed = break_even_needed(take_rate, coupon_cost, redemption_rate)

    joined = (
        joined
        .withColumn("uplift_gmv_user", F.col("gmv_trat") - F.col("gmv_ctrl"))
        .withColumn("uplift_needed", F.lit(float(uplift_needed)))
        .withColumn("gap_uplift", F.col("uplift_gmv_user") - F.col("uplift_needed"))
        .withColumn("receita_total", F.lit(float(take_rate)) * F.col("uplift_gmv_user") * F.col("usuarios_trat"))
        .withColumn("custo_total", F.lit(float(coupon_cost) * float(redemption_rate)) * F.col("usuarios_trat"))
        .withColumn("lucro_total", F.col("receita_total") - F.col("custo_total"))
        .withColumn("lucro_por_usuario", F.when(F.col("usuarios_trat") > 0, F.col("lucro_total")/F.col("usuarios_trat")).otherwise(F.lit(0.0)))
        .withColumn("roi_percent", F.when(F.col("custo_total") > 0, F.col("lucro_total")/F.col("custo_total")).otherwise(F.lit(None)))
        .withColumn("status", F.when(F.col("lucro_por_usuario") >= 0, F.lit("OK")).otherwise(F.lit("NEG")))
    )

    cols = [
        "segment","usuarios_trat","gmv_ctrl","gmv_trat","uplift_gmv_user",
        "uplift_needed","gap_uplift","receita_total","custo_total","lucro_total",
        "lucro_por_usuario","roi_percent","status"
    ]
    pdf = joined.select(*cols).toPandas()

    round2 = ["gmv_ctrl","gmv_trat","uplift_gmv_user","uplift_needed","gap_uplift","lucro_por_usuario"]
    for c in round2:
        if c in pdf.columns:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce").astype(float).round(2)
    for c in ["receita_total","custo_total","lucro_total"]:
        if c in pdf.columns:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce").astype(float).round(0)
    if "roi_percent" in pdf.columns:
        pdf["roi_percent"] = pd.to_numeric(pdf["roi_percent"], errors="coerce").astype(float).round(3)

    if "segment" in pdf.columns and pdf["segment"].dtype == object:
        pdf = pdf.sort_values("segment").reset_index(drop=True)

    return pdf

def projetar_lucro(df_break_even: pd.DataFrame, segmentos_ok:list[str],
                   *, modo="continuo", n_campanhas_ano=12,
                   ajustar_cobertura=True, users_silver=None, seg_col=None, annual_factor_continuo=365):
    """
    df_break_even: saída do break_even_table_spark para um segment_col.
    segmentos_ok:  lista de rótulos (coluna 'segment') que vamos manter.
    modo:          'continuo' (multiplica por 365/dias) OU 'campanhas' (multiplica por n_campanhas_ano)
    ajustar_cobertura: ajusta o resultado pela fatia de usuários tratados presentes nesses segmentos
    users_silver:  Spark DF para calcular cobertura (opcional mas recomendado)
    seg_col:       nome da coluna de segmento usada no df_break_even (ex.: 'origin_platform')
    annual_factor_continuo: fator anual para cálculo
    """
    base = df_break_even.copy()
    base_ok = base[base["segment"].isin(segmentos_ok)]

    lucro_periodo = float(base_ok["lucro_total"].sum())  
    if modo == "continuo":
        anual = lucro_periodo * annual_factor_continuo
    else:
        anual = lucro_periodo * n_campanhas_ano

    cobertura = 1.0
    if ajustar_cobertura and users_silver is not None and seg_col is not None:
        tratados = (users_silver.filter(F.col("is_target")==1)
                    .groupBy(seg_col).agg(F.countDistinct("customer_id").alias("n")).toPandas())
        total_trat = tratados["n"].sum()
        fatia = tratados[tratados[seg_col].isin(segmentos_ok)]["n"].sum() / max(total_trat, 1)
        cobertura = float(fatia)
        anual *= cobertura

    return {
        "lucro_periodo_selecionados": round(lucro_periodo, 0),
        "projecao_anual": round(anual, 0),
        "fator_anual": (annual_factor_continuo if modo=="continuo" else n_campanhas_ano),
        "cobertura_aplicada": round(cobertura, 3),
    }
