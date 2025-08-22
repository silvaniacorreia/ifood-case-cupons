from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import types as T
from scipy import stats

def build_rfm_buckets(
    users_silver: DataFrame,
    *,
    qcuts: Tuple[float, ...] = (0.2, 0.4, 0.6, 0.8),
) -> DataFrame:
    """
    Cria scores R, F, M via quantis e um rótulo RFM.

    Parâmetros:
        users_silver (DataFrame): DataFrame Spark com colunas 'recency','frequency','monetary'.
        qcuts (Tuple[float, ...]): Quantis usados para cortar as distribuições.

    Retorna:
        DataFrame: DataFrame Spark contendo colunas r_score, f_score, m_score e rfm_segment.
    """
    qs = list(qcuts)
    r_q = users_silver.approxQuantile("recency", qs, 1e-3)
    f_q = users_silver.approxQuantile("frequency", qs, 1e-3)
    m_q = users_silver.approxQuantile("monetary", qs, 1e-3)

    def score_q(col, cuts, ascending=True):
        conds = []
        if ascending:
            conds.append((F.col(col) <= cuts[0], F.lit(1)))
            for i in range(len(cuts)-1):
                conds.append(( (F.col(col) > cuts[i]) & (F.col(col) <= cuts[i+1]), F.lit(i+2) ))
            conds.append((F.col(col) > cuts[-1], F.lit(len(cuts)+1)))
        else:
            conds.append((F.col(col) <= cuts[0], F.lit(len(cuts)+1)))
            for i in range(len(cuts)-1):
                conds.append(( (F.col(col) > cuts[i]) & (F.col(col) <= cuts[i+1]), F.lit(len(cuts)-i) ))
            conds.append((F.col(col) > cuts[-1], F.lit(1)))
        expr = F.when(conds[0][0], conds[0][1])
        for cnd, val in conds[1:]:
            expr = expr.when(cnd, val)
        return expr.otherwise(F.lit(3))

    users = (
        users_silver
        .withColumn("r_score", score_q("recency", r_q, ascending=False))
        .withColumn("f_score", score_q("frequency", f_q, ascending=True))
        .withColumn("m_score", score_q("monetary", m_q, ascending=True))
        .withColumn("rfm_segment", F.concat_ws("", F.col("r_score"), F.col("f_score"), F.col("m_score")))
    )
    return users

def ab_metrics_by_segment(
    users_with_segments: DataFrame,
    *,
    segment_col: str,
    top_k_segments: Optional[int] = None
) -> DataFrame:
    """
    Calcula métricas A/B por segmento (Spark).

    Parâmetros:
        users_with_segments (DataFrame): DataFrame Spark contendo colunas de segmento e métricas.
        segment_col (str): Nome da coluna de segmento.
        top_k_segments (Optional[int]): Se fornecido, limita aos top K segmentos por número de usuários.

    Retorna:
        DataFrame: DataFrame Spark com uma linha por (segmento, is_target) contendo as métricas agregadas.
    """
    aov_user = F.when(F.col("frequency") > 0, F.col("monetary") / F.col("frequency"))
    df = (
        users_with_segments
        .groupBy(segment_col, "is_target")
        .agg(
            F.countDistinct("customer_id").alias("usuarios"),
            F.avg("frequency").alias("pedidos_user"),
            F.avg("monetary").alias("gmv_user"),
            F.avg((F.col("frequency") > 0).cast("double")).alias("conversao"),
            F.avg(aov_user).alias("aov"),
        )
    )
    if top_k_segments:
        totals = (
            users_with_segments.groupBy(segment_col)
            .agg(F.countDistinct("customer_id").alias("n"))
            .orderBy(F.col("n").desc())
            .limit(top_k_segments)
        )
        df = df.join(totals.select(segment_col), on=segment_col, how="inner")
    return df.orderBy(segment_col, "is_target")

def _mw(x_t: pd.Series, x_c: pd.Series) -> Dict[str, float]:
    xt = pd.to_numeric(x_t, errors="coerce").dropna()
    xc = pd.to_numeric(x_c, errors="coerce").dropna()
    if len(xt) == 0 or len(xc) == 0:
        return {"U": np.nan, "pval": np.nan}
    U, p = stats.mannwhitneyu(xt, xc, alternative="two-sided")
    return {"U": float(U), "pval": float(p)}

def nonparam_tests_by_segment(
    users_pdf: pd.DataFrame,
    *,
    segment_col: str
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Executa Mann–Whitney por segmento para:
      - monetary (gmv_user), frequency (pedidos_user), aov_user
    Retorna: {segment_value: {metric: {U, pval}}}
    """
    if segment_col not in users_pdf.columns:
        raise KeyError(f"[robust_metrics_by_segment] coluna de segmento '{segment_col}' não está em users_pdf. "
                       f"Colunas disponíveis: {list(users_pdf.columns)}")
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    for seg_val, g in users_pdf.groupby(segment_col):
        t = g[g["is_target"] == 1]
        c = g[g["is_target"] == 0]
        res = {
            "gmv_user_mw": _mw(t["monetary"],  c["monetary"]),
            "pedidos_user_mw": _mw(t["frequency"], c["frequency"]),
            "aov_user_mw": _mw(t["aov_user"], c["aov_user"]),
        }
        out[str(seg_val)] = res
    return out

def robust_metrics_by_segment(users, *, segment_col: str, heavy_threshold: int = 3):
    """
    Versão que aceita tanto pandas DataFrame quanto Spark DataFrame.

    Se `users` for um DataFrame Spark: calcula medianas, p95 e heavy_users por (segment, is_target)
    usando `percentile_approx` e retorna um DataFrame pandas (pequeno) pronto para salvar/plot.

    Se `users` for pandas: mantém o comportamento anterior.
    """
    try:
        from pyspark.sql import DataFrame as SparkDF
        is_spark = isinstance(users, SparkDF)
    except Exception:
        is_spark = False

    if is_spark:
        df_spark = users
        required = [segment_col, "is_target", "customer_id", "monetary", "frequency"]
        missing = [c for c in required if c not in df_spark.columns]
        if missing:
            raise KeyError(f"Colunas faltando para robust_metrics_by_segment (Spark): {missing}")

        aov_expr = F.when(F.col("frequency") > 0, F.col("monetary") / F.col("frequency")).cast(T.DoubleType())
        df_proc = df_spark.select(segment_col, "is_target", "customer_id", "monetary", "frequency")
        df_proc = df_proc.withColumn("aov_user", aov_expr)

        agg = (
            df_proc.groupBy(F.col(segment_col).alias("segment"), F.col("is_target"))
            .agg(
                F.countDistinct("customer_id").alias("usuarios"),
                F.expr("percentile_approx(monetary, 0.5)").alias("median_gmv_user"),
                F.expr("percentile_approx(frequency, 0.5)").alias("median_pedidos_user"),
                F.expr("percentile_approx(aov_user, 0.5)").alias("median_aov_user"),
                F.expr("percentile_approx(monetary, 0.95)").alias("p95_gmv_user"),
                F.expr("percentile_approx(frequency, 0.95)").alias("p95_pedidos_user"),
                F.expr("percentile_approx(aov_user, 0.95)").alias("p95_aov_user"),
                (F.sum((F.coalesce(F.col("frequency"), F.lit(0)) >= heavy_threshold).cast("int")) / F.count("customer_id")).alias("heavy_users_rate"),
                F.lit(heavy_threshold).alias("heavy_threshold")
            )
        )
        pdf = agg.orderBy("segment", "is_target").toPandas()
        return pdf

    else:
        users_pdf = users
        rows = []
        for (seg, tgt), g in users_pdf.groupby([segment_col, "is_target"]):
            gmv = pd.to_numeric(g["monetary"], errors="coerce")
            freq = pd.to_numeric(g["frequency"], errors="coerce")
            aov  = pd.to_numeric(g["aov_user"], errors="coerce")

            rows.append({
                "segment": seg,
                "is_target": int(tgt),
                "usuarios": int(len(g)),
                "median_gmv_user": float(gmv.median()),
                "median_pedidos_user": float(freq.median()),
                "median_aov_user": float(aov.median()),
                "p95_gmv_user": float(np.nanpercentile(gmv.dropna(), 95)) if gmv.notna().any() else np.nan,
                "p95_pedidos_user": float(np.nanpercentile(freq.dropna(), 95)) if freq.notna().any() else np.nan,
                "p95_aov_user": float(np.nanpercentile(aov.dropna(), 95)) if aov.notna().any() else np.nan,
                "heavy_users_rate": float((freq >= heavy_threshold).mean()) if len(freq) else np.nan,
                "heavy_threshold": heavy_threshold
            })
        return pd.DataFrame(rows).sort_values(["segment","is_target"])

def finance_by_segment(
    users_silver,
    *,
    segment_col: str,
    take_rate: float,
    coupon_cost: float,
    redemption_rate: float,
    id_col: str = "customer_id",
    group_col: str = "is_target",
    monetary_col: str = "monetary",
):
    """
    Financeiro por segmento em Spark (100% dos dados).
    Retorna dict {segmento: {...}} com totais e ROI por usuário.
    """
    ab = (users_silver
          .groupBy(segment_col, group_col)
          .agg(F.countDistinct(id_col).alias("usuarios"),
               F.avg(monetary_col).alias("gmv_user")))

    ctrl = (ab.filter(F.col(group_col)==0)
              .select(segment_col, F.col("usuarios").alias("usuarios_ctrl"),
                      F.col("gmv_user").alias("gmv_ctrl")))
    trat = (ab.filter(F.col(group_col)==1)
              .select(segment_col, F.col("usuarios").alias("usuarios_trat"),
                      F.col("gmv_user").alias("gmv_trat")))

    joined = (trat.join(ctrl, on=segment_col, how="inner")
                   .withColumn("uplift", F.col("gmv_trat") - F.col("gmv_ctrl"))
                   .withColumn("receita", F.lit(take_rate) * F.col("uplift") * F.col("usuarios_trat"))
                   .withColumn("custo", F.lit(coupon_cost) * F.lit(redemption_rate) * F.col("usuarios_trat"))
                   .withColumn("lucro", F.col("receita") - F.col("custo"))
                   .withColumn("roi_por_usuario", F.when(F.col("usuarios_trat")>0, F.col("lucro")/F.col("usuarios_trat")).otherwise(F.lit(0.0)))
                   .withColumn("roi_percent", F.when(F.col("custo")>0, F.col("lucro")/F.col("custo")).otherwise(F.lit(None))))

    out = {}
    for r in joined.select(segment_col, "usuarios_trat","gmv_trat","gmv_ctrl","uplift",
                           "receita","custo","lucro","roi_por_usuario","roi_percent").collect():
        out[str(r[segment_col])] = {
            "n_treated": int(r["usuarios_trat"]),
            "gmv_user_treat": float(r["gmv_trat"]),
            "gmv_user_ctrl": float(r["gmv_ctrl"]),
            "uplift_gmv_user": float(r["uplift"]),
            "take_rate": take_rate,
            "coupon_cost": coupon_cost,
            "redemption_rate": redemption_rate,
            "receita_incremental_total": float(r["receita"]),
            "custo_total": float(r["custo"]),
            "lucro_incremental_total": float(r["lucro"]),
            "roi_por_usuario": float(r["roi_por_usuario"]),
            "roi_percent": None if r["roi_percent"] is None else float(r["roi_percent"]),
        }
    return out
