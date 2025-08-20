from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, functions as F
from scipy import stats

from src.analysis_ab import financial_viability

# -----------------------------
# 1) RFM buckets (Spark)
# -----------------------------
def build_rfm_buckets(
    users_silver: DataFrame,
    *,
    qcuts: Tuple[float, ...] = (0.2, 0.4, 0.6, 0.8),
) -> DataFrame:
    """
    Cria scores R, F, M via quantis e um rótulo RFM.
    Regra comum:
      - Recência: menor é melhor (score invertido)
      - Frequência/Monetary: maior é melhor
    Retorna um DataFrame Spark com colunas r_score, f_score, m_score, rfm_segment.
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

# -----------------------------
# 2) Agregações por segmento (Spark)
# -----------------------------
def ab_metrics_by_segment(
    users_with_segments: DataFrame,
    *,
    segment_col: str,
    top_k_segments: Optional[int] = None
) -> DataFrame:
    """
    Calcula métricas A/B por segmento (Spark):
      usuarios, pedidos_user, gmv_user, conversao, aov
    Retorna um DF com linhas por (segmento, is_target).
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

# -----------------------------
# 3) Testes por segmento (Pandas)
# -----------------------------
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

# -----------------------------
# 4) Métricas robustas por segmento (Pandas)
# -----------------------------
def robust_metrics_by_segment(
    users_pdf: pd.DataFrame,
    *,
    segment_col: str,
    heavy_threshold: int = 3
) -> pd.DataFrame:
    """
    Calcula medianas, p95 e taxa de heavy users por segmento e grupo A/B.
    Retorna DataFrame pandas com linhas por (segment, is_target).
    """
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
    users_pdf: pd.DataFrame,
    *,
    segment_col: str,
    take_rate: float = 0.23,
    coupon_cost: float = 10.0,
    redemption_rate: float = 0.30
) -> Dict[str, Dict[str, float]]:
    """
    Calcula viabilidade financeira por segmento reaproveitando financial_viability.
    Retorna: {segment_value: {... métricas financeiras ...}}
    """
    out: Dict[str, Dict[str, float]] = {}
    for seg_val, g in users_pdf.groupby(segment_col):
        out[str(seg_val)] = financial_viability(
            g, take_rate=take_rate, coupon_cost=coupon_cost, redemption_rate=redemption_rate
        )
    return out
