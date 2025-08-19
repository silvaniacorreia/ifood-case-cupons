from __future__ import annotations
from typing import Dict, Tuple
import numpy as np
import pandas as pd
from scipy import stats
from pyspark.sql import DataFrame, functions as F

# ========== MÉTRICAS ==========
def compute_ab_summary(users_silver: DataFrame) -> DataFrame:
    """
    Gera o resumo por grupo A/B com as principais métricas.
    Métricas:
      - usuarios: contagem de usuários
      - pedidos_user: média de pedidos por usuário
      - gmv_user: média de GMV por usuário
      - conversao: % usuários com >=1 pedido
      - aov: média do ticket por usuário (monetary/frequency, apenas frequency>0)
    Retorna um DataFrame Spark com uma linha por grupo is_target ∈ {0,1}.
    """
    base = users_silver.select("customer_id", "is_target", "frequency", "monetary")

    aov_user = F.when(F.col("frequency") > 0, F.col("monetary") / F.col("frequency"))

    summary = (
        base.groupBy("is_target")
            .agg(
                F.countDistinct("customer_id").alias("usuarios"),
                F.avg("frequency").alias("pedidos_user"),
                F.avg("monetary").alias("gmv_user"),
                F.avg((F.col("frequency") > 0).cast("double")).alias("conversao"),
                F.avg(aov_user).alias("aov")
            )
            .orderBy("is_target")
    )
    return summary


def collect_user_level_for_tests(users_silver: DataFrame) -> pd.DataFrame:
    """
    Coleta as colunas necessárias para testes estatísticos em um DataFrame pandas:
      - is_target, frequency, monetary, aov_user (freq>0)
      - conv_flag (1 se freq>0, 0 caso contrário)
    """
    pdf = (
        users_silver
        .select("is_target", "frequency", "monetary")
        .withColumn("aov_user", F.when(F.col("frequency") > 0, F.col("monetary") / F.col("frequency")))
        .withColumn("conv_flag", (F.col("frequency") > 0).cast("int"))
        .toPandas()
    )
    return pdf


# ========== TESTES ==========
def welch_ttest(x_treat: pd.Series, x_ctrl: pd.Series) -> Dict[str, float]:
    """
    Executa Welch t-test para duas amostras independentes (médias).
    Retorna estatística t e p-valor (bilateral).
    """
    res = stats.ttest_ind(x_treat, x_ctrl, equal_var=False, nan_policy="omit")
    return {"t": float(res.statistic), "pval": float(res.pvalue)}


def ztest_proportions(p1: float, p2: float, n1: int, n2: int) -> Dict[str, float]:
    """
    Z-test para diferença de proporções (conversão entre grupos).
    Retorna estatística z e p-valor (bilateral).
    """
    p_pool = (p1 * n1 + p2 * n2) / (n1 + n2)
    se = np.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2))
    z = (p1 - p2) / se if se > 0 else np.nan
    pval = 2 * (1 - stats.norm.cdf(abs(z))) if se > 0 else np.nan
    return {"z": float(z), "pval": float(pval)}


def run_ab_tests(users_pdf: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """
    Roda os testes de significância:
      - Welch t-test para gmv_user, pedidos_user, aov_user
      - Z-test para conversão
    Retorna um dicionário com estatísticas e p-values por métrica.
    """
    t = users_pdf[users_pdf["is_target"] == 1]
    c = users_pdf[users_pdf["is_target"] == 0]

    out: Dict[str, Dict[str, float]] = {}

    out["gmv_user"] = welch_ttest(t["monetary"], c["monetary"])
    out["pedidos_user"] = welch_ttest(t["frequency"], c["frequency"])

    aov_t = t["aov_user"].dropna()
    aov_c = c["aov_user"].dropna()
    out["aov"] = welch_ttest(aov_t, aov_c)

    conv_t, conv_c = t["conv_flag"].mean(), c["conv_flag"].mean()
    z = ztest_proportions(conv_t, conv_c, len(t), len(c))
    out["conversao"] = z

    return out


# ========== VIABILIDADE ==========
def financial_viability(
    users_pdf: pd.DataFrame,
    *,
    take_rate: float = 0.23,
    coupon_cost: float = 10.0,  
    redemption_rate: float | None = None,
) -> Dict[str, float]:
    """
    Estima a viabilidade financeira da campanha.
    Definições:
      - Uplift de GMV por usuário: E[GMV_user|T] - E[GMV_user|C]
      - Receita incremental total: uplift_user * N_tratados * take_rate
      - Custo da campanha: N_tratados * redemption_rate * coupon_cost
        (se redemption_rate=None, usa conversão no tratamento como aproximação superior)
      - ROI absoluto: Receita incremental total - Custo
      - ROI por usuário tratado: ROI absoluto / N_tratados
    Retorna um dicionário com métricas de resultado.
    """
    t = users_pdf[users_pdf["is_target"] == 1]
    c = users_pdf[users_pdf["is_target"] == 0]
    n_t = len(t)

    gmv_t = float(t["monetary"].mean())
    gmv_c = float(c["monetary"].mean())
    uplift_user = gmv_t - gmv_c

    receita_inc_total = uplift_user * n_t * take_rate

    conv_t = float(t["conv_flag"].mean()) if "conv_flag" in t.columns else float((t["frequency"] > 0).mean())
    red = conv_t if redemption_rate is None else redemption_rate
    custo_total = n_t * red * coupon_cost

    roi_abs = receita_inc_total - custo_total
    roi_user = roi_abs / n_t if n_t > 0 else np.nan

    return {
        "n_treated": n_t,
        "gmv_user_treat": gmv_t,
        "gmv_user_ctrl": gmv_c,
        "uplift_gmv_user": uplift_user,
        "take_rate": take_rate,
        "coupon_cost": coupon_cost,
        "redemption_rate": red,
        "receita_incremental_total": receita_inc_total,
        "custo_total": custo_total,
        "roi_absoluto": roi_abs,
        "roi_por_usuario": roi_user,
    }
