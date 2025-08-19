from __future__ import annotations
from typing import Dict, Tuple
import numpy as np
import pandas as pd
from scipy import stats
from pyspark.sql import DataFrame, functions as F

# ========== MÉTRICAS ==========
def compute_ab_summary(
    metrics,
    take_rate: float = 0.23,
    coupon_cost: float = 10.0,
    redemption_rate: float = 0.25,
):
    """
    Calcula resumo financeiro e de impacto do experimento A/B.
    Inclui métricas ROI, CAC, LTV e LTV:CAC.
    """
    n_treated = metrics["usuarios_treat"]
    gmv_user_treat = metrics["gmv_user_treat"]
    gmv_user_ctrl = metrics["gmv_user_ctrl"]

    uplift_gmv_user = gmv_user_treat - gmv_user_ctrl
    receita_incremental_total = uplift_gmv_user * n_treated * take_rate

    custo_total = n_treated * coupon_cost * redemption_rate
    roi_absoluto = receita_incremental_total - custo_total
    roi_por_usuario = roi_absoluto / n_treated

    # novas métricas
    ltv = gmv_user_treat * take_rate
    n_redim = max(1, int(n_treated * redemption_rate))
    cac = custo_total / n_redim
    ltv_cac_ratio = ltv / cac if cac > 0 else None

    return {
        "n_treated": n_treated,
        "gmv_user_treat": gmv_user_treat,
        "gmv_user_ctrl": gmv_user_ctrl,
        "uplift_gmv_user": uplift_gmv_user,
        "take_rate": take_rate,
        "coupon_cost": coupon_cost,
        "redemption_rate": redemption_rate,
        "receita_incremental_total": receita_incremental_total,
        "custo_total": custo_total,
        "roi_absoluto": roi_absoluto,
        "roi_por_usuario": roi_por_usuario,
        # novas métricas
        "ltv": ltv,
        "cac": cac,
        "ltv_cac_ratio": ltv_cac_ratio,
    }

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

def compute_robust_metrics(
    users_pdf: pd.DataFrame,
    *,
    heavy_threshold: int = 3
) -> pd.DataFrame:
    """
    Calcula métricas robustas por grupo (controle vs tratamento):
      - Medianas de GMV/usuário, pedidos/usuário e AOV.
      - p95 (percentil 95) para as mesmas métricas.
      - Heavy users: % de usuários com frequência >= heavy_threshold.
    Retorna um DataFrame pandas com uma linha por grupo (is_target).
    """
    def _p95(x: pd.Series) -> float:
        x = x.dropna().astype(float)
        return float(np.percentile(x, 95)) if len(x) else np.nan

    def _heavy_rate(freq: pd.Series, thr: int) -> float:
        freq = freq.fillna(0).astype(float)
        n = len(freq)
        return float((freq >= thr).mean()) if n > 0 else np.nan

    rows = []
    for grp, g in users_pdf.groupby("is_target"):
        gmv = g["monetary"].astype(float)
        freq = g["frequency"].astype(float)
        aov = g["aov_user"].astype(float)

        rows.append({
            "is_target": int(grp),
            "usuarios": int(len(g)),
            # medianas
            "median_gmv_user": float(gmv.median()),
            "median_pedidos_user": float(freq.median()),
            "median_aov_user": float(aov.median(skipna=True)),
            # p95
            "p95_gmv_user": _p95(gmv),
            "p95_pedidos_user": _p95(freq),
            "p95_aov_user": _p95(aov.dropna()),
            # heavy users
            "heavy_users_rate": _heavy_rate(freq, heavy_threshold),
            "heavy_threshold": heavy_threshold,
        })
    return pd.DataFrame(rows)

# ========== TESTES ==========
def welch_ttest(x_treat: pd.Series, x_ctrl: pd.Series) -> Dict[str, float]:
    """
    Executa Welch t-test para duas amostras independentes (médias).
    Retorna estatística t e p-valor (bilateral).
    """
    res = stats.ttest_ind(x_treat, x_ctrl, equal_var=False, nan_policy="omit")
    return {"t": float(res.statistic), "pval": float(res.pvalue)}

def mannwhitney_u(x_treat: pd.Series, x_ctrl: pd.Series) -> Dict[str, float]:
    """
    Mann–Whitney U (não-paramétrico) para comparar distribuições.
    Retorna estatística U e p-valor (bilateral).
    """
    xt = x_treat.dropna().astype(float)
    xc = x_ctrl.dropna().astype(float)
    if len(xt) == 0 or len(xc) == 0:
        return {"U": np.nan, "pval": np.nan}
    U, p = stats.mannwhitneyu(xt, xc, alternative="two-sided")
    return {"U": float(U), "pval": float(p)}

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

def run_nonparam_tests(users_pdf: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """
    Executa Mann–Whitney U para métricas assimétricas:
      - gmv_user, pedidos_user, aov_user.
    Retorna dicionário com U e p-valor por métrica.
    """
    t = users_pdf[users_pdf["is_target"] == 1]
    c = users_pdf[users_pdf["is_target"] == 0]

    out: Dict[str, Dict[str, float]] = {}
    out["gmv_user_mw"] = mannwhitney_u(t["monetary"], c["monetary"])
    out["pedidos_user_mw"] = mannwhitney_u(t["frequency"], c["frequency"])
    out["aov_user_mw"] = mannwhitney_u(t["aov_user"], c["aov_user"])
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
