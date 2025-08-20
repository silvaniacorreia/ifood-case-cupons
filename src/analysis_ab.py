from __future__ import annotations
from typing import Dict, Tuple
import numpy as np
import pandas as pd
from scipy import stats
from pyspark.sql import DataFrame, functions as F, types as T
from typing import Iterable, Optional

# ========== MÉTRICAS ==========
def compute_ab_summary(users_silver: DataFrame) -> DataFrame:
    """
    Resumo descritivo por grupo A/B (controle vs tratamento).
    Métricas:
      - usuarios: contagem de usuários
      - pedidos_user: média de pedidos por usuário
      - gmv_user: média de GMV por usuário
      - conversao: % usuários com >=1 pedido
      - aov: média do ticket por usuário (apenas frequência > 0)
    Retorna um DataFrame Spark com uma linha por grupo (is_target ∈ {0,1}).
    """
    aov_user = F.when(F.col("frequency") > 0, F.col("monetary") / F.col("frequency"))
    return (
        users_silver
        .groupBy("is_target")
        .agg(
            F.countDistinct("customer_id").alias("usuarios"),
            F.avg("frequency").alias("pedidos_user"),
            F.avg("monetary").alias("gmv_user"),
            F.avg((F.col("frequency") > 0).cast("double")).alias("conversao"),
            F.avg(aov_user).alias("aov"),
        )
        .orderBy("is_target")
    )

def collect_user_level_for_tests(
    users_silver: DataFrame,
    *,
    required_cols: Optional[Iterable[str]] = None,
    sample_frac: Optional[float] = None,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Coleta as colunas necessárias para testes estatísticos em um DataFrame pandas.
    Retorna colunas:
      - customer_id, is_target, frequency, monetary
      - heavy_user, is_new_customer, origin_platform
      - aov_user (monetary/frequency quando frequency>0)
      - conv_flag (1 se frequency>0, 0 caso contrário)
    Parâmetros:
      - required_cols: validação opcional das colunas esperadas (falha cedo se ausentes).
      - sample_frac: fração (0–1) para amostrar antes de toPandas() se a base for muito grande.
    """
    base_cols = [
        "customer_id", "is_target", "frequency", "monetary",
        "heavy_user", "is_new_customer", "origin_platform"
    ]
    cols = base_cols

    existing = set(users_silver.columns)
    missing = [c for c in cols if c not in existing]
    if required_cols is not None:
        missing_req = [c for c in required_cols if c not in existing]
        missing.extend(missing_req)
    if missing:
        raise KeyError(f"Colunas ausentes em users_silver: {sorted(set(missing))}")

    df = users_silver.select(*cols)

    if sample_frac is not None and 0 < sample_frac < 1:
        df = df.sample(False, sample_frac, seed=seed)

    df = (
        df
        .withColumn(
            "aov_user",
            F.when(F.col("frequency") > 0, F.col("monetary") / F.col("frequency"))
             .cast(T.DoubleType())
        )
        .withColumn("conv_flag", (F.col("frequency") > 0).cast(T.IntegerType()))
    )

    return df.toPandas()

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
            "median_gmv_user": float(gmv.median()),
            "median_pedidos_user": float(freq.median()),
            "median_aov_user": float(aov.median(skipna=True)),
            "p95_gmv_user": _p95(gmv),
            "p95_pedidos_user": _p95(freq),
            "p95_aov_user": _p95(aov.dropna()),
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
    winsor: float | None = None,  # ex.: 0.01 aplica winsor 1% e 99% em monetary
) -> Dict[str, float]:
    """
    Estima viabilidade financeira da campanha a partir de dados no nível de usuário.
    - Receita incremental total = (E[GMV_user|T] - E[GMV_user|C]) * N_tratados * take_rate
    - Custo total = N_tratados * redemption_rate * coupon_cost
      (se redemption_rate=None, usa conversão do tratamento como aproximação superior)
    - ROI absoluto = Receita incremental total - Custo
    - ROI por usuário = ROI absoluto / N_tratados
    - LTV (horizonte do experimento) = take_rate * E[GMV_user|T]
    - CAC = custo_total / (# usuários que resgataram) ≈ coupon_cost
    """
    df = users_pdf.copy()
    # preparo
    df["conv_flag"] = (df["frequency"] > 0).astype(int)
    if winsor is not None and 0 < winsor < 0.5:
        lo, hi = df["monetary"].quantile([winsor, 1 - winsor])
        df["monetary"] = df["monetary"].clip(lo, hi)

    t = df[df["is_target"] == 1]
    c = df[df["is_target"] == 0]
    n_t = len(t)

    gmv_t = float(t["monetary"].mean())
    gmv_c = float(c["monetary"].mean())
    uplift_user = gmv_t - gmv_c

    conv_t = float(t["conv_flag"].mean())
    red = conv_t if redemption_rate is None else redemption_rate

    receita_inc_total = uplift_user * n_t * take_rate
    custo_total = n_t * red * coupon_cost

    roi_abs = receita_inc_total - custo_total
    roi_user = roi_abs / n_t if n_t > 0 else np.nan

    # LTV/CAC
    ltv = take_rate * gmv_t
    n_red = max(1, int(n_t * red))
    cac = custo_total / n_red

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
        "ltv": ltv,
        "cac": cac,
    }
