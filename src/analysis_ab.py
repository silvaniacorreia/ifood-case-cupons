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
    sample_frac: Optional[float] = 0.25,
    seed: int = 42,
    stratify: bool = True,
) -> pd.DataFrame:
    """
    Coleta colunas para testes em pandas. Se `sample_frac` ∈ (0,1),
    amostra no Spark antes do toPandas (opcionalmente estratificado por is_target).
    """
    base_cols = ["customer_id","is_target","frequency","monetary"]
    extended_cols = ["heavy_user","is_new_customer","origin_platform"]

    req = list(required_cols) if required_cols else []
    existing = set(users_silver.columns)

    missing = [c for c in base_cols if c not in existing]
    missing += [c for c in req if c not in existing]
    if missing:
        raise KeyError(f"Colunas ausentes em users_silver: {sorted(set(missing))}")

    to_select = [c for c in base_cols + extended_cols + req if c in existing]
    df = users_silver.select(*to_select)

    if sample_frac is not None and 0 < sample_frac < 1:
        try:
            users_silver.sparkSession.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        except Exception:
            pass
        if stratify and "is_target" in df.columns:
            fracs = {r["is_target"]: float(sample_frac) for r in df.select("is_target").distinct().collect()}
            df = df.sampleBy("is_target", fractions=fracs, seed=seed)
        else:
            df = df.sample(withReplacement=False, fraction=float(sample_frac), seed=seed)

    df = (
        df
        .withColumn("aov_user", F.when(F.col("frequency") > 0, F.col("monetary") / F.col("frequency")).cast(T.DoubleType()))
        .withColumn("conv_flag", (F.col("frequency") > 0).cast(T.IntegerType()))
    )

    keep = to_select + ["aov_user","conv_flag"]
    pdf = df.select(*keep).toPandas()
    ordered = base_cols + ["aov_user","conv_flag"] + [c for c in keep if c not in base_cols + ["aov_user","conv_flag"]]
    pdf = pdf[[c for c in ordered if c in pdf.columns]]

    return pdf

def compute_robust_metrics_spark(users_silver: DataFrame, *, heavy_threshold: int = 3) -> DataFrame:
    """
    Calcula métricas robustas por grupo (controle vs tratamento) usando funções Spark:
      - medianas (percentile_approx 0.5)
      - p95 (percentile_approx 0.95)
      - heavy users rate (freq >= threshold)

    Retorna DataFrame Spark com uma linha por is_target.
    """
    aov_expr = F.when(F.col("frequency") > 0, F.col("monetary") / F.col("frequency")).cast(T.DoubleType())
    df = users_silver.select("customer_id", "is_target", "monetary", "frequency").withColumn("aov_user", aov_expr)

    agg = (
        df.groupBy("is_target")
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
    return agg.orderBy("is_target")

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
    users_silver: DataFrame,
    *,
    take_rate: float,
    coupon_cost: float,
    redemption_rate: float,
    id_col: str = "customer_id",
    group_col: str = "is_target",
    monetary_col: str = "monetary",
) -> Dict[str, float]:
    """
    Calcula viabilidade financeira total usando apenas agregados Spark (100% da base).
    Retorna o mesmo dicionário de chaves usado no notebook original.
    - receita_incremental_total = take_rate * (gmv_user_treat - gmv_user_ctrl) * n_treated
    - custo_total               = coupon_cost * redemption_rate * n_treated
    - roi_absoluto              = receita_incremental_total - custo_total
    - roi_por_usuario           = roi_absoluto / n_treated
    - ltv                       = take_rate * gmv_user_treat
    - cac                       = coupon_cost (custo unitário do cupom)
    """
    ab = (
        users_silver
        .groupBy(group_col)
        .agg(
            F.countDistinct(F.col(id_col)).alias("usuarios"),
            F.avg(F.col(monetary_col)).alias("gmv_user")
        )
    )

    row_ctrl = ab.filter(F.col(group_col) == 0).select("usuarios","gmv_user").first()
    row_tret = ab.filter(F.col(group_col) == 1).select("usuarios","gmv_user").first()

    if row_ctrl is None or row_tret is None:
        raise ValueError("Não foi possível encontrar ambos os grupos (controle=0 e tratamento=1).")

    n_treated = int(row_tret[0])
    gmv_user_ctrl = float(row_ctrl[1]) if row_ctrl[1] is not None else 0.0
    gmv_user_treat = float(row_tret[1]) if row_tret[1] is not None else 0.0
    uplift_gmv_user = gmv_user_treat - gmv_user_ctrl

    receita_incremental_total = take_rate * uplift_gmv_user * n_treated
    custo_total               = coupon_cost * redemption_rate * n_treated
    roi_absoluto              = receita_incremental_total - custo_total
    roi_por_usuario           = (roi_absoluto / n_treated) if n_treated > 0 else 0.0
    ltv                       = take_rate * gmv_user_treat
    cac                       = coupon_cost

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
        "ltv": ltv,
        "cac": cac,
    }
