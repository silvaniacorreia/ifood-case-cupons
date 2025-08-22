import os
from typing import List, Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def ensure_dir(path: str) -> None:
    """
    Cria um diretório se ele não existir.
    """
    os.makedirs(path, exist_ok=True)

def to_pandas_spark(df_spark) -> pd.DataFrame:
    """
    Converte um DataFrame do Spark para um DataFrame do Pandas, se necessário.
    """
    if hasattr(df_spark, "toPandas") and not isinstance(df_spark, pd.DataFrame):
        return df_spark.toPandas()
    return df_spark

def save_table_csv(df: pd.DataFrame, outdir: str, name: str) -> str:
    """
    Salva um DataFrame como um arquivo CSV.
    """
    ensure_dir(outdir)
    p = os.path.join(outdir, f"{name}.csv")
    df.to_csv(p, index=False)
    return p

def plot_bars_by_segment(
    df_or_spark: pd.DataFrame,
    segment_col: str,
    value_cols: List[str],
    group_col: str = "is_target",
    title: Optional[str] = None,
    outdir: Optional[str] = None,
    fname: Optional[str] = None,
    labels_map: Optional[dict] = None,
    y_label: Optional[str] = None,
):
    """
    Plota barras para a comparação entre grupos de controle e tratamento.

    - labels_map: mapeia nome de coluna -> rótulo amigável (ex: "gmv_user" -> "GMV (mediana)").
    - y_label: rótulo do eixo y (ex: "Valor (mediana)").
    """
    df = to_pandas_spark(df_or_spark)
    segs = sorted(df[segment_col].astype(str).unique().tolist())
    groups = sorted(df[group_col].astype(int).unique().tolist())  

    group_name_map = {0: "Controle", 1: "Tratamento"}

    def _pretty_segment_label(s: str) -> str:
        if "heavy" in segment_col.lower():
            sl = s.lower()
            if sl in ("true", "1", "t", "yes", "y"):
                return "Heavy"
            if sl in ("false", "0", "f", "no", "n"):
                return "Não-Heavy"
        return s

    for metric in value_cols:
        fig, ax = plt.subplots(figsize=(10, 5))
        x = np.arange(len(segs))
        width = 0.35
        for gi, g in enumerate(groups):
            vals = []
            for s in segs:
                v = df[(df[segment_col].astype(str) == str(s)) & (df[group_col] == g)][metric]
                vals.append(float(v.values[0]) if len(v) else np.nan)
            legend_label = group_name_map.get(g, f"{group_col}={g}")
            ax.bar(x + (gi - 0.5) * width, vals, width, label=legend_label)

        ax.set_xticks(x)
        ax.set_xticklabels([_pretty_segment_label(s) for s in segs], rotation=30, ha="right")

        pretty_metric = (labels_map or {}).get(metric, metric)
        ax.set_ylabel(y_label or pretty_metric)

        if title:
            ax.set_title(f"{title} — {pretty_metric}")
        else:
            ax.set_title(f"{pretty_metric} por segmento")

        ax.legend()
        plt.tight_layout()
        if outdir and fname:
            ensure_dir(outdir)
            outp = os.path.join(outdir, f"{fname}_{metric}.png")
            plt.savefig(outp, dpi=160, bbox_inches="tight")
        plt.close(fig)

def plot_box_by_segment(
    users_pdf: pd.DataFrame,
    segment_col: str,
    metric_col: str,
    clip_p: Optional[float] = None,
    title: Optional[str] = None,
    outdir: Optional[str] = None,
    fname: Optional[str] = None,
):
    """
    Plota um boxplot para a comparação entre grupos de controle e tratamento.
    """
    df = users_pdf.copy()
    df["segment"] = df[segment_col].astype(str)
    df["group"] = df["is_target"].astype(int)
    x_labels = []
    data = []
    for seg in sorted(df["segment"].unique().tolist()):
        for g in [0, 1]:
            s = df[(df["segment"] == seg) & (df["group"] == g)][metric_col].astype(float)
            s = s.dropna()
            if clip_p is not None and 0 < clip_p < 0.5 and len(s) > 0:
                lo, hi = np.quantile(s, [clip_p, 1-clip_p])
                s = s.clip(lo, hi)
            data.append(s.values)
            x_labels.append(f"{seg}\n{segment_col} | is_target={g}")
    fig, ax = plt.subplots(figsize=(max(10, 1.2*len(x_labels)), 5))
    ax.boxplot(data, showfliers=True)
    ax.set_xticklabels(x_labels, rotation=30, ha="right")
    ax.set_ylabel(metric_col)
    if title:
        ax.set_title(title)
    plt.tight_layout()
    if outdir and fname:
        ensure_dir(outdir)
        outp = os.path.join(outdir, f"{fname}_{metric_col}.png")
        plt.savefig(outp, dpi=160, bbox_inches="tight")
    plt.close(fig)

def plot_hist_by_segment(
    users_pdf: pd.DataFrame,
    segment_col: str,
    metric_col: str,
    bins: int = 40,
    clip_p: Optional[float] = None,
    title: Optional[str] = None,
    outdir: Optional[str] = None,
    fname: Optional[str] = None,
):
    """
    Plota um histograma para a comparação entre grupos de controle e tratamento.
    """
    seg_vals = sorted(users_pdf[segment_col].astype(str).unique().tolist())
    for seg in seg_vals:
        fig, ax = plt.subplots(figsize=(8, 4))
        for g in [0, 1]:
            s = users_pdf[(users_pdf[segment_col].astype(str) == seg) & (users_pdf["is_target"] == g)][metric_col]
            s = pd.to_numeric(s, errors="coerce").dropna()
            if clip_p is not None and 0 < clip_p < 0.5 and len(s) > 0:
                lo, hi = np.quantile(s, [clip_p, 1-clip_p])
                s = s.clip(lo, hi)
            ax.hist(s.values, bins=bins, alpha=0.5, label=f"is_target={g}", density=True)
        ax.set_xlabel(metric_col)
        ax.set_ylabel("densidade")
        ttl = title or f"{metric_col} — segmento={seg}"
        ax.set_title(ttl)
        ax.legend()
        plt.tight_layout()
        if outdir and fname:
            ensure_dir(outdir)
            outp = os.path.join(outdir, f"{fname}_{metric_col}_{seg}.png")
            plt.savefig(outp, dpi=160, bbox_inches="tight")
        plt.close(fig)

def _robust_mapping(which: str):
    which = which.lower()
    if which == "median":
        return {
            "median_gmv_user": "gmv_user",
            "median_pedidos_user": "pedidos_user",
            "median_aov_user": "aov",
        }
    if which == "p95":
        return {
            "p95_gmv_user": "gmv_user",
            "p95_pedidos_user": "pedidos_user",
            "p95_aov_user": "aov",
        }
    raise ValueError("`which` deve ser 'median' ou 'p95'.")

def prepare_bars_from_robust(df_robust, segment_col: str, which: str = "median"):
    """
    Converte um DF 'robusto' (com colunas median_* ou p95_*) para o formato
    esperado por plot_bars_by_segment: gmv_user, pedidos_user, aov.
    Mantém 'is_target' e a coluna de segmento.
    """
    mapping = _robust_mapping(which)
    cols = [segment_col, "is_target"] + list(mapping.keys())
    df = df_robust[cols].rename(columns=mapping).copy()
    return df

def plot_bars_from_robust(
    df_robust,
    segment_col: str,
    which: str = "median",
    title: str = None,
    outdir: str = None,
    fname: str = None,
):
    """
    Faz barras diretamente de um DF robusto.
    which='median' (padrão) ou 'p95'.
    """
    df_bars = prepare_bars_from_robust(df_robust, segment_col, which=which)
    metrics_cols = ["gmv_user", "pedidos_user", "aov"]
    ttl = title or (f"{which.upper()} por segmento (GMV/usuário, Pedidos/usuário, AOV)")

    if which.lower() == "median":
        ylbl = "Valor (mediana)"
        labels_map = {
            "gmv_user": "GMV (mediana)",
            "pedidos_user": "Pedidos (mediana)",
            "aov": "AOV (Mediana)",
        }
    else:
        ylbl = None
        labels_map = {
            "gmv_user": "GMV",
            "pedidos_user": "Pedidos",
            "aov": "AOV",
        }

    return plot_bars_by_segment(
        df_bars,
        segment_col,
        metrics_cols,
        title=ttl,
        outdir=outdir,
        fname=fname,
        labels_map=labels_map,
        y_label=ylbl,
    )

def plot_rate_by_segment(
    df_robust,
    segment_col: str,
    rate_col: str = "heavy_users_rate",
    title: str = None,
    outdir: str = None,
    fname: str = None,
):
    """
    Gráfico de barras para uma taxa por segmento (ex.: heavy_users_rate).
    Espera colunas: segment_col, is_target, rate_col.
    """
    import matplotlib.pyplot as plt
    tmp = (
        df_robust[[segment_col, "is_target", rate_col]]
        .pivot(index=segment_col, columns="is_target", values=rate_col)
        .rename(columns={0: "Controle", 1: "Tratamento"})
        .sort_index()
    )
    ax = tmp.plot(kind="bar", figsize=(9, 5), legend=True)

    ax.set_title(title or "% de heavy users (≥3 pedidos) por segmento")
    ax.set_xlabel("")  
    ax.set_ylabel("% de usuários")

    ax.set_xticks(range(len(tmp.index)))
    ax.set_xticklabels(tmp.index, rotation=30, ha="right")

    leg = ax.get_legend()
    if leg is not None:
        leg.set_title(None)

    plt.tight_layout()
    if outdir and fname:
        import os
        os.makedirs(outdir, exist_ok=True)
        plt.savefig(f"{outdir.rstrip('/')}/{fname}.png", dpi=120)
        plt.close()
    else:
        return ax
