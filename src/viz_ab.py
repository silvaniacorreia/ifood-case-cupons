from __future__ import annotations
import os
from typing import Iterable, Optional, List
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def _fmt_axes(ax, y_label: str, title: Optional[str] = None, y_log: bool = False):
    ax.set_ylabel(y_label)
    if title: ax.set_title(title)
    if y_log:
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(plt.MaxNLocator(8))
    ax.grid(True, axis="y", alpha=0.25)

def save_table_csv(df: pd.DataFrame, outdir: str, name: str) -> str:
    """
    Salva um DataFrame como um arquivo CSV.
    """
    _ensure_dir(outdir)
    outp = os.path.join(outdir, f"{name}.csv")
    df.to_csv(outp, index=False)
    return outp

def to_pandas_safe(df):
    """
    Converte um DataFrame do Spark para um DataFrame do Pandas, se necessário.
    """
    try:
        if hasattr(df, "toPandas") and not isinstance(df, pd.DataFrame):
            return df.toPandas()
    except Exception:
        pass
    return df

def plot_ab_box(
    users_pdf_or_spark,
    metric_col: str,
    *,
    outdir: str,
    fname: Optional[str] = None,
    clip_p: Optional[float] = 0.01,
    y_log: bool = False,
    title: Optional[str] = None,
):
    """
    Plota um boxplot para a comparação entre grupos de controle e tratamento.
    """
    # mapeamento amigável de nomes de métricas para eixo/título
    label_map = {
        "frequency": "Pedidos por usuário",
        "monetary": "GMV por usuário",
        "aov_user": "AOV por usuário",
        "aov": "AOV por usuário",
    }
    pretty = label_map.get(metric_col.lower(), metric_col)

    df = to_pandas_safe(users_pdf_or_spark).copy()
    df = df[["is_target", metric_col]].dropna()
    df["is_target"] = df["is_target"].astype(int)

    s0 = pd.to_numeric(df[df["is_target"]==0][metric_col], errors="coerce").dropna()
    s1 = pd.to_numeric(df[df["is_target"]==1][metric_col], errors="coerce").dropna()
    if clip_p and 0 < clip_p < 0.5:
        def _clip(s):
            lo, hi = np.quantile(s, [clip_p, 1-clip_p])
            return s.clip(lo, hi)
        s0, s1 = _clip(s0), _clip(s1)

    fig, ax = plt.subplots(figsize=(8,4))
    ax.boxplot([s0.values, s1.values], showfliers=True, labels=["Controle", "Tratamento"])
    _fmt_axes(ax, y_label=pretty, title=title or f"Boxplot • {pretty}", y_log=y_log)
    plt.tight_layout()
    _ensure_dir(outdir)
    outp = os.path.join(outdir, f"{fname or ('box_'+metric_col)}.png")
    plt.savefig(outp, dpi=160, bbox_inches="tight"); plt.close(fig)
    return outp

def plot_ab_hist_overlay(
    users_pdf: pd.DataFrame,
    metric_col: str,
    *,
    bins: int = 50,
    clip_p: Optional[float] = 0.01,
    outdir: str,
    fname: Optional[str] = None,
    title: Optional[str] = None,
):
    """
    Plota um histograma sobreposto para a comparação entre grupos de controle e tratamento.
    """
    # mapeamento amigável de nomes de métricas para eixo/título
    label_map = {
        "frequency": "Pedidos por usuário",
        "monetary": "GMV por usuário",
        "aov_user": "AOV por usuário",
        "aov": "AOV por usuário",
    }
    pretty = label_map.get(metric_col.lower(), metric_col)

    df = users_pdf.copy()
    df = df[["is_target", metric_col]].dropna()
    df["is_target"] = df["is_target"].astype(int)

    s0 = pd.to_numeric(df[df["is_target"]==0][metric_col], errors="coerce").dropna()
    s1 = pd.to_numeric(df[df["is_target"]==1][metric_col], errors="coerce").dropna()
    if clip_p and 0 < clip_p < 0.5:
        def _clip(s):
            lo, hi = np.quantile(s, [clip_p, 1-clip_p])
            return s.clip(lo, hi)
        s0, s1 = _clip(s0), _clip(s1)

    fig, ax = plt.subplots(figsize=(8,4))
    ax.hist(s0.values, bins=bins, alpha=0.5, density=True, label="Controle")
    ax.hist(s1.values, bins=bins, alpha=0.5, density=True, label="Tratamento")
    ax.legend()
    ax.set_xlabel(pretty)
    ax.set_ylabel("densidade")
    ax.set_title(title or f"Distribuição • {pretty}")
    plt.tight_layout()
    _ensure_dir(outdir)
    outp = os.path.join(outdir, f"{fname or ('hist_'+metric_col)}.png")
    plt.savefig(outp, dpi=160, bbox_inches="tight"); plt.close(fig)
    return outp

def plot_group_bars(
    df_summary: pd.DataFrame,
    *,
    metrics: Iterable[str],
    labels_map: Optional[dict] = None,
    outdir: str,
    fname: str = "group_bars",
    title: Optional[str] = None
):
    """
    Plota barras para a comparação entre grupos de controle e tratamento.
    """
    d = df_summary.copy()
    d["Grupo"] = d["is_target"].map({0:"Controle",1:"Tratamento"}).astype(str)

    median_default_map = {
        "GMV mediano": "GMV (mediana)",
        "GMV mediano ": "GMV (mediana)",
        "Pedidos medianos": "Pedidos (mediana)",
        "Pedidos medianos ": "Pedidos (mediana)",
        "AOV mediano": "AOV (Mediana)",
        "AOV mediano ": "AOV (Mediana)",
        "gmv_user": "GMV (mediana)",
        "pedidos_user": "Pedidos (mediana)",
        "aov": "AOV (Mediana)",
    }

    median_flag = any(("median" in str(m).lower()) or ("mediano" in str(m).lower()) or ("mediana" in str(m).lower()) for m in metrics)

    fig, ax = plt.subplots(figsize=(8,5))
    x = np.arange(len(d["Grupo"]))
    width = 0.22

    for i, m in enumerate(metrics):
        lab = None
        if labels_map and m in labels_map:
            lab = labels_map[m]
        else:
            lab = median_default_map.get(m, m)
        ax.bar(x + (i - (len(list(metrics))-1)/2)*width, d[m].astype(float).values, width, label=lab)

    ax.set_xticks(x); ax.set_xticklabels(d["Grupo"])
    ax.legend()
    y_label = "Valor (mediana)" if median_flag else "Valor média/mediana"
    _fmt_axes(ax, y_label=y_label, title=title or "Comparação entre grupos")
    plt.tight_layout()
    _ensure_dir(outdir)
    outp = os.path.join(outdir, f"{fname}.png")
    plt.savefig(outp, dpi=160, bbox_inches="tight"); plt.close(fig)
    return outp
