from __future__ import annotations
import os
from typing import Iterable, Optional, List
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ---------- util ----------
def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def _fmt_axes(ax, y_label: str, title: Optional[str] = None, y_log: bool = False):
    ax.set_ylabel(y_label)
    if title: ax.set_title(title)
    if y_log:
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(plt.MaxNLocator(8))
    ax.grid(True, axis="y", alpha=0.25)

# ---------- tabelas ----------
def save_table_csv(df: pd.DataFrame, outdir: str, name: str) -> str:
    _ensure_dir(outdir)
    outp = os.path.join(outdir, f"{name}.csv")
    df.to_csv(outp, index=False)
    return outp

# ---------- gráficos de distribuição / box ----------
def to_pandas_safe(df):
    try:
        # Spark DataFrame
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
    df = to_pandas_safe(users_pdf_or_spark).copy()
    df = df[["is_target", metric_col]].dropna()
    df["is_target"] = df["is_target"].astype(int)

    # clipping leve contra outliers extremos (sem esconder o shape)
    s0 = pd.to_numeric(df[df["is_target"]==0][metric_col], errors="coerce").dropna()
    s1 = pd.to_numeric(df[df["is_target"]==1][metric_col], errors="coerce").dropna()
    if clip_p and 0 < clip_p < 0.5:
        def _clip(s):
            lo, hi = np.quantile(s, [clip_p, 1-clip_p])
            return s.clip(lo, hi)
        s0, s1 = _clip(s0), _clip(s1)

    fig, ax = plt.subplots(figsize=(8,4))
    ax.boxplot([s0.values, s1.values], showfliers=True, labels=["Controle", "Tratamento"])
    _fmt_axes(ax, y_label=metric_col, title=title or f"Boxplot • {metric_col}", y_log=y_log)
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
    ax.set_xlabel(metric_col); ax.set_ylabel("densidade")
    ax.set_title(title or f"Distribuição • {metric_col}")
    plt.tight_layout()
    _ensure_dir(outdir)
    outp = os.path.join(outdir, f"{fname or ('hist_'+metric_col)}.png")
    plt.savefig(outp, dpi=160, bbox_inches="tight"); plt.close(fig)
    return outp

# ---------- barras de médias/medianas ----------
def plot_group_bars(
    df_summary: pd.DataFrame,
    *,
    metrics: Iterable[str],
    labels_map: Optional[dict] = None,
    outdir: str,
    fname: str = "group_bars",
    title: Optional[str] = None
):
    d = df_summary.copy()
    d["Grupo"] = d["is_target"].map({0:"Controle",1:"Tratamento"}).astype(str)

    fig, ax = plt.subplots(figsize=(8,5))
    x = np.arange(len(d["Grupo"]))
    width = 0.22

    for i, m in enumerate(metrics):
        lab = labels_map.get(m, m) if labels_map else m
        ax.bar(x + (i - (len(list(metrics))-1)/2)*width, d[m].astype(float).values, width, label=lab)

    ax.set_xticks(x); ax.set_xticklabels(d["Grupo"])
    ax.legend()
    _fmt_axes(ax, y_label="Valor médio/robusto", title=title or "Comparação entre grupos")
    plt.tight_layout()
    _ensure_dir(outdir)
    outp = os.path.join(outdir, f"{fname}.png")
    plt.savefig(outp, dpi=160, bbox_inches="tight"); plt.close(fig)
    return outp
