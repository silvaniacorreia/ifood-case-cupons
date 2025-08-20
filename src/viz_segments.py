import os
from typing import List, Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def to_pandas_spark(df_spark) -> pd.DataFrame:
    return df_spark.toPandas()

def save_table_csv(df: pd.DataFrame, outdir: str, name: str) -> str:
    ensure_dir(outdir)
    p = os.path.join(outdir, f"{name}.csv")
    df.to_csv(p, index=False)
    return p

def plot_bars_by_segment(
    df: pd.DataFrame,
    segment_col: str,
    value_cols: List[str],
    group_col: str = "is_target",
    title: Optional[str] = None,
    outdir: Optional[str] = None,
    fname: Optional[str] = None,
):
    segs = sorted(df[segment_col].astype(str).unique().tolist())
    groups = sorted(df[group_col].astype(int).unique().tolist())  # [0, 1]

    for metric in value_cols:
        fig, ax = plt.subplots(figsize=(10, 5))
        x = np.arange(len(segs))
        width = 0.35
        for gi, g in enumerate(groups):
            vals = []
            for s in segs:
                v = df[(df[segment_col].astype(str) == str(s)) & (df[group_col] == g)][metric]
                vals.append(float(v.values[0]) if len(v) else np.nan)
            ax.bar(x + (gi-0.5)*width, vals, width, label=f"{group_col}={g}")
        ax.set_xticks(x)
        ax.set_xticklabels(segs, rotation=30, ha="right")
        ax.set_ylabel(metric)
        if title:
            ax.set_title(f"{title} — {metric}")
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
