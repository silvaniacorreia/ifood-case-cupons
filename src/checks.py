from __future__ import annotations
import os, gzip, json, tarfile
from pathlib import Path
from typing import List, Dict, Any

REQUIRED_FILES = {
    "orders":      "order.json.gz",
    "consumers":   "consumer.csv.gz",
    "restaurants": "restaurant.csv.gz",
    "ab_tar":      "ab_test_ref.tar.gz",
}

def _exists_and_size(p: Path) -> tuple[bool, int]:
    return p.exists(), (p.stat().st_size if p.exists() else 0)

def validate_gzip(p: Path) -> bool:
    try:
        with gzip.open(p, "rb") as g:
            g.peek(1)
        return True
    except Exception:
        return False

def preview_tar_members(p: Path, limit: int = 5) -> list[str]:
    names = []
    with tarfile.open(p, "r:gz") as tar:
        for m in tar.getmembers():
            if m.isfile():
                names.append(m.name)
            if len(names) >= limit:
                break
    return names

def list_valid_ab_csvs(ab_dir: Path, min_bytes: int = 1024) -> list[Path]:
    if not ab_dir.exists():
        return []
    out = []
    for p in ab_dir.rglob("*.csv"):
        if p.name.startswith("._") or p.name.startswith("."):
            continue  
        try:
            if p.stat().st_size >= min_bytes:
                out.append(p)
        except Exception:
            pass
    out.sort(key=lambda x: x.stat().st_size if x.exists() else 0, reverse=True)
    return out

def sniff_orders_format(orders_gz: Path) -> str:
    """
    Retorna:
      - "json_array" se o arquivo começa com '['
      - "ndjson_or_objects" caso contrário (1 JSON por linha ou objetos por linha)
      - "missing" se o arquivo não existir
      - "error:<mensagem>" em caso de erro
    """
    if not orders_gz.exists():
        return "missing"
    try:
        with gzip.open(orders_gz, "rt", encoding="utf-8") as f:
            first_non_ws = None
            while True:
                ch = f.read(1)
                if not ch:
                    break
                if not ch.isspace():
                    first_non_ws = ch
                    break
        if first_non_ws == "[":
            return "json_array"
        return "ndjson_or_objects"
    except Exception as e:
        return f"error:{e}"

def preflight(raw_dir: str | Path, strict: bool = True) -> Dict[str, Any]:
    """
    Executa checagens de presença/tamanho/integração dos arquivos.
    strict=True levanta RuntimeError em problemas críticos.
    """
    raw = Path(raw_dir)
    report: Dict[str, Any] = {"raw_dir": str(raw), "files": {}, "ab_csv_candidates": []}

    # 1) presença / tamanho / integridade gzip
    for key, fname in REQUIRED_FILES.items():
        p = raw / fname
        exists, size = _exists_and_size(p)
        item = {"path": str(p), "exists": exists, "size_bytes": size}
        if fname.endswith(".gz") and exists:
            item["gzip_ok"] = validate_gzip(p)
        if fname.endswith(".tar.gz") and exists:
            try:
                item["tar_preview"] = preview_tar_members(p, 5)
                item["tar_ok"] = True
            except Exception as e:
                item["tar_ok"] = False
                item["tar_error"] = str(e)
        report["files"][key] = item

    # 2) CSVs do A/B válidos
    ab_dir = raw / "ab_test_ref_extracted"
    valids = list_valid_ab_csvs(ab_dir)
    report["ab_csv_candidates"] = [str(p) for p in valids]

    # 3) formato do orders
    orders_fmt = sniff_orders_format(raw / REQUIRED_FILES["orders"])
    report["orders_format_guess"] = orders_fmt

    # 4) decisões de rigor
    criticals = []
    for k in ["orders", "consumers", "restaurants", "ab_tar"]:
        f = report["files"].get(k, {})
        if not f.get("exists"):
            criticals.append(f"arquivo ausente: {k} -> {f.get('path')}")
    if report["files"].get("orders", {}).get("exists") and not report["files"]["orders"].get("gzip_ok", False):
        criticals.append("orders gzip inválido")
    if report["files"].get("ab_tar", {}).get("exists") and not report["files"]["ab_tar"].get("tar_ok", False):
        criticals.append("ab_test_ref.tar.gz inválido")
    if not report["ab_csv_candidates"]:
        criticals.append("nenhum CSV válido encontrado em data/raw/ab_test_ref_extracted/")

    if strict and criticals:
        msg = "Preflight falhou:\n - " + "\n - ".join(criticals)
        raise RuntimeError(msg)

    return report
