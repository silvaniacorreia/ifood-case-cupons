from __future__ import annotations
import os, tarfile
from pathlib import Path
from typing import Tuple

import requests
from tqdm import tqdm
import yaml

def load_settings(path: str = "config/settings.yaml") -> dict:
    if not os.path.exists(path):
        path = "config/settings.example.yaml"
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def stream_download(url: str, dest: Path, chunk_size: int = 1024 * 1024) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with tqdm(total=total, unit="B", unit_scale=True, desc=dest.name) as pbar:
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

def extract_tar_gz(tar_path: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(out_dir)
    return out_dir

def remove_mac_artifacts(dirpath: Path) -> None:
    for p in dirpath.rglob("*"):
        if p.name.startswith("._") or p.name == ".DS_Store":
            try:
                p.unlink()
                print(f"[clean] removido {p.name}")
            except Exception:
                pass

def main():
    s = load_settings()
    raw_dir = Path(s["data"]["raw_dir"])
    sources = s.get("sources", {})
    if not sources:
        raise RuntimeError("Bloco 'sources' não encontrado no settings.yaml")

    raw_dir.mkdir(parents=True, exist_ok=True)

    # 1) Baixar todos os arquivos
    for key, meta in sources.items():
        url = meta["url"]
        filename = meta["filename"]
        dest = raw_dir / filename
        if dest.exists():
            print(f"[skip] {filename} já existe.")
        else:
            print(f"[down] {key}: {url}")
            stream_download(url, dest)

        # 2) Descompactar somente quando necessário
        if filename.endswith(".tar.gz"):
            out = raw_dir / "ab_test_ref_extracted"
            if any(p.suffix == ".csv" for p in out.rglob("*")):
                print(f"[skip] CSV de ab_test_ref já extraído em {out}")
            else:
                print(f"[extract] {filename} -> {out}")
                extract_tar_gz(dest, out)
                remove_mac_artifacts(out)

    print("Downloads finalizados. Para ler no ETL:")
    print(" - JSON/CSV .gz: Spark lê nativamente comprimidos (não precisa extrair).")
    print(" - TAR.GZ ab_test_ref: CSV já extraído em data/raw/ab_test_ref_extracted/")

if __name__ == "__main__":
    main()
