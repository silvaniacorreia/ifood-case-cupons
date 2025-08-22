from __future__ import annotations
import os, tarfile
from pathlib import Path
from typing import Tuple

import requests
from tqdm import tqdm
import yaml
import gzip

def shard_ndjson_gz(src_gz: Path, out_dir: Path, target_mb: int = 100) -> None:
    """
    Converte um único NDJSON .gz em vários arquivos JSON ('part-xxxxx.json') com tamanho aproximado.

    Parâmetros:
        src_gz (Path): Caminho para o arquivo NDJSON comprimido (.gz).
        out_dir (Path): Diretório de saída onde as partes serão escritas.
        target_mb (int): Tamanho alvo em megabytes por parte (aproximado).

    Retorna:
        None

    Observações:
        - Idempotente: se partes já existirem no `out_dir`, não refaz.
        - Não altera o conteúdo; apenas divide por linhas.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    if any(out_dir.glob("part-*.json")):
        print(f"[shard] Já existem partes em {out_dir}, skip.")
        return

    lines_per_part = max(200_000, int(target_mb * 10_000))
    part_idx, line_idx = 0, 0
    fout = open(out_dir / f"part-{part_idx:05d}.json", "w", encoding="utf-8")

    print(f"[shard] Iniciando shard de {src_gz} para {out_dir} (target ~{target_mb}MB/parte)")
    with gzip.open(src_gz, "rt", encoding="utf-8") as fin:
        for line in fin:
            fout.write(line)
            line_idx += 1
            if line_idx >= lines_per_part:
                fout.close()
                part_idx += 1
                line_idx = 0
                fout = open(out_dir / f"part-{part_idx:05d}.json", "w", encoding="utf-8")
    fout.close()
    print(f"[shard] Geradas {part_idx+1} partes.")

def load_settings(path: str = "config/settings.yaml") -> dict:
    """
    Carrega as configurações do arquivo YAML.

    Parâmetros:
        path (str): Caminho para o arquivo de configurações YAML.

    Retorna:
        dict: Conteúdo do YAML convertido para dicionário Python.
    """
    if not os.path.exists(path):
        path = "config/settings.example.yaml"
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def stream_download(url: str, dest: Path, chunk_size: int = 1024 * 1024) -> None:
    """
    Faz o download de um arquivo de forma eficiente, salvando em `dest`.

    Parâmetros:
        url (str): URL a ser baixada.
        dest (Path): Caminho do arquivo de destino.
        chunk_size (int): Tamanho dos blocos lidos por iteração.

    Retorna:
        None
    """
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
    """
    Extrai um arquivo TAR.GZ para um diretório de saída.

    Parâmetros:
        tar_path (Path): Caminho para o arquivo tar.gz.
        out_dir (Path): Diretório de saída para os arquivos extraídos.

    Retorna:
        Path: Diretório de saída onde os arquivos foram extraídos.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(out_dir)
    return out_dir

def remove_mac_artifacts(dirpath: Path) -> None:
    """
    Remove artefatos do macOS (arquivos que começam com "._" ou ".DS_Store").
    """
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

    for key, meta in sources.items():
        url = meta["url"]
        filename = meta["filename"]
        dest = raw_dir / filename
        if dest.exists():
            print(f"[skip] {filename} já existe.")
        else:
            print(f"[down] {key}: {url}")
            stream_download(url, dest)

        if filename.endswith(".tar.gz"):
            out = raw_dir / "ab_test_ref_extracted"
            if any(p.suffix == ".csv" for p in out.rglob("*")):
                print(f"[skip] CSV de ab_test_ref já extraído em {out}")
            else:
                print(f"[extract] {filename} -> {out}")
                extract_tar_gz(dest, out)
                remove_mac_artifacts(out)

    orders_gz = raw_dir / "order.json.gz"
    sharded_dir = raw_dir / "orders_sharded"
    if orders_gz.exists():
        shard_ndjson_gz(orders_gz, sharded_dir, target_mb=100)

    print("Downloads finalizados. Para ler no ETL:")
    print(" - JSON/CSV .gz: Spark lê nativamente comprimidos (não precisa extrair).")
    print(" - TAR.GZ ab_test_ref: CSV já extraído em data/raw/ab_test_ref_extracted/")

if __name__ == "__main__":
    main()
