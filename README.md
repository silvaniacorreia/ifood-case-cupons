# iFood Case – Cupons

Case para vaga de Analista de Dados no iFood, com o objetivo de analisar um **teste A/B** de uma estratégia de **cupons** com foco em **retenção** e crescimento.

**O case solicita que a entrega contemple, na ordem, os itens abaixo:**

1) **Teste A/B (Campanha de Cupons)**
   - **(a)** Definir **indicadores/métricas de sucesso** da campanha e analisar se houve **impacto estatisticamente significativo** no período avaliado.
   - **(b)** Realizar **análise de viabilidade financeira** (ex.: ROI / payback), **explicitando as premissas** adotadas.
   - **(c)** Recomendar **oportunidades de melhoria** na ação e **desenhar um novo teste A/B** para validar as hipóteses (desenho experimental, métricas e guardrails).

2) **Segmentação de Usuários**
   - **(a)** Estabelecer **critérios/ regras** para cada **segmento** (ex.: RFM), **explicando o racional** da construção.
   - **(b)** Analisar os **resultados do A/B por segmento** e **propor ações específicas** para cada público.

3) **Recomendações e Próximos Passos**
   - Sugerir **próximos passos** com **previsão de impacto** (financeiro ou não), defendendo as recomendações para as **lideranças de Negócio**.
   - Incluir **melhorias de processo/teste** e **estratégias diferenciadas por segmento**.  
   - Quando necessário, **adotar premissas** e **deixá-las claras** no material.

<a href="https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/00_setup_and_checks.ipynb" target="_blank" rel="noopener">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab">
</a>

> **O que este repositório entrega agora:** bootstrap reprodutível (local e Colab), **download programático** + extração, inicialização do **PySpark** e **smoke test**. As etapas de ETL, RFM, A/B e ROI estão sendo adicionadas na sequência.

> **Como executar (avaliadores):** abra e rode **um notebook por vez na ordem** → `00_setup_and_checks` → `01_etl_pyspark` → `02_abtest_and_segments` → `03_financial_roi`.

---

## ✅ Status (parcial)

- **Download programático** e extração (`scripts/download_data.py`) lendo as fontes de `config/settings.yaml`.
- **Bootstrap Colab/Local** em `notebooks/00_setup_and_checks.ipynb`:
  - Colab: clona o repositório, instala dependências, baixa e prepara os dados.
  - Local: verifica paths e roda o mesmo script de download.
- **Utilitários** (`src/utils.py`): `load_settings`, `get_spark`, `set_seeds`, `stop_spark`.
- **Smoke test** do Spark no `00_setup_and_checks.ipynb`.

> Em construção: `src/etl.py`, `src/segments.py` (RFM), `src/abtest.py`, `src/finance.py` + notebooks `01_`, `02_`, `03_`.

---

## 🧩 Estrutura do repositório

```markdown
ifood-case-cupons/
├─ README.md
├─ requirements.txt
├─ Makefile
├─ notebooks/
│  ├─ 00_setup_and_checks.ipynb      # bootstrap + download programático + smoke do Spark
│  ├─ 01_etl_pyspark.ipynb           # (em construção)
│  ├─ 02_abtest_and_segments.ipynb   # (em construção)
│  └─ 03_financial_roi.ipynb         # (em construção)
├─ src/
│  ├─ __init__.py
│  └─ utils.py                       # settings + spark + seeds
├─ scripts/
│  └─ download_data.py               # baixa .gz/.tar.gz e extrai o .tar.gz
├─ config/
│  ├─ settings.example.yaml
│  └─ settings.yaml                  # contém o bloco `sources:` com as URLs
├─ data/
│  ├─ raw/                           # arquivos baixados (spark lê .gz direto)
│  └─ processed/                     # saídas intermediárias (parquet)
└─ report/
```

## ⚙️ Configuração

Arquivo: `config/settings.yaml`

Parâmetros do projeto (seed, spark, caminhos) e bloco `sources` com as URLs e nomes dos 4 arquivos do case:

```yaml
sources:
  orders:      { url: ".../order.json.gz",      filename: "order.json.gz" }
  consumers:   { url: ".../consumer.csv.gz",    filename: "consumer.csv.gz" }
  restaurants: { url: ".../restaurant.csv.gz",  filename: "restaurant.csv.gz" }
  ab_test_ref: { url: ".../ab_test_ref.tar.gz", filename: "ab_test_ref.tar.gz" }
```

O `scripts/download_data.py` lê essas entradas, baixa os arquivos para `data/raw/` e extrai o `ab_test_ref.tar.gz` para `data/raw/ab_test_ref_extracted/`. Os `.gz` (JSON/CSV) não precisam ser descompactados para o Spark.

---

## ▶️ Como executar no Colab (avaliadores)

Clique no badge acima (“Open in Colab”).

No Colab, vá em `Runtime → Run all (Executar tudo)`.

A primeira célula:
- clona este repositório para `/content/ifood-case-cupons`,
- instala dependências do `requirements.txt`,
- roda `scripts/download_data.py` (download programático + extração).

A segunda célula faz o smoke test do Spark (deve imprimir “Spark OK: x.y.z” e exibir uma tabela 0..4).

Após isso, os dados estarão em:

```
/content/ifood-case-cupons/data/raw/
/content/ifood-case-cupons/data/raw/ab_test_ref_extracted/
```

Quando os notebooks seguintes forem concluídos, basta abrir `01_`, `02_` e `03_` e executar normalmente.

---

## 💻 Como executar localmente (desenvolvedores)

Pré-requisitos: Python 3.10+, JDK 11+ (para Spark).

```bash
# 1) Ambiente
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m ipykernel install --user --name ifood-case

# 2) Download programático
python scripts/download_data.py

# 3) Jupyter
jupyter notebook   # ou jupyter lab
# Abra notebooks/00_setup_and_checks.ipynb e rode "Run all"
```

Se aparecer erro de Java local, instale um JDK 11+ e confira `java -version` no terminal.

---

## 🧪 Teste rápido (smoke do Spark)

No `notebooks/00_setup_and_checks.ipynb` já existe uma célula que faz:

```python
from src.utils import load_settings, set_seeds, get_spark, stop_spark
s = load_settings()
set_seeds(s.runtime.seed)
spark = get_spark(app_name=s.runtime.spark.app_name,
                  shuffle_partitions=s.runtime.spark.shuffle_partitions)
print("Spark OK:", spark.version)
spark.range(5).show()
stop_spark(spark)
```

---

## 🔜 Roadmap (próximas entregas)

- `src/etl.py` + `notebooks/01_etl_pyspark.ipynb`
- `src/segments.py` (RFM) + `notebooks/02_abtest_and_segments.ipynb`
- `src/abtest.py` (A/B com CUPED) + `src/finance.py` (ROI) + `notebooks/03_financial_roi.ipynb`

