# iFood Case – Cupons

Repositório para o case de **Analista de Dados** do iFood. Objetivo: analisar um **teste A/B** de uma estratégia de **cupons** com foco em **retenção** e crescimento, seguindo a ordem e escopo pedidos no case.

---

## ✨ Escopo do Case (ordem de entrega)

1) **Teste A/B (Campanha de Cupons)**
   - **(a)** Definir **métricas de sucesso** e analisar **impacto estatisticamente significativo** no período avaliado.
   - **(b)** Realizar **análise de viabilidade financeira** (ex.: ROI / payback), deixando **premissas explícitas**.
   - **(c)** Recomendar **oportunidades de melhoria** e **desenhar um novo A/B** (desenho experimental, métricas e guardrails).

2) **Segmentação de Usuários**
   - **(a)** Estabelecer **critérios/regras** dos segmentos (neste projeto: **RFM** como baseline), explicando o racional.
   - **(b)** Analisar **resultados do A/B por segmento** e **propor ações** específicas.

3) **Recomendações e Próximos Passos**
   - Plano com **previsão de impacto** (financeiro ou não) para as **Lideranças de Negócio**.
   - **Melhorias de processo/teste** e **estratégias por segmento**.
   - **Premissas documentadas** quando necessário.

---

## 🚀 Status (até agora)

- **Bootstrap reprodutível** (local e Colab) no `00_setup_and_checks.ipynb`:
  - Colab: clona o repo, instala deps, baixa e prepara os dados (**download programático**).
  - Local: garante paths e roda o mesmo script de download.
- **Download programático** + extração: `scripts/download_data.py`
  - Baixa os 4 insumos de `config/settings.yaml`.
  - **Extrai** apenas o `ab_test_ref.tar.gz` (o Spark lê `.gz` diretamente).
  - **Ignora artefatos** do macOS `._*.csv` e `.DS_Store` ao localizar o CSV do A/B.
- **ETL base** (`01_etl_pyspark.ipynb` + `src/etl.py`):
  - **Orders**: tipagem, normalização de **timezone** (UTC canônico; BRT para “dia de negócio”), remoção/hasheamento de **PII**, validações (duplicados, valores, geolocalização), escolha do **timestamp do evento** (agendado vs criado).
  - **Consumers / Restaurants**: tipagem, normalização, remoção/hasheamento de PII e renomeações para joins seguros.
  - **Janela do experimento**: parametrizada via `settings.yaml` **ou** **inferida automaticamente** dos dados quando não definida.
  - Saídas: `data/processed/orders_silver.parquet` (fato por pedido) e `data/processed/users_silver.parquet` (R/F/M por usuário).
- **Checks de qualidade** no final do notebook (nulos, unicidade, janelas, split A/B, distribuições).

> Próximas etapas (em construção): `02_abtest_core.ipynb` (métricas + testes), `03_financial_viability.ipynb` (ROI) e `04_segments_ab_by_segment.ipynb` (RFM + A/B por segmento).

---

## 📁 Estrutura do repositório

```
ifood-case-cupons/
├─ README.md
├─ requirements.txt
├─ notebooks/
│  ├─ 00_setup_and_checks.ipynb      # bootstrap + download programático + smoke Spark
│  ├─ 01_etl_pyspark.ipynb           # ETL e silvers (orders/users)
│  ├─ 02_abtest_core.ipynb           # (em construção) A/B métricas + significância
│  ├─ 03_financial_viability.ipynb   # (em construção) ROI e sensibilidade
│  └─ 04_segments_ab_by_segment.ipynb# (em construção) RFM + leitura do A/B por segmento
├─ src/
│  ├─ __init__.py
│  ├─ utils.py                       # settings + spark + seeds
│  └─ etl.py                         # ingestão + limpeza + joins + silvers
├─ scripts/
│  └─ download_data.py               # baixa .gz/.tar.gz; extrai tar e limpa artefatos
├─ config/
│  ├─ settings.example.yaml
│  └─ settings.yaml                  # fontes + parâmetros (ver abaixo)
├─ data/
│  ├─ raw/                           # arquivos baixados
│  └─ processed/                     # parquet gerados pelo ETL
└─ report/                           # relatório final (PDF)
```

---

## 🔗 Acesso rápido aos Notebooks (abre em nova aba)

- **Setup & Checks**  
  <a href="https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/00_setup_and_checks.ipynb" target="_blank" rel="noopener">Abrir no Colab</a> ·
  <a href="https://github.com/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/00_setup_and_checks.ipynb" target="_blank" rel="noopener">Ver no GitHub</a>

- **ETL (PySpark)**  
  <a href="https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/01_etl_pyspark.ipynb" target="_blank" rel="noopener">Abrir no Colab</a> ·
  <a href="https://github.com/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/01_etl_pyspark.ipynb" target="_blank" rel="noopener">Ver no GitHub</a>

*(Os notebooks 02/03/04 aparecerão aqui quando forem versionados.)*

---

## ⚙️ Configuração & Execução

### `config/settings.yaml` (exemplo mínimo)
```yaml
runtime:
  seed: 42
  spark:
    app_name: "ifood-case-cupons"
    shuffle_partitions: 64

data:
  raw_dir: "data/raw"
  processed_dir: "data/processed"

sources:
  orders:      { url: "https://.../order.json.gz",      filename: "order.json.gz" }
  consumers:   { url: "https://.../consumer.csv.gz",    filename: "consumer.csv.gz" }
  restaurants: { url: "https://.../restaurant.csv.gz",  filename: "restaurant.csv.gz" }
  ab_test_ref: { url: "https://.../ab_test_ref.tar.gz", filename: "ab_test_ref.tar.gz" }

analysis:
  # Se NÃO definir a janela, o ETL **infere automaticamente** [min_data, max_data+1d) em UTC
  # experiment_window:
  #   start: "YYYY-MM-DD"
  #   end:   "YYYY-MM-DD"   # exclusivo
  business_tz: "America/Sao_Paulo"
  auto_infer_window: true
  treat_is_target_null_as_control: false
  winsorize: 0.02
  use_cuped: true
```

### Execução no **Colab** (avaliadores)
1. Abra **cada notebook** pelos links acima (o Colab abre em nova aba).  
2. Em `00_setup_and_checks`: **Runtime → Run all**. A primeira célula clona o repo, instala deps e roda o **download programático**; a segunda faz o **smoke** do Spark.  
3. Em `01_etl_pyspark`: **Run all** para gerar `orders_silver.parquet` e `users_silver.parquet` em `data/processed/`.


### Execução **local** (desenvolvedores)
Pré-requisitos: **Python 3.10+** e **JDK 11+** (Spark).

```bash
# 1) Ambiente
python -m venv .venv && . .venv/Scripts/activate  # Windows PowerShell
# ou: source .venv/bin/activate                    # macOS/Linux
pip install -r requirements.txt
python -m ipykernel install --user --name ifood-case

# 2) Download programático
python scripts/download_data.py

# 3) Jupyter
jupyter notebook  # ou jupyter lab
# Abra 00_setup_and_checks.ipynb (Run all) e depois 01_etl_pyspark.ipynb (Run all)
```

> **Windows (importante):** para **escrever Parquet** com Spark, configure o **winutils.exe** (Hadoop):
> - Descubra a versão do Hadoop usada pelo Spark:
>   ```python
>   print("Spark:", spark.version)
>   hadoop_ver = spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()
>   print("Hadoop:", hadoop_ver)
>   ```
> - Instale o `winutils.exe` da **mesma linha de versão** (por ex., `C:\hadoop\hadoop-3.3.4\bin\winutils.exe`) e exporte:
>   ```powershell
>   $env:HADOOP_HOME="C:\hadoop\hadoop-3.3.4"
>   $env:PATH="$env:HADOOP_HOMEin;$env:PATH"
>   ```
> Reinicie o Jupyter após configurar.

---

## 🧱 Premissas & Decisões de Projeto

- **Timezone**: timestamps normalizados para **UTC** (`event_ts_utc`); para relatórios diários, usamos **BRT** (`event_date_brt`).  
- **Evento do pedido**: se `order_scheduled==true` **e** `order_scheduled_date` existir, usamos a data agendada; caso contrário, a data de criação do pedido.  
- **Janela do experimento**: se `analysis.experiment_window` estiver **ausente**, o ETL **infere** automaticamente `[min(data), max(data)+1d)` em UTC e aplica o filtro.  
- **A/B sem marcação**: `treat_is_target_null_as_control=false` por padrão (linhas sem `is_target` são excluídas, para evitar viés).  
- **PII**: `cpf` e telefone são **hasheados**; nome e endereço são **removidos** nas camadas analíticas.  
- **Leitura do A/B (CSV dentro de tar.gz)**: ignoramos arquivos “fantasma” do macOS (`._*.csv`, `.DS_Store`) e escolhemos o **maior CSV válido**.  
- **Outliers**: estatísticas de cauda são inspecionadas no ETL; a decisão de **winsorizar** (ex.: 1–99%) será aplicada no notebook do A/B.  
- **Reprodutibilidade**: o **download é programático** e o pipeline é executável **no Colab** sem setup manual.

---

## 🧪 Checks rápidos no ETL (no final do `01_etl_pyspark`)

- Unicidade: `order_id` e `customer_id` distintos nas silvers.  
- Nulos por coluna (especial atenção a `event_ts_utc`, `order_total_amount`, `is_target`).  
- Faixa de datas (UTC) e contagem diária (BRT) por grupo (`is_target`).  
- Split do A/B (equilíbrio entre controle e tratamento).  
- Resumo/quantis de `order_total_amount` e inspeção de outliers.

---

## 🗺️ Roadmap (próximas entregas)

- `02_abtest_core.ipynb`: métricas por usuário (GMV/U, Pedidos/U, Conversão, AOV), **Welch t-test** e **z-test de proporções**; (opcional) **CUPED**.
- `03_financial_viability.ipynb`: **ROI** e análise de **sensibilidade** (take rate, custo do cupom, cobertura).
- `04_segments_ab_by_segment.ipynb`: **RFM** + leitura do **uplift por segmento** e recomendações direcionadas.

---

## 📄 Licença & Privacidade

- Dados **não são versionados**. O script `scripts/download_data.py` baixa os insumos a partir das URLs configuradas.  
- PII é protegida nas **camadas analíticas** (hash/removidos).  
- Repositório preparado para execução em **ambiente gerenciado** (Colab) e **local** (com as observações de Windows acima).
