# Case iFood: Teste A/B Estratégia de Cupons

Repositório do case para **Analista de Dados** no iFood. Objetivo: analisar um **teste A/B** de cupons com foco em **retenção** e crescimento.

> Execução **100% no Google Colab** para máxima reprodutibilidade (sem dependências locais).

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/pipeline_analise_completa.ipynb)

---

## 🧭 Escopo da análise

1) **A/B de cupons**  
   (a) Definir **métricas de sucesso** e checar **significância** no período;  
   (b) **Viabilidade financeira** (ROI/payback) com **premissas explícitas**;  
   (c) Recomendar **melhorias** e desenhar um **novo A/B** (métricas/guardrails).

2) **Segmentação de usuários**  
   (a) **RFM** como baseline (critérios e racional claros);  
   (b) Ler o **efeito do A/B por segmento** e propor **ações por público**.

3) **Próximos passos**  
   Estimativa de **impacto** (financeiro ou não) e sugestões de **processo/teste**.

---

## 🗂️ Estrutura do repositório

```
ifood-case-cupons/
├─ README.md
├─ requirements.txt
├─ notebooks/
│  ├─ pipeline_analise_completa.ipynb # notebook principal (orquestração da análise)
├─ src/
│  ├─ __init__.py
│  ├─ utils.py                       # settings + spark + seeds
│  ├─ etl.py                         # ingestão + limpeza + joins + silvers
│  └─ analysis_ab.py                 # funções de análise A/B
├─ scripts/
│  └─ download_data.py               # baixa .gz/.tar.gz; extrai tar e limpa artefatos
├─ config/
│  └─ settings.yaml                  # fontes + parâmetros (ver abaixo)
├─ data/
│  ├─ raw/                           # arquivos baixados
│  └─ processed/                     # parquet gerados pelo ETL
└─ report/                           # relatório final (PDF)
```

---

## 🧱 Arquitetura & otimizações

### ETL otimizado
1. **Janela de análise antes do join (robusta a outliers)**  
   A janela é **inferida automaticamente pelos quantis 1–99% de `event_ts_utc`** e aplicada **antes** dos joins.  
   **Impacto:** reduz shuffle, memória e custo de join sem ser “puxada” por timestamps anômalos.

2. **Projeção mínima de colunas**  
   Selecionamos apenas as colunas necessárias antes dos joins.  
   **Impacto:** menos shuffle/memória, joins e writes mais rápidos.

3. **Reparticionamento por chave de join**  
   `orders` é reparticionado por `customer_id` usando `spark.sql.shuffle.partitions`.  
   **Impacto:** melhor balanceamento no shuffle durante o join.

4. **Spark-first & amostragem**
   • Métricas descritivas e robustas (medianas, p95, taxa de heavy users) são calculadas no Spark e só então convertidas para pandas quando necessário (tabelas pequenas).
   • Amostragem opcional para testes/plots: `collect_user_level_for_tests(sample_frac=...)` amostra no Spark antes do `toPandas()`, reduzindo tempo e memória (use 0.20–0.30 como referência).
   • Arrow ativado quando disponível, acelerando `toPandas()` de agregados pequenos. (Habilitado via `spark.sql.execution.arrow.pyspark.enabled=true`).
   • Reuso de shards do `order.json.gz`: `scripts/download_data.py` grava `data/raw/orders_sharded/` e o ETL lê diretamente desses shards em execuções seguintes.

4. **Broadcast de dimensão pequena**  
   `restaurants` é broadcast para habilitar broadcast-hash join.  
   **Impacto:** elimina shuffle dessa dimensão e acelera o join.

5. **Controles de verbosidade e cache**  
   `verbose=False` por padrão (evita `count()`/`show()` desnecessários) e `cache_intermediates=False` (reduz risco de OOM).

6. **Escrita em Parquet opcional e leve**  
   Persistência é **opcional** (desligada por padrão). Ao salvar, usamos `partitionBy(event_date_brt)` e **coalesce** para evitar explosão de arquivos.

### Configurações do Spark para Colab
- **AQE ligado:** `spark.sql.adaptive.enabled=true`
- **Serializer Kryo:** menor overhead de serialização
- **Timezone fixa (UTC):** conversões consistentes
- **`spark.sql.shuffle.partitions`:** 32 (definido por testes de benchmark) via `config/settings.yaml`

**Exemplo de `runtime.spark.conf` no Colab:**
```yaml
runtime:
  spark:
    app_name: "ifood-case-cupons"
    driver_memory: "12g"
    shuffle_partitions: 128
    conf:
      spark.master: "local[*]"
      spark.sql.adaptive.enabled: "true"
      spark.sql.adaptive.coalescePartitions.enabled: "true"
      spark.sql.files.maxPartitionBytes: "64m"
      spark.serializer: "org.apache.spark.serializer.KryoSerializer"
      spark.memory.fraction: "0.6"
      spark.sql.autoBroadcastJoinThreshold: "50MB"
```

### Leituras e conformizações robustas
- **Reader resiliente de orders:** detecta NDJSON ou JSON array e tem fallback com gzip/json.
- **Sanitização de PII e tipagem:** normalizamos timestamps, lat/long, flags, calculamos `basket_size` de forma segura, e removemos/hasheamos PII.
- **Checagens prévias (preflight):** verificamos tamanhos, compressão e candidatos do `ab_test_ref` antes de acionar o Spark; falha cedo em caso de problema.

### Enriquecimentos para análise (camada “silver”)
- **`origin_platform` nulo → `"unknown"`** (evita perdas em cortes por canal).
- **Campos de consumidor faltantes**: versões limpas para segmentação (ex.: `language_clean = coalesce(language, 'unknown')`), mantendo os originais para auditoria.
- **Atributos de restaurante imputados (colunas paralelas)**  
  - `minimum_order_value_imputed`: mediana por (`merchant_city`, `price_range`) com fallback por `price_range`.  
  - `delivery_time_imputed`: mediana por `price_range`.  
  As colunas **originais são preservadas**; as versões imputadas são usadas apenas para diagnóstico/controle (ex.: balance check/CUPED).

---

## ▶️ Como executar

### Execução no Colab

1. Abra o notebook **no Colab**:  
   [**pipeline_analise_completa.ipynb**](https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/pipeline_analise_completa.ipynb)

2. **Runtime → Run all**. A primeira célula:
   - clona/atualiza o repositório;
   - instala as dependências de `requirements.txt`;
   - roda o **download programático** (`scripts/download_data.py`);

3. O notebook então executa:
   - **Pré-flight** (fail-fast) dos arquivos baixados;  
   - **Profiling** dos 4 dataframes brutos;  
   - **ETL** completo com normalização de timezone/PII e joins;  
   - **A/B**, **viabilidade** e **RFM** (em sequência).

> **Tempo de execução:** a leitura de `orders` (~1.6 GB gz) pode levar alguns minutos no Colab (gzip não é splittable). Depois da leitura, o ETL **reparticiona por `customer_id`** para paralelizar os joins.

---

## ⚙️ Configurações importantes (`config/settings.yaml`)

| Caminho                         | Descrição |
|--------------------------------|-----------|
| `data.raw_dir`                 | Pasta dos brutos (default: `data/raw`) |
| `data.processed_dir`           | Pasta dos parquet (se habilitar salvar) |
| `runtime.spark.driver_memory`  | Memória do driver no Colab (`12g`) |
| `analysis.business_tz`         | TZ de negócio (default `America/Sao_Paulo`) |
| `analysis.experiment_window`   | **auto-inferida** |
| `analysis.auto_infer_window`   | `true`/`false` — ativa a inferência de janela |
| `analysis.treat_is_target_null_as_control` | `false` por padrão (linhas sem grupo são excluídas) |
| `analysis.winsorize`/`use_cuped` | Parâmetros para A/B (aplicados nas análises) |
| `runtime.spark.conf.*`        | Confs avançadas do Spark (AQE, Kryo, partições, broadcast etc.) |

---

## 🧱 Decisões técnicas & otimizações de desempenho

### Formato e leitura dos dados
- **Formato de `orders`**: detectado como **NDJSON**; leitura com `spark.read.json(...)`.  
  - Como o arquivo é grande e gzip não é splittable, a leitura inicial roda em 1 task; após ler, fazemos:  
    **`o = o.repartition(spark.sql.shuffle.partitions, 'customer_id')`** para **distribuir** o trabalho nos joins.
  - **Broadcast** de dimensões: `restaurants` sempre (pequena) e `abmap` se `count ≤ 2M`.

### Configuração do Spark
- **driver_memory**: ajustado para **12g** para evitar problemas de memória.

### Persistência e formato de saída
- **Persistência estratégica**: usamos `.cache()` para evitar recomputação em etapas subsequentes.  
- **Parquet**: após o ETL, os DataFrames processados podem ser salvos em Parquet para acelerar leituras futuras.

---

## 🧰 O notebook como orquestrador técnico

O notebook **pipeline_analise_completa.ipynb** funciona como um **orquestrador técnico** das tarefas de análise, integrando os diferentes módulos do repositório:

1. **Configuração inicial**:
   - Clona o repositório e instala as dependências.
   - Faz o download programático dos dados brutos.

2. **Configuração do Spark**:
   - Lê as configurações do arquivo `settings.yaml` e inicializa o Spark com parâmetros ajustados para o Colab.

3. **Execução das tarefas**:
   - **Pré-flight**: validação dos arquivos brutos.
   - **ETL**: utiliza funções do módulo `src/etl.py` para ingestão, conformação e geração dos DataFrames "silver".
   - **Análises**: executa métricas de A/B, ROI e segmentação, utilizando funções específicas dos módulos `src/utils.py` e `src/checks.py`.

4. **Orquestração**:
   - O notebook organiza a execução das etapas de forma sequencial, garantindo que cada tarefa seja realizada com base nos resultados da anterior.

---

## ✅ Pré-flight & Profiling (o que verificar ao apresentar)

- **Pré-flight (fail-fast)**: arquivos existem, tamanhos coerentes, gzip/tar íntegros, **CSVs válidos** do A/B encontrados.  
- **Profiling (pós-leitura)**:  
  - `orders/consumers/restaurants/abmap`: **schema** e **amostras**;  
  - faixa de datas (min/max) e **nulos em campos-chave**;  
  - distribuição de **grupo** (controle vs tratamento) no A/B.

Esses passos mostram maturidade de engenharia e evitam “rodar com tabelas vazias”.

---

## 📦 Saídas do ETL (em memória)

- `orders_silver`: fato por pedido (UTC/BRT, valores, flags, atributos do consumidor e do restaurante, `is_target`).  
- `users_silver`: R/F/M por usuário + `is_target`, com `recency` calculado a partir do último `event_ts_utc`.

---

## 🗂️ Resumo dos Módulos

### `src/etl.py`
Funções de:
- Ingestão de dados brutos (JSON, CSV)
- Limpeza e conformidade de dados
- Joins e agregações
- Normalização de timestamps

### `src/utils.py`
Utilitários para:
- Configuração de SparkSession
- Carregamento de configurações (YAML)
- Controle de seeds e benchmarking para shuffle

### `src/checks.py`
Funções de validação e pré-checagem:
- Validação de arquivos gzip e tar
- Listagem de CSVs válidos para testes A/B
- Checagem de formatos de arquivos de pedidos

### `src/analysis_ab.py`
Funções de:
- Métricas A/B por grupo (Spark) e robustas (Spark, `percentile_approx`); coleta para testes em pandas com amostragem opcional.

## Etapas da Análise

### 1. Preparação e Limpeza de Dados

* Leitura de shards e normalização de schemas.
* Tratamento de nulos e imputação:

  * `minimum_order_value_imputed`: mediana por `price_range`.
  * `delivery_time_imputed`: mediana por `price_range`.
* Garantia de unicidade de `order_id` (checagem de duplicatas com colunas principais).

### 2. Checagens Pós-ETL

* Faixa de datas (UTC) de `orders_silver`.
* Distribuição A/B (`users_silver`).
* Contagem de nulos em colunas-chave.
* Amostragem de previews para sanity check.
* Checagem adicional de duplicatas por conteúdo de ordem (mesmo cliente, valor, data).

### 3. Análise A/B (Tarefa 1)

#### Métricas de impacto

* **GMV/usuário (média)**
* **Pedidos/usuário (média)**
* **Conversão (proporção de usuários com ≥1 pedido)**
* **AOV (média do valor dos pedidos)**

Além das médias, reportamos também:

* **Mediana de GMV/usuário, Pedidos/usuário e AOV** (reduz outliers)
* **p95 de GMV/usuário, Pedidos/usuário e AOV** (captura a cauda superior sem extremos)
* **Heavy users (% com ≥3 pedidos no período)**

Também reportamos métricas robustas (medianas, p95, heavy users) calculadas no Spark e usamos amostragem para gráficos e testes não-paramétricos quando necessário.

#### Testes estatísticos

* **Welch t-test** (médias, exploratório)  
* **Mann–Whitney U** (robusto, apresentado no relatório)  
* **Z-test de proporções** (para conversão; não aplicável no experimento atual)

#### Premissas financeiras

As premissas padrão (take rate, custo do cupom) são lidas de `config/settings.yaml` e podem ser sobrescritas no notebook.
Exemplo atual: `take_rate=0.23`, `coupon_cost=10.0`.
Outras premissas (taxa de resgate, horizonte temporal) também podem ser parametrizadas no notebook ao chamar `financial_viability`.

#### Indicadores financeiros

Calculados na função `financial_viability`, a partir de premissas explícitas e com agregassão no Spark:

* **ROI absoluto e por usuário**
* **CAC (Custo de Aquisição de Cliente)**
* **LTV (Lifetime Value, horizonte do experimento)**
* **LTV:CAC (sustentabilidade financeira)**

#### Relatório final (executivo)

No relatório, mantemos apenas:

* Métricas robustas (medianas, p95, heavy users)
* Testes robustos (Mann–Whitney U)
* ROI, LTV, CAC, LTV:CAC  
* Premissas financeiras claras (take rate, valor do cupom, taxa de resgate)

### 4. Segmentação de usuários (Tarefa 2)

#### Objetivo
Agrupar clientes com comportamentos semelhantes para **direcionar o cupom certo ao público certo**, maximizando engajamento/retenção e **ROI**.

#### (a) Critérios de segmentação e racional
- **Frequência (Heavy user)** — *heavy* (≥ 3 pedidos no período) vs *não-heavy* (< 3).  
  *Por quê?* frequência é o melhor preditor de valor; separa quem já tem hábito de quem ainda está “em formação”.
- **Plataforma de origem** — `android`, `ios`, `desktop`.  
  *Por quê?* jornada/ticket variam por dispositivo/canal.
- **RFM (Recency-Frequency-Monetary)** — códigos `111–555` (1=baixo, 5=alto).  
  *Por quê?* permite graduar incentivo por valor e recência.
- **Novo vs recorrente** — sinal informativo; nesta base, “novo” é residual (amostra focada em quem já comprou).

> **Como ler os gráficos:** mostramos o **valor típico por cliente (mediana)**; **p95** ilustra a “ponta de cima”; **heavy users** é a % de clientes com ≥3 pedidos.  
> **Financeiro (R$)** é calculado sobre **100% da base** no Spark (agregados), e só o **resumo** vem para o relatório.

**Artefatos gerados (salvos em `outputs/`):**
- Tabelas robustas (medianas/p95): `robust_heavy_user.csv`, `robust_origin_platform.csv`, `robust_rfm_segment.csv`, `robust_is_new_customer.csv`.
- Tabelas de médias (sanity check / apêndice): `ab_*_summary.csv`.
- Figuras: `outputs/figs_segments/` (barras de **medianas**, p95 e taxa de heavy; boxplots/hist EDA).

## 🔒 Privacidade

- Dados PII **não** são mantidos nas camadas analíticas (hash/removidos).  
- Os arquivos de dados **não** são versionados no Git; sempre baixados de fontes configuradas em `settings.yaml`.


