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
- Métricas A/B por grupo (Spark)
- Coleta de dados por usuário para testes (Pandas)
- Testes estatísticos (Welch t-test e z-test)
- Viabilidade financeira (ROI com premissas)

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

* **GMV/usuário**: soma do valor de pedidos dividido pelo número de usuários.
* **Pedidos/usuário**: frequência média de pedidos por usuário.
* **Conversão**: proporção de usuários com ≥1 pedido no período.
* **AOV (Average Order Value)**: valor médio por pedido.
* **CAC (Custo de Aquisição de Cliente)**: custo de cupons dividido por número de usuários que resgataram.
* **LTV (Lifetime Value)**: receita líquida média por usuário no horizonte do experimento (`GMV/usuário × take rate`).
* **LTV\:CAC**: razão entre LTV e CAC, avalia sustentabilidade financeira.
- **Mediana de GMV/usuário, Pedidos/usuário e AOV**: reduz a influência de outliers e representa o “usuário típico” no período.
- **p95 (GMV/usuário, Pedidos/usuário, AOV)**: ponto abaixo do qual estão 95% dos usuários; útil para reportar resultados robustos sem a cauda extrema.
- **Heavy users (% com ≥3 pedidos no período)**: indica se a campanha aumentou hábito e recorrência, não apenas compras pontuais.

**Racional**: Complementar médias com medianas/p95 e a taxa de heavy users torna a análise mais fiel ao comportamento da maioria e mais defensável para stakeholders.

#### Testes estatísticos

* **Welch t-test** para comparação de médias (GMV/usuário, pedidos/usuário, AOV).
* **Z-test para proporções** (conversão). No experimento atual, não aplicável pois todos usuários já tinham ≥1 pedido.

- **Mann–Whitney U (não-paramétrico)** para GMV/usuário, Pedidos/usuário e AOV. Não assume normalidade; compara distribuições (medianas/ordens) e complementa o Welch t-test em dados com long tail.

**Nota**: mantemos o Welch t-test por ser padrão para médias, mas reportamos também o Mann–Whitney para robustez.

#### Premissas financeiras

1. **Custo do cupom**: R\$ 10,00, 100% pago pelo iFood (sem coparticipação de restaurantes).
2. **Take rate (comissão iFood)**: 23%, valor plausível do mercado, usado como referência fixa.
3. **Taxa de resgate**: cenário base 30%.
4. **Horizonte temporal**: apenas o período do experimento (jan/2019), LTV calculado nesse intervalo.
5. **Receita incremental**: `uplift de GMV/usuário × número de usuários tratados × take rate`.
6. **Custos adicionais**: não considerados (marketing, operação, suporte). Somente custo direto dos cupons.

#### Indicadores financeiros

* **Receita incremental total**: diferença de GMV ajustada pela comissão.
* **Custo total da campanha**: `n_treated × valor cupom × taxa de resgate`.
* **ROI absoluto**: receita incremental – custo total.
* **ROI por usuário**: ROI absoluto dividido pelo nº de usuários tratados.
* **LTV, CAC e LTV\:CAC** complementam análise de sustentabilidade.
* Os **KPIs robustos** (**medianas/p95/heavy users**) ajudam a evitar decisões enviesadas por outliers, garantindo que o ROI/LTV:CAC seja interpretado à luz do comportamento da maioria dos usuários.”

---

## 🔒 Privacidade

- Dados PII **não** são mantidos nas camadas analíticas (hash/removidos).  
- Os arquivos de dados **não** são versionados no Git; sempre baixados de fontes configuradas em `settings.yaml`.

---

## 📌 Resumo para a apresentação

- **Por que Colab-only?** Reprodutibilidade e simplicidade para os avaliadores.  
- **Gargalo conhecido:** `orders` é grande e gzip não é splittable → leitura 1 task; depois **repartition + broadcast**.  
- **Qualidade:** pré-flight fail-fast + profiling guiando o ETL; timezone/PII/validações.  
- **A/B → ROI → RFM** na ordem pedida, com **premissas explícitas** e **próximos passos**.

---
