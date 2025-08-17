# iFood Case – Cupons

Repositório do case para **Analista de Dados** no iFood. Objetivo: analisar um **teste A/B** de cupons com foco em **retenção** e crescimento, seguindo **exatamente** a ordem pedida no case (A/B → viabilidade financeira → segmentação → recomendações).

> Execução **100% no Google Colab** para máxima reprodutibilidade (sem dependências locais).

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/analise_completa.ipynb){:target="_blank" rel="noopener"}

---

## 🔄 O que mudou desde a última versão

- **Colab-only**: removida a necessidade de execução local (JDK/winutils etc.). O notebook faz **clone do repo**, **instala dependências** e **baixa dados** sozinho.
- Novo **pré-flight fail-fast** (`src/checks.py`): valida existência/tamanho/integração (`.gz` e `.tar.gz`) e identifica **CSVs válidos** do A/B (ignora artefatos `._*.csv`, `.DS_Store`).
- **Leitura robusta** de `orders` em `src/etl.py`:
  - Detecção automática **NDJSON** (um JSON por linha) **vs** **JSON array**.
  - Para NDJSON (nosso caso), usamos `spark.read.json(...)`.  
- **Orders é muito grande** (~**1.6 GB** comprimido): gzip **não é splittable**, então a descompressão ocorre em **um único task**; após ler, **reparticionamos** o dataset conformado por `customer_id` (usando `spark.sql.shuffle.partitions`) para **paralelizar os joins**.  
  - Também **broadcast** das dimensões pequenas: `restaurants` (sempre) e `abmap` (se couber).
- **Profiling rápido** pós-leitura (`profile_loaded`): mostra **schema**, **amostras**, **faixa de datas**, **nulos** e **distribuição** do grupo do A/B — orienta as regras do ETL.
- **Janela do experimento**: se não vier no `settings.yaml`, é **inferida automaticamente** como `[min(data), max(data)+1d)` em UTC e aplicada como filtro.
- **PII**: `cpf`/telefone **hasheados**; campos de endereço/nome **removidos** das camadas analíticas.
- **Camadas “silver” em memória**: salvar Parquet é **opcional** (desativado por padrão no Colab para reduzir I/O).

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

## ▶️ Como executar

1. Abra o notebook **no Colab**:  
   [**analise_completa.ipynb**](https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/analise_completa.ipynb){:target="_blank" rel="noopener"}

2. Menu **Runtime → Run all**. A primeira célula:
   - clona/atualiza o repositório;  
   - instala as dependências do `requirements.txt`;  
   - roda o **download programático** (`scripts/download_data.py`).

3. O notebook então executa:
   - **Pré-flight** (fail-fast) dos arquivos baixados;  
   - **Profiling** dos 4 dataframes brutos;  
   - **ETL** completo com normalização de timezone/PII e joins;  
   - **A/B**, **viabilidade** e **RFM** (em sequência).

> **Tempo de execução:** a leitura de `orders` (~1.6 GB gz) pode levar alguns minutos no Colab (gzip não é splittable). Depois da leitura, o ETL **reparticiona por `customer_id`** para paralelizar os joins.

---

## ⚙️ Parâmetros importantes (`config/settings.yaml`)

| Caminho                         | Descrição |
|--------------------------------|-----------|
| `data.raw_dir`                 | Pasta dos brutos (default: `data/raw`) |
| `data.processed_dir`           | Pasta dos parquet (se habilitar salvar) |
| `runtime.spark.shuffle_partitions` | Nº de partições p/ shuffles/joins (usado no `repartition`) |
| `runtime.spark.driver_memory`  | Memória do driver no Colab (ex.: `8g`/`12g`) |
| `analysis.business_tz`         | TZ de negócio (default `America/Sao_Paulo`) |
| `analysis.experiment_window`   | `{start: 'YYYY-MM-DD', end: 'YYYY-MM-DD'}`; se ausente, **auto-inferida** |
| `analysis.auto_infer_window`   | `true`/`false` — ativa a inferência de janela |
| `analysis.treat_is_target_null_as_control` | `false` por padrão (linhas sem grupo são excluídas) |
| `analysis.winsorize`/`use_cuped` | Parâmetros para A/B (aplicados nas análises) |

---

## 🧱 Decisões técnicas & premissas (essenciais para explicar)

- **Formato de `orders`**: detectado como **NDJSON**; leitura com `spark.read.json(...)`.  
  - Como o arquivo é grande e gzip não é splittable, a leitura inicial roda em 1 task; após ler, fazemos:  
    **`o = o.repartition(spark.sql.shuffle.partitions, 'customer_id')`** para **distribuir** o trabalho nos joins.
  - **Broadcast** de dimensões: `restaurants` sempre (pequena) e `abmap` se `count ≤ 2M`.
- **Timezone**: criamos `event_ts_utc` (UTC canônico). Para análises diárias de negócio, usamos `event_date_brt` (UTC → BRT).  
- **Evento**: se `order_scheduled==true` e há `order_scheduled_date`, o **evento** é a data agendada; caso contrário, a de criação.  
- **PII**: `cpf` e telefone geram `*_hash`; campos sensíveis (nome/endereço) **removidos** nas “silver”.  
- **Qualidade de dados**:  
  - descartamos `order_id` ou `customer_id` **nulos**, `order_id` **duplicado**;  
  - `order_total_amount` negativo vira **nulo**;  
  - coordenadas de `merchant_*` fora do intervalo válido viram **nulo**.  
- **Janela do experimento**: se não definida, inferimos `[min, max+1)` e reportamos no log (UTC).  
- **Sem marcação A/B (`is_target` nulo)**: **excluímos** por padrão (evitar viés); pode ser mudado por config.  
- **Persistência**: por padrão **não** salvamos Parquet no Colab (`SAVE_PARQUET=False`) — tudo roda **em memória**; habilitar apenas se necessário.

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

## 📈 A/B, ROI e Segmentação (no notebook)

- **A/B**: métricas por usuário (**GMV/U, Pedidos/U, Conversão, AOV**), **Welch t-test** (médias) e **z-test** (proporções). Opcional: **CUPED**.  
- **Viabilidade**: **ROI** e **sensibilidade** (take rate, custo do cupom, cobertura). **Premissas** ficam explícitas no topo da seção.  
- **Segmentação (RFM)**: regras claras e leitura do **uplift por segmento**, com **ações sugeridas** por público.
- **Melhorias futuras**: **K-Means** como refinamento da segmentação; **guardrails** adicionais para o novo A/B.

---

## 🧰 Solução de problemas (Colab)

- **Execução lenta no começo**: esperado por causa de `orders` (~1.6 GB gz, gzip não splittable). Após a leitura, o ETL paraleliza.  
- **Out of memory**: aumente `runtime.spark.driver_memory` no `settings.yaml` (ex.: `12g`) e reduza o número de colunas exibidas (menos `.toPandas()` em previews).  
- **Erro de rede no download**: reexecute a 1ª célula (o script é idempotente).  
- **CSV do A/B não encontrado**: o pré-flight falha cedo e indica o problema na pasta `data/raw/ab_test_ref_extracted/`.

---

## 🔒 Privacidade

- Dados PII **não** são mantidos nas camadas analíticas (hash/removidos).  
- Os arquivos de dados **não** são versionados no Git; sempre baixados de fontes configuradas em `settings.yaml`.

---

## 📌 Resumo para a apresentação

- **Por que Colab-only?** Reprodutibilidade e simplicidade para os avaliadores.  
- **Gargalo conhecido**: `orders` é grande e gzip não splittable → leitura 1 task; depois **repartition + broadcast**.  
- **Qualidade**: pré-flight fail-fast + profiling guiando o ETL; timezone/PII/validações.  
- **A/B → ROI → RFM** na ordem pedida, com **premissas explícitas** e **próximos passos**.

---
