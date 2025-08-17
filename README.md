# Case iFood: Teste A/B Estratégia de Cupons

Repositório do case para **Analista de Dados** no iFood. Objetivo: analisar um **teste A/B** de cupons com foco em **retenção** e crescimento.

> Execução **100% no Google Colab** para máxima reprodutibilidade (sem dependências locais).

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/analise_completa.ipynb)

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
│  ├─ pipeline_analise_completa.ipynb  # notebook principal (orquestração da análise)
├─ src/
│  ├─ __init__.py
│  ├─ utils.py                       # settings + spark + seeds
│  └─ etl.py                         # ingestão + limpeza + joins + silvers
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

## ▶️ Como executar

### Execução no Colab

1. Abra o notebook **no Colab**:  
   [**analise_completa.ipynb**](https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/analise_completa.ipynb)

2. Menu **Ambiente de Execução → Executar Tudo**. A primeira célula:
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

## ⚙️ Configurações importantes (`config/settings.yaml`)

| Caminho                         | Descrição |
|--------------------------------|-----------|
| `data.raw_dir`                 | Pasta dos brutos (default: `data/raw`) |
| `data.processed_dir`           | Pasta dos parquet (se habilitar salvar) |
| `runtime.spark.shuffle_partitions` | Nº de partições p/ shuffles/joins (usado no `repartition`) |
| `runtime.spark.driver_memory`  | Memória do driver no Colab (`12g`) |
| `analysis.business_tz`         | TZ de negócio (default `America/Sao_Paulo`) |
| `analysis.experiment_window`   | **auto-inferida** |
| `analysis.auto_infer_window`   | `true`/`false` — ativa a inferência de janela |
| `analysis.treat_is_target_null_as_control` | `false` por padrão (linhas sem grupo são excluídas) |
| `analysis.winsorize`/`use_cuped` | Parâmetros para A/B (aplicados nas análises) |

---

## 🧱 Decisões técnicas & otimizações de desempenho

### Formato e leitura dos dados
- **Formato de `orders`**: detectado como **NDJSON**; leitura com `spark.read.json(...)`.  
  - Como o arquivo é grande e gzip não é splittable, a leitura inicial roda em 1 task; após ler, fazemos:  
    **`o = o.repartition(spark.sql.shuffle.partitions, 'customer_id')`** para **distribuir** o trabalho nos joins.
  - **Broadcast** de dimensões: `restaurants` sempre (pequena) e `abmap` se `count ≤ 2M`.

### Configuração do Spark
- **shuffle_partitions**: otimizado para **128** com base em benchmarks no Colab.  
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
- **Gargalo conhecido**: `orders` é grande e gzip não é splittable → leitura 1 task; depois **repartition + broadcast**.  
- **Qualidade**: pré-flight fail-fast + profiling guiando o ETL; timezone/PII/validações.  
- **A/B → ROI → RFM** na ordem pedida, com **premissas explícitas** e **próximos passos**.

---
