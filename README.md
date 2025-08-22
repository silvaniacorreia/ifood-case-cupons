# Case iFood: Teste A/B de Cupons

Repositório do case técnico de **Analista de Dados** no iFood.
O objetivo foi analisar um **teste A/B de cupons**, avaliando impacto em engajamento, gasto dos usuários e viabilidade financeira, além de propor **melhorias estratégicas** e **segmentação de usuários**.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/pipeline_analise_completa.ipynb)

---

## Contexto da análise

* **Objetivo:** verificar se o cupom aumenta pedidos, GMV (gasto total) e engajamento.
* **Método:** experimento A/B (controle vs tratamento).
* **Avaliações principais:**

  * Métricas de engajamento (GMV, pedidos, AOV, heavy users).
  * Sustentabilidade financeira (ROI, LTV, CAC).
  * Segmentação de clientes (frequência, plataforma, RFM).
* **Entrega final:** relatório executivo com insights e recomendações para próximos testes.

---

## Escopo

1. **Análise A/B de cupons**

   * Definição de métricas de sucesso.
   * Testes estatísticos robustos (medianas, p95, heavy users).
   * Viabilidade financeira (ROI e payback).

2. **Segmentação de usuários**

   * Critérios: frequência, plataforma, RFM, novos vs recorrentes.
   * Leitura do efeito do cupom em cada grupo.
   * Ações recomendadas por segmento.

3. **Próximos passos**

   * Proposta de novos testes A/B direcionados.
   * Estratégias para melhorar eficiência dos cupons.

---

## Como executar

### Execução no Google Colab

1. Abra o notebook (botão direito → `Abrir link em uma nova guia`)
   [**pipeline\_analise\_completa.ipynb**](https://colab.research.google.com/github/silvaniacorreia/ifood-case-cupons/blob/main/notebooks/pipeline_analise_completa.ipynb)

2. No menu superior, clique em `Executar Tudo`. Ao exibir a janela `Aviso: este notebook não é de autoria do Google`, clique em `Executar assim mesmo`.
   A primeira célula irá:

   * Clonar/atualizar o repositório.
   * Instalar dependências (`requirements.txt`).
   * Baixar os dados (`scripts/download_data.py`).

3. O notebook roda sequencialmente:

   * **Pré-checks** dos dados.
   * **ETL** (limpeza, joins, normalizações).
   * **Análise A/B** com métricas e testes estatísticos.
   * **Análise financeira** (ROI, LTV/CAC).
   * **Segmentação de usuários** (frequência, plataforma, RFM).
   * **Cálculo de break-even** e projeção de margem incremental.

> ⏱️ O processamento pode levar alguns minutos devido ao tamanho dos arquivos, variando conforme o horário de execução (o Google altera a oferta de recursos no Google Colab de forma dinâmica).

---

## Configurações principais

As configurações ficam em `config/settings.yaml`:

* `runtime.spark.driver_memory`: memória do driver (12g no Colab).
* `analysis.business_tz`: timezone de negócio (America/Sao\_Paulo).
* `analysis.winsorize` / `analysis.use_cuped`: parâmetros opcionais para o A/B.
* `data.raw_dir` e `data.processed_dir`: caminhos dos dados.
* `finance.take_rate`: taxa de comissão do iFood (23%).
* `finance.coupon_cost_default`: valor padrão do cupom (R\$10).
* `finance.coupon_cost_grid`: grade de valores de cupons (R\$3 a R\$15).

---

## Saídas

* **Artefatos intermediários**:

  * Tabelas robustas por grupo/segmento (`outputs/*.csv`).
  * Gráficos comparativos (boxplots, histogramas, barras).

---

## Privacidade

* Dados PII foram **removidos ou hasheados**.
* Nenhum dado bruto é versionado no Git (apenas baixado via script).

## Estrutura do repositório

```
ifood-case-cupons/
├─ README.md
├─ requirements.txt
├─ notebooks/
│  └─ pipeline_analise_completa.ipynb   # notebook principal (orquestração)
├─ src/
│  ├─ utils.py          # config Spark, seeds
│  ├─ etl.py            # ingestão + limpeza + joins
│  ├─ finance.py        # funções de análise financeira
│  ├─ analysis_ab.py    # funções de análise A/B
│  ├─ analysis_segments.py # funções de análise por segmentos
│  ├─ viz_ab.py         # visualizações A/B
│  └─ viz_segments.py   # visualizações por segmentos
├─ scripts/
│  └─ download_data.py  # baixa e organiza dados brutos
├─ config/
│  └─ settings.yaml     # parâmetros de execução
├─ data/
│  ├─ raw/              # dados brutos
│  └─ processed/        # parquet do ETL
└─ report/
   ├─ assets/
   │  ├─ figs_segments/ # figuras por segmento
   │  └─ ab/            # figuras do A/B
   └─ relatorio_final.md/.pdf # relatório executivo
```