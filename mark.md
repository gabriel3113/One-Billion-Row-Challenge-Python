# Estratégia de Ingestão e Historização de Transações (Postgres → BigQuery)

## 1) Contexto e problema atual

- Serviço no GCP lê múltiplas tabelas do **Postgres** e consolida **transações** no **BigQuery**.
- Processo atual **apaga e repopula** a tabela final com **últimos 6 meses** diariamente.
- Necessidade de **manter histórico completo** (auditoria) **sem** explodir custo/performance.

## 2) Objetivos

1. **Parar** o ciclo de truncar + recarregar 6m.
2. **Capturar deltas** (linhas alteradas) usando **watermark** baseado em `updated_at`.
3. Manter um **histórico SCD2** (quem/quando mudou) na camada **trusted**.
4. Expor uma camada **refined** enxuta e **sempre atualizada** (janela de 6 meses) para consumo analítico.
5. **Reduzir custo** de processamento e melhorar performance de consulta.

## 3) Visão geral da solução

- **Staging** recebe **apenas deltas** do Postgres: `updated_at >= (watermark - 2 dias)` (janela de segurança para eventos atrasados).
- **Trusted (SCD2)** guarda histórico de mudanças **somente quando valores relevantes mudam** (ex.: `status`, `amount`).
- **Refined** mantém:
  - **estado vigente** por `transaction_id` (view/tabela atualizada), e
  - **janela de 6 meses** para relatórios (atualização incremental por partição ou recriação diária barata).

## 4) Modelagem e tabelas

### 4.1) Staging (atômica, descartável)

``

- **Partição**: `dt_insert` (data/hora de ingestão).
- Conteúdo: somente registros cujo `updated_at` ≥ (watermark - 2 dias).
- Deduplicação local por `(transaction_id, updated_at)` se necessário.

```sql
CREATE TABLE IF NOT EXISTS `staging.transactions_delta` AS
SELECT * FROM UNNEST([]); -- placeholder para criação inicial
```

### 4.2) Trusted (histórico SCD2 "light")

``

- **Chaves/colunas** (exemplo, ajuste aos seus campos reais):
  - `transaction_id` STRING
  - `status` STRING
  - `amount` NUMERIC
  - `created_at` TIMESTAMP (origem)
  - `updated_at` TIMESTAMP (origem)
  - `valid_from` TIMESTAMP (quando a versão passou a valer)
  - `valid_to` TIMESTAMP (quando deixou de valer; default 9999-12-31)
  - `is_current` BOOL
  - `dt_insert` TIMESTAMP (ingestão)
  - `row_hash` STRING (hash dos campos monitorados)

**Particionamento e clustering** (recomendado):

- **PARTITION BY** `DATE(updated_at)` (usa o timestamp de origem das mudanças / carimbo temporal do status).
- **CLUSTER BY** `transaction_id, status`.
- `` para evitar *full scans*.
- **CLUSTER BY** `transaction_id, status`.
- `` para evitar *full scans*.

```sql
CREATE TABLE IF NOT EXISTS `trusted.transactions_scd2` (
  transaction_id STRING,
  transaction_status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_current BOOL,
  dt_insert TIMESTAMP,
  row_hash STRING
)
PARTITION BY DATE(updated_at)
CLUSTER BY transaction_id, transaction_status
OPTIONS (require_partition_filter = TRUE);
```

**Como definimos mudança (foco em custo)**:

- Para reduzir armazenamento e gravações, **versione apenas quando **``** mudar**. Assim, a maioria dos updates que não alteram status **não** gera nova versão.

```sql
TO_HEX(SHA256(TO_JSON_STRING(STRUCT(status)))) AS row_hash
```

> Se for necessário incluir outros atributos (ex.: `amount`), adicione-os ao `STRUCT`. Isso aumenta a fidelidade histórica, mas também o volume de linhas.

**MERGE SCD2 (exemplo completo, sem **``**)**

```sql
MERGE `trusted.transactions_scd2` T
USING (
  SELECT
    transaction_id,
    status,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP() AS dt_insert,
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(status)))) AS row_hash
  FROM `staging.transactions_delta`
) S
ON T.transaction_id = S.transaction_id AND T.is_current = TRUE
WHEN MATCHED AND T.row_hash <> S.row_hash THEN
  UPDATE SET T.is_current = FALSE
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    transaction_id, transaction_status,
    created_at, updated_at, is_current, dt_insert, row_hash
  )
  VALUES (
    S.transaction_id, S.transaction_status,
    S.created_at, S.updated_at, TRUE, S.dt_insert, S.row_hash
  );
```

> **Deleções** no Postgres: se desejar refletir, envie *tombstones* no delta e trate com um `WHEN MATCHED AND S.is_deleted THEN ...` fechando `valid_to`.

### 4.3) Refined (estado vigente + janela 6m)

`` (estado atual por `transaction_id`)

```sql
CREATE OR REPLACE TABLE `refined.transactions_current`
PARTITION BY DATE(updated_at)
CLUSTER BY transaction_id AS
SELECT
  a.*,  -- atributos vigentes
  s.transaction_status
FROM `refined.transactions_attributes_current` a
JOIN `trusted.transactions_scd2` s
  ON s.transaction_id = a.transaction_id
WHERE s.is_current = TRUE;
```

`` (janela para relatórios)

- **Opção simples (recria janela diariamente)** — custo baixo, robusto:

```sql
CREATE OR REPLACE TABLE `refined.transactions_6m`
PARTITION BY DATE(updated_at)
CLUSTER BY transaction_id AS
SELECT *
FROM `refined.transactions_current`
WHERE updated_at >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 6 MONTH);
```

- **Opção incremental por partição** — MERGE apenas nas datas afetadas pelas mudanças do dia.

## 5) Orquestração (Composer/Airflow)

**Passos do DAG diário**

1. **Extrair Postgres → Staging**
   - Query: `SELECT ... FROM src WHERE updated_at >= :watermark - INTERVAL '2 days'`.
   - Carregar em `staging.transactions_delta` (partição `dt_insert = hoje`).
2. **MERGE SCD2** em `trusted.transactions_scd2`.
3. **Atualizar** `refined.transactions_current`.
4. **Atualizar** `refined.transactions_6m` (recriar janela **ou** MERGE por partições tocadas).
5. **Atualizar watermark** com `MAX(updated_at)` processado.
6. **Manutenção**: expirar partições antigas de `staging` (ex.: 7 dias), compactar partições muito fragmentadas se necessário.

**Esqueleto (pseudo) de DAG**

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime

# 1) Extrair delta do Postgres e gravar em staging (use um PythonOperator)
# 2) Executar MERGE SCD2 (BigQueryInsertJobOperator)
# 3) Atualizar refined_current
# 4) Atualizar refined_6m
# 5) Atualizar watermark (tabela control.watermarks)
```

## 6) Watermark e janela de segurança

**Tabela de controle (watermark composto)**: `control.watermarks`

```sql
CREATE TABLE IF NOT EXISTS `control.watermarks` (
  source STRING,   -- ex.: 'transactions'
  wm_ts  TIMESTAMP, -- maior updated_at processado
  wm_id  STRING     -- desempate: maior transaction_id no wm_ts
);
```

**Leitura do watermark** (no início do DAG):

```sql
-- Se não existir linha para a fonte, trate default no código (ex.: 1970-01-01 e wm_id = '')
SELECT wm_ts, wm_id
FROM `control.watermarks`
WHERE source = 'transactions';
```

**Extração do delta** (Postgres) com **janela de segurança** (ex.: 2 dias) e desempate por ID:

```sql
SELECT *
FROM transactions
WHERE updated_at >  :wm_ts - INTERVAL '2 days'
   OR (updated_at = :wm_ts AND transaction_id > :wm_id);
```

**Atualização do watermark** (após processar o delta):

```sql
MERGE `control.watermarks` t
USING (
  SELECT
    'transactions' AS source,
    MAX(updated_at) AS wm_ts,
    -- pega o maior transaction_id no maior updated_at
    ANY_VALUE(transaction_id) AS wm_id
  FROM `staging.transactions_delta`
  QUALIFY ROW_NUMBER() OVER (ORDER BY updated_at DESC, transaction_id DESC) = 1
) s
ON t.source = s.source
WHEN MATCHED THEN UPDATE SET t.wm_ts = s.wm_ts, t.wm_id = s.wm_id
WHEN NOT MATCHED THEN INSERT (source, wm_ts, wm_id) VALUES (s.source, s.wm_ts, s.wm_id);
```

> **Por que composto?** Evita perder/duplicar linhas quando múltiplos registros compartilham o mesmo `updated_at`.

## 7) Custos e performance

- **Particionamento + clustering** → leituras mais baratas e rápidas.
- `` evita *full scan acidental*.
- **Gravar só quando **``** muda** diminui muito o volume no histórico.
- **Staging com TTL** (ex.: 7 dias). Trusted com retenção conforme compliance (ex.: 12–24 meses) ou arquivamento em GCS/`trusted_archive`.
- BigQuery aplica **long-term storage** automático em partições não alteradas por >90 dias (custo de storage cai).

## 8) Alternativas/Complementos

- **CDC gerenciado**: Datastream (GCP) ou Debezium (Kafka) para captar mudanças do Postgres em tempo (quase) real.
- **dbt/Dataform** para gerenciar modelos incrementais e *tests* (chaves únicas, *not null*, etc.).
- **Materialized Views** quando aplicável, para *queries* quentes.

## 9) Perguntas em aberto (para fechar desenho)

1. **Data de criação**: é o momento de checkout ou o registro do pedido no sistema?
2. **Data de atualização**: representa qualquer alteração ou apenas mudança de **status**?
3. **Data de status**: onde e como é gerada (fonte/regra de negócio)?
4. **Histórico de status por \*\*\*\*\*\*\*\***``: manter um **histórico de mudanças** (semana 1 status 1; semana 2 status 2) e análises por **snapshot temporal** ou sobrescrever o status ao longo de todo o percurso?
5. Filtros mais comuns nas consultas (para definirmos **a melhor coluna de partição**): `valid_from`, `updated_at` ou `dt_insert`?

## 10) Próximos passos

1. Validar as **perguntas em aberto** acima.
2. Confirmar **colunas que disparam versão** no SCD2 (ex.: só `status` ou `status+amount+...`).
3. Definir a **coluna de partição** da trusted (recomendo `valid_from` se houver análise temporal).
4. Entregar **DAG** com watermark + MERGE + atualização refined.
5. Executar **teste piloto** (7 dias) e avaliar custo/performance.

## 11) Exemplos de consultas úteis

- **Estado vigente** de uma transação (linha atual):

```sql
SELECT * FROM `trusted.transactions_scd2`
WHERE transaction_id = '...'
  AND is_current = TRUE;
```

- **Status "as-of" em uma data/hora de referência (sem **``**):**

```sql
SELECT * EXCEPT (rn)
FROM (
  SELECT t.*, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY updated_at DESC) rn
  FROM `trusted.transactions_scd2` t
  WHERE transaction_id = '...'
    AND updated_at <= TIMESTAMP('2025-08-01 00:00:00')
)
WHERE rn = 1;
```

- **Contagem de mudanças de status por mês (com base em **``**):**

```sql
SELECT DATE_TRUNC(DATE(updated_at), MONTH) AS mes, COUNT(*) AS qtd
FROM `trusted.transactions_scd2`
WHERE DATE(updated_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND CURRENT_DATE()
GROUP BY mes
ORDER BY mes;
```

## 12) Estratégia foco em custo: **Status vigente** + histórico enxuto) Estratégia foco em custo: **Status vigente** + histórico enxuto

Para equilibrar **histórico** e **baixo custo**, priorize o consumo no **estado vigente** e mantenha um histórico **mínimo**:

**Opção A — SCD2 somente por mudança de **``** (PADRÃO ADOTADO):**

- `trusted.transactions_scd2` guarda versões **apenas** quando `status` muda (hash em `status`).
- Colunas mínimas: `transaction_id`, `status`, `updated_at`, `is_current`, `dt_insert`, `row_hash`.
- **Vantagem:** poucas gravações, histórico suficiente para auditoria de status.

**Opção B — SCD1 + Log de mudanças (ainda mais leve):**

- Tabela **current** (upsert) com o estado atual por `transaction_id` (sem histórico completo).
- Tabela de **log de status** append-only (1 linha por transição):

```sql
CREATE TABLE IF NOT EXISTS `trusted.transactions_status_log` (
  transaction_id STRING,
  from_status    STRING,
  to_status      STRING,
  changed_at     TIMESTAMP,
  dt_insert      TIMESTAMP
) PARTITION BY DATE(changed_at) CLUSTER BY transaction_id;
```

- **Vantagem:** armazenamento mínimo. **Desvantagem:** consultas “as-of” exigem reconstrução via log.

**Consumo (refined):**

- `refined.transactions_current` é a tabela **oficial** para analytics/BI.
- `refined.transactions_6m` é opcional (janela quente), podendo ser recriada diariamente (barato) ou atualizada por partição tocada.

**Arquivamento:**

- Se necessário, mova versões antigas (ex.: > 24 meses) para `trusted_archive` ou GCS/Parquet.

## 13) Regras de integridade e qualidade de dados

**Garantir 1 versão vigente por **``**:**

```sql
SELECT transaction_id, COUNTIF(is_current) AS curr_rows
FROM `trusted.transactions_scd2`
GROUP BY transaction_id
HAVING curr_rows != 1;
```

**Detectar intervalos sobrepostos (valid\_from/valid\_to):**

```sql
SELECT transaction_id
FROM (
  SELECT transaction_id, valid_from, valid_to,
         LAG(valid_to) OVER (PARTITION BY transaction_id ORDER BY valid_from) AS prev_to
  FROM `trusted.transactions_scd2`
)
WHERE prev_to IS NOT NULL AND valid_from < prev_to;
```

**Boas práticas de custo:**

- `OPTIONS(require_partition_filter = TRUE)` em tabelas particionadas.
- **Particionamento** por data de vigência (`valid_from`) ou `updated_at` e **clustering** por `transaction_id`.
- **TTL** no `staging` (ex.: 7 dias) e retenção/arquivamento na `trusted`.
- Deduplicação no **staging** por `(transaction_id, updated_at)`.
- **Recriar janela 6m** diariamente quando aceitável (simples e barato).

## 14) Integração com a **query fonte (Postgres)**

Abaixo está a **query atualmente utilizada** para consolidar transações e enriquecer com ofertas (principal e order bumps). Usaremos esta estrutura como **fonte** para o pipeline incremental (staging → trusted SCD2 → refined):

```sql
WITH order_bumps_pivot AS (
  SELECT
    transaction_id,
    MAX(CASE WHEN rn = 1 THEN checkout_data->>'name' END) AS offer_name_ob1,
    MAX(CASE WHEN rn = 1 THEN checkout_data->>'amount' END) AS offer_amount_ob1,
    MAX(CASE WHEN rn = 2 THEN checkout_data->>'name' END) AS offer_name_ob2,
    MAX(CASE WHEN rn = 2 THEN checkout_data->>'amount' END) AS offer_amount_ob2,
    MAX(CASE WHEN rn = 3 THEN checkout_data->>'name' END) AS offer_name_ob3,
    MAX(CASE WHEN rn = 3 THEN checkout_data->>'amount' END) AS offer_amount_ob3,
    MAX(CASE WHEN rn = 4 THEN checkout_data->>'name' END) AS offer_name_ob4,
    MAX(CASE WHEN rn = 4 THEN checkout_data->>'amount' END) AS offer_amount_ob4
  FROM (
    SELECT
      transaction_checkout.transaction_id,
      transaction_checkout.checkout_data,
      ROW_NUMBER() OVER (PARTITION BY transaction_checkout.transaction_id ORDER BY checkout_id) AS rn
    FROM transaction_checkout
    INNER JOIN transaction t ON t.transaction_id = transaction_checkout.transaction_id
    WHERE checkout_data->>'checkoutType' = 'ORDERBUMP'
      AND t.created_at >= CURRENT_DATE - INTERVAL '6 months'
  ) t
  GROUP BY transaction_id
),
principal_checkout AS (
  SELECT DISTINCT ON (transaction_id)
    transaction_checkout.transaction_id,
    transaction_checkout.checkout_data->>'name'   AS name,
    transaction_checkout.checkout_data->>'amount' AS amount
  FROM transaction_checkout
  INNER JOIN transaction t ON t.transaction_id = transaction_checkout.transaction_id
  WHERE transaction_checkout.checkout_data->>'checkoutType' != 'ORDERBUMP'
    AND t.created_at >= CURRENT_DATE - INTERVAL '6 months'
  ORDER BY transaction_checkout.transaction_id, transaction_checkout.checkout_id
)

SELECT DISTINCT ON (t.transaction_id)
    t.transaction_id,
    t.status AS transaction_status,
    t.amount_with_tax AS amount,
    t.amount_assiny,
    t.payment_type,
    COALESCE(t.external_id, '') AS external_id,
    COALESCE(t.error_message, '') AS error_message,
    COALESCE(CAST(t.created_at AS TEXT), '') AS created_at,
    COALESCE(CAST(t.updated_at AS TEXT), '') AS updated_at,
    t.installments,
    COALESCE(t.pix_qr_code, '')     AS pix_qr_code,
    COALESCE(t.boleto_barcode, '')  AS boleto_barcode,

    -- JSONB Parameters
    COALESCE(parameters->>'firstName', '')      AS first_name,
    COALESCE(parameters->>'lastName', '')       AS last_name,
    COALESCE(parameters->>'phone', '')          AS phone,
    COALESCE(parameters->>'email', '')          AS email,
    COALESCE(parameters->>'document', '')       AS document,
    COALESCE(parameters->>'eventSourceUrl', '') AS event_source_url,
    COALESCE(parameters->>'userAgent', '')      AS user_agent,
    COALESCE(parameters->>'streetAddress', '')  AS street_address,
    COALESCE(parameters->>'city', '')           AS city,
    COALESCE(parameters->>'state', '')          AS state,
    COALESCE(parameters->>'postalCode', '')     AS postal_code,
    COALESCE(parameters->>'countryCode', '')    AS country_code,
    COALESCE(parameters->>'nodeId', '')         AS node_id,

    -- UTM/Tracking
    COALESCE(parameters->'query'->>'utm_campaign', '') AS utm_campaign,
    COALESCE(parameters->'query'->>'utm_term', '')     AS utm_term,
    COALESCE(parameters->'query'->>'utm_content', '')  AS utm_content,
    COALESCE(parameters->'query'->>'utm_medium', '')   AS utm_medium,
    COALESCE(parameters->'query'->>'utm_source', '')   AS utm_source,
    COALESCE(parameters->'query'->>'trk_src', '')      AS trk_src,
    COALESCE(parameters->'query'->>'trk_cpg', '')      AS trk_cpg,
    COALESCE(parameters->'query'->>'trk_adgp', '')     AS trk_adgp,
    COALESCE(parameters->'query'->>'trk_ad', '')       AS trk_ad,

    -- Relacionamentos
    p.name   AS project_name,
    o.email  AS organization_email,
    o.document AS organization_document,
    o.name  AS organization_name,
    COALESCE(u.name, '')          AS user_name,
    COALESCE(u.email, '')         AS user_email,
    COALESCE(u.phone_number, '')  AS user_phone_number,
    COALESCE(pr.name, '')         AS product_name,
    COALESCE(uw.wallet_id, '')    AS recipient_id,

    -- Ofertas
    COALESCE(pc.name, '')         AS offer_name_pp,
    COALESCE(pc.amount, '')       AS offer_amount_pp,
    COALESCE(ob.offer_name_ob1, '')   AS offer_name_ob1,
    COALESCE(ob.offer_amount_ob1, '') AS offer_amount_ob1,
    COALESCE(ob.offer_name_ob2, '')   AS offer_name_ob2,
    COALESCE(ob.offer_amount_ob2, '') AS offer_amount_ob2,
    COALESCE(ob.offer_name_ob3, '')   AS offer_name_ob3,
    COALESCE(ob.offer_amount_ob3, '') AS offer_amount_ob3,
    COALESCE(ob.offer_name_ob4, '')   AS offer_name_ob4,
    COALESCE(ob.offer_amount_ob4, '') AS offer_amount_ob4

FROM transaction t
INNER JOIN project       p  ON p.id  = t.project_id
INNER JOIN organization  o  ON o.id  = p.organization_id
INNER JOIN "user"       u  ON o.user_id = u.id
INNER JOIN product       pr ON pr.id = t.product_id
LEFT  JOIN user_wallet   uw ON uw.user_id = u.id AND uw.project_id = p.id
LEFT  JOIN principal_checkout pc ON pc.transaction_id = t.transaction_id
LEFT  JOIN order_bumps_pivot ob ON ob.transaction_id = t.transaction_id
WHERE t.created_at >= CURRENT_DATE - INTERVAL '6 months';
```

### 14.1) Ajustes para **carga incremental** (watermark) e custo

- \*\*Trocar o filtro de `` por \*\*`` para capturar **alterações de status** e demais mudanças após a criação.
- **Aplicar watermark composto** (\`wm\_ts\`, \`wm\_id\`) na extração do Postgres:

```sql
WHERE t.updated_at >  :wm_ts - INTERVAL '2 days'
   OR (t.updated_at = :wm_ts AND t.transaction_id > :wm_id)
```

- Manter as CTEs `principal_checkout` e `order_bumps_pivot`, mas **restritas ao conjunto de transações do delta** (por `transaction_id` vindo do filtro acima) para reduzir leitura.
- **No primeiro backfill**, use o filtro por `created_at` (6–12 meses) e depois migre para o incremental via `updated_at`.

### 14.2) **Staging** no BigQuery (esquema sugerido)

Gravar o resultado da query fonte (já "denormalizado") em `staging.transactions_delta` com \*\*partição por \*\*`` e **dedup** por `(transaction_id, updated_at)`:

```sql
CREATE TABLE IF NOT EXISTS `staging.transactions_delta` (
  transaction_id STRING,
  transaction_status STRING,
  amount NUMERIC,
  amount_assiny NUMERIC,
  payment_type STRING,
  external_id STRING,
  error_message STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  installments INT64,
  pix_qr_code STRING,
  boleto_barcode STRING,
  -- parâmetros
  first_name STRING, last_name STRING, phone STRING, email STRING, document STRING,
  event_source_url STRING, user_agent STRING, street_address STRING, city STRING,
  state STRING, postal_code STRING, country_code STRING, node_id STRING,
  -- utm/tracking
  utm_campaign STRING, utm_term STRING, utm_content STRING, utm_medium STRING, utm_source STRING,
  trk_src STRING, trk_cpg STRING, trk_adgp STRING, trk_ad STRING,
  -- relacionamentos
  project_name STRING, organization_email STRING, organization_document STRING, organization_name STRING,
  user_name STRING, user_email STRING, user_phone_number STRING,
  product_name STRING, recipient_id STRING,
  -- ofertas (pivotadas)
  offer_name_pp STRING,  offer_amount_pp STRING,
  offer_name_ob1 STRING, offer_amount_ob1 STRING,
  offer_name_ob2 STRING, offer_amount_ob2 STRING,
  offer_name_ob3 STRING, offer_amount_ob3 STRING,
  offer_name_ob4 STRING, offer_amount_ob4 STRING,
  -- metadados
  dt_insert TIMESTAMP
)
PARTITION BY DATE(dt_insert);
```

> **Observação:** `offer_amount_*` vêm como texto no Postgres; considere **normalizar para NUMERIC** no staging/refined se possível.

**Deduplicação no staging (exemplo):**

```sql
CREATE OR REPLACE TABLE `staging.transactions_delta_dedup` AS
SELECT AS VALUE any_value(t) OVER (PARTITION BY transaction_id, updated_at)
FROM `staging.transactions_delta` t
WHERE DATE(dt_insert) = CURRENT_DATE('America/Sao_Paulo');
```

### 14.3) **Trusted SCD2** com foco em custo

- \*\*Versionar apenas \*\*`` (hash em `status`) para reduzir linhas históricas:

```sql
TO_HEX(SHA256(TO_JSON_STRING(STRUCT(transaction_status)))) AS row_hash
```

- Mapear campos: `valid_from = updated_at`, `valid_to = 9999-12-31`, `is_current = TRUE` na inserção; fechar a versão anterior quando o hash mudar.
- Campos **não versionados** (ex.: `amount`, `utm`, `recipient_id` etc.) podem ser:
  - Mantidos na **refined** (estado vigente) para consumo;
  - Ou armazenados em tabelas auxiliares (ex.: `trusted.transaction_offers`) se quiser modelo mais normalizado.

### 14.4) **Refined** incorporando ofertas

- `refined.transactions_current`: incluir as colunas de oferta **principal** e **order bumps** pivotadas, pois são úteis para BI.
- `refined.transactions_6m`: manter a janela de 6 meses a partir do `updated_at` do estado vigente.

### 14.5) Alternativa de modelagem para ofertas (opcional)

Para reduzir largura/fragmentação da tabela, você pode manter ofertas em uma tabela separada (ou coluna repetida):

```sql
CREATE TABLE IF NOT EXISTS `trusted.transaction_offers` (
  transaction_id STRING,
  position INT64,        -- 0 = principal, 1..4 = OB1..OB4
  offer_type STRING,     -- 'PRINCIPAL' | 'ORDERBUMP'
  offer_name STRING,
  offer_amount NUMERIC,
  dt_insert TIMESTAMP
) PARTITION BY DATE(dt_insert) CLUSTER BY transaction_id;
```

E gerar `refined` pivotada só para consumo.

### 14.6) Impactos de performance

- **Restringir CTEs** `order_bumps_pivot`/`principal_checkout` ao conjunto de `transaction_id` do delta reduz leituras.
- Indexes no Postgres em `transaction.updated_at`, `transaction.transaction_id`, `transaction_checkout.transaction_id`, `transaction_checkout.checkout_id` ajudam no incremental.
- No BigQuery, \*\*cluster por \*\*`` nas tabelas `refined` para acelerar *joins* e filtros por transação.

## 15) Glossário: **Staging (tabela work)**

**Definição.** Área de aterrissagem **temporária** no BigQuery para receber o **delta** extraído do Postgres (via watermark) antes de carregar a camada **trusted**.

**Para que serve.**

- **Isolar** a ingestão das camadas históricas/refined.
- **Deduplicar** registros (ex.: `(transaction_id, updated_at)`).
- **Normalizar tipos** (texto → NUMERIC/TIMESTAMP), higienizar campos.
- **Validar/quarentenar** linhas problemáticas sem poluir o histórico.
- Reduzir custo evitando reprocessos pesados na trusted.

**Características.**

- **Descartável** (pode ser recriada/overwrite por partição sem dor).
- **Não é** fonte para BI/relatórios.
- Esquema simples, focado em facilitar o MERGE da trusted.

**Boas práticas.**

- **Particionar** por `DATE(dt_insert)` e **clusterizar** por `transaction_id`.
- Exigir filtro de partição: `OPTIONS(require_partition_filter = TRUE)`.
- **TTL curto**/expiração de partições (7–15 dias) para controlar storage.
- **Carregar apenas o delta** (watermark composto: `wm_ts`, `wm_id`).
- **Deduplicar** por `(transaction_id, updated_at)` na partição do dia.
- Manter somente as **colunas necessárias** ao MERGE e à auditoria básica.

**Validações úteis.**

- `transaction_id` **NOT NULL** e unicidade por `(transaction_id, updated_at)`.
- Timestamps plausíveis (`updated_at >= created_at`, janela de segurança aplicada).
- Campos numéricos convertidos para **NUMERIC**; JSONs “quebrados” enviados para fila de erro/quarentena.

**Exemplos práticos.**

- Definir expiração e obrigatoriedade de filtro de partição:

```sql
ALTER TABLE `staging.transactions_delta`
SET OPTIONS (
  partition_expiration_days = 7,
  require_partition_filter = TRUE
);
```

- Deduplicar a partição do dia:

```sql
CREATE OR REPLACE TABLE `staging.transactions_delta_dedup` AS
SELECT AS VALUE ANY_VALUE(t) OVER (PARTITION BY transaction_id, updated_at)
FROM `staging.transactions_delta` t
WHERE DATE(dt_insert) = CURRENT_DATE('America/Sao_Paulo');
```

> **Resumo:** pense na *staging* como a sua **“tabela work”**: pouso rápido do delta, limpeza/dedup, tipos corretos e segue o jogo para a **trusted** via MERGE SCD2.

## 16) Decisão de design e DDL/MERGE finais (Opção 1 — SCD2 por mudança de status)

**Decisão:** Adotamos **SCD2 apenas quando **``** muda** na camada **trusted**. As demais colunas (amount, UTM, ofertas, etc.) serão mantidas como **estado vigente (SCD1)** em uma tabela de atributos para consumo na **refined**.

### 16.1) DDL — Staging (delta aterrissado)

> Mantém o esquema amplo (conforme sua query) para facilitar JOINs/validações; partição por `dt_insert`.

```sql
CREATE TABLE IF NOT EXISTS `staging.transactions_delta` (
  transaction_id STRING,
  transaction_status STRING,
  amount NUMERIC,
  amount_assiny NUMERIC,
  payment_type STRING,
  external_id STRING,
  error_message STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  installments INT64,
  pix_qr_code STRING,
  boleto_barcode STRING,
  -- parâmetros
  first_name STRING, last_name STRING, phone STRING, email STRING, document STRING,
  event_source_url STRING, user_agent STRING, street_address STRING, city STRING,
  state STRING, postal_code STRING, country_code STRING, node_id STRING,
  -- utm/tracking
  utm_campaign STRING, utm_term STRING, utm_content STRING, utm_medium STRING, utm_source STRING,
  trk_src STRING, trk_cpg STRING, trk_adgp STRING, trk_ad STRING,
  -- relacionamentos
  project_name STRING, organization_email STRING, organization_document STRING, organization_name STRING,
  user_name STRING, user_email STRING, user_phone_number STRING,
  product_name STRING, recipient_id STRING,
  -- ofertas (pivotadas)
  offer_name_pp STRING,  offer_amount_pp STRING,
  offer_name_ob1 STRING, offer_amount_ob1 STRING,
  offer_name_ob2 STRING, offer_amount_ob2 STRING,
  offer_name_ob3 STRING, offer_amount_ob3 STRING,
  offer_name_ob4 STRING, offer_amount_ob4 STRING,
  -- metadados
  dt_insert TIMESTAMP
)
PARTITION BY DATE(dt_insert)
CLUSTER BY transaction_id
OPTIONS (require_partition_filter = TRUE);
```

### 16.2) DDL — Trusted (SCD2 só de status)

> Armazena **apenas** a história do `transaction_status` (enxuto e barato).

```sql
CREATE TABLE IF NOT EXISTS `trusted.transactions_scd2` (
  transaction_id STRING,
  transaction_status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_current BOOL,
  dt_insert TIMESTAMP,
  row_hash STRING
)
PARTITION BY DATE(updated_at)
CLUSTER BY transaction_id, transaction_status
OPTIONS (require_partition_filter = TRUE);
```

### 16.3) MERGE — Trusted SCD2 (por mudança de status)

```sql
MERGE `trusted.transactions_scd2` T
USING (
  SELECT
    transaction_id,
    transaction_status,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP() AS dt_insert,
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(transaction_status)))) AS row_hash
  FROM `staging.transactions_delta_dedup`
) S
ON T.transaction_id = S.transaction_id AND T.is_current = TRUE
WHEN MATCHED AND T.row_hash <> S.row_hash THEN
  UPDATE SET T.is_current = FALSE
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    transaction_id, transaction_status,
    created_at, updated_at, is_current, dt_insert, row_hash
  )
  VALUES (
    S.transaction_id, S.transaction_status,
    S.created_at, S.updated_at, TRUE, S.dt_insert, S.row_hash
  );
```

> **Observação:** se precisar refletir deleções do Postgres, inclua um marcador `is_deleted` no delta e trate com um `WHEN MATCHED AND S.is_deleted THEN ...` fechando o `valid_to`.

### 16.4) DDL — Refined (atributos vigentes SCD1)

> Mantém o **último valor** de cada `transaction_id` para colunas não versionadas (SCD1). Atualizado por **upsert** a partir do staging.

```sql
CREATE TABLE IF NOT EXISTS `refined.transactions_attributes_current` (
  transaction_id STRING,
  amount NUMERIC,
  amount_assiny NUMERIC,
  payment_type STRING,
  external_id STRING,
  error_message STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  installments INT64,
  pix_qr_code STRING,
  boleto_barcode STRING,
  -- parâmetros
  first_name STRING, last_name STRING, phone STRING, email STRING, document STRING,
  event_source_url STRING, user_agent STRING, street_address STRING, city STRING,
  state STRING, postal_code STRING, country_code STRING, node_id STRING,
  -- utm/tracking
  utm_campaign STRING, utm_term STRING, utm_content STRING, utm_medium STRING, utm_source STRING,
  trk_src STRING, trk_cpg STRING, trk_adgp STRING, trk_ad STRING,
  -- relacionamentos
  project_name STRING, organization_email STRING, organization_document STRING, organization_name STRING,
  user_name STRING, user_email STRING, user_phone_number STRING,
  product_name STRING, recipient_id STRING,
  -- ofertas (pivotadas)
  offer_name_pp STRING,  offer_amount_pp STRING,
  offer_name_ob1 STRING, offer_amount_ob1 STRING,
  offer_name_ob2 STRING, offer_amount_ob2 STRING,
  offer_name_ob3 STRING, offer_amount_ob3 STRING,
  offer_name_ob4 STRING, offer_amount_ob4 STRING
)
PARTITION BY DATE(updated_at)
CLUSTER BY transaction_id
OPTIONS (require_partition_filter = TRUE);
```

### 16.5) MERGE — Refined (upsert de atributos vigentes)

```sql
MERGE `refined.transactions_attributes_current` T
USING (
  SELECT * FROM `staging.transactions_delta_dedup`
) S
ON T.transaction_id = S.transaction_id
WHEN MATCHED THEN UPDATE SET
  amount = S.amount,
  amount_assiny = S.amount_assiny,
  payment_type = S.payment_type,
  external_id = S.external_id,
  error_message = S.error_message,
  created_at = S.created_at,
  updated_at = S.updated_at,
  installments = S.installments,
  pix_qr_code = S.pix_qr_code,
  boleto_barcode = S.boleto_barcode,
  first_name = S.first_name, last_name = S.last_name, phone = S.phone, email = S.email, document = S.document,
  event_source_url = S.event_source_url, user_agent = S.user_agent, street_address = S.street_address, city = S.city,
  state = S.state, postal_code = S.postal_code, country_code = S.country_code, node_id = S.node_id,
  utm_campaign = S.utm_campaign, utm_term = S.utm_term, utm_content = S.utm_content, utm_medium = S.utm_medium, utm_source = S.utm_source,
  trk_src = S.trk_src, trk_cpg = S.trk_cpg, trk_adgp = S.trk_adgp, trk_ad = S.trk_ad,
  project_name = S.project_name, organization_email = S.organization_email, organization_document = S.organization_document, organization_name = S.organization_name,
  user_name = S.user_name, user_email = S.user_email, user_phone_number = S.user_phone_number,
  product_name = S.product_name, recipient_id = S.recipient_id,
  offer_name_pp = S.offer_name_pp,  offer_amount_pp = S.offer_amount_pp,
  offer_name_ob1 = S.offer_name_ob1, offer_amount_ob1 = S.offer_amount_ob1,
  offer_name_ob2 = S.offer_name_ob2, offer_amount_ob2 = S.offer_amount_ob2,
  offer_name_ob3 = S.offer_name_ob3, offer_amount_ob3 = S.offer_amount_ob3,
  offer_name_ob4 = S.offer_name_ob4, offer_amount_ob4 = S.offer_amount_ob4
WHEN NOT MATCHED THEN INSERT ROW;
```

### 16.6) View/Tabela — Refined *transactions\_current* (status vigente + atributos vigentes)

> Junta o **status vigente** (trusted SCD2) aos **atributos vigentes** (SCD1) para consumo analítico.

```sql
CREATE OR REPLACE VIEW `refined.transactions_current` AS
SELECT
  a.*,  -- atributos vigentes
  s.transaction_status
FROM `refined.transactions_attributes_current` a
JOIN `trusted.transactions_scd2` s
  ON s.transaction_id = a.transaction_id
WHERE s.is_current = TRUE;
```

### 16.7) Tabela — Refined *transactions\_6m* (janela quente)

> Recriada diariamente (simples e barata):

```sql
CREATE OR REPLACE TABLE `refined.transactions_6m`
PARTITION BY DATE(updated_at)
CLUSTER BY transaction_id AS
SELECT *
FROM `refined.transactions_current`
WHERE updated_at >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 6 MONTH);
```

### 16.8) Orquestração (ordem das etapas)

1. **Extrair Postgres → staging.transa****watermark composto****ctions\_delta** usando  (`wm_ts`, `wm_id`) e janela de segurança de 2 dias.
2. **Deduplicar** partição do dia → `staging.transactions_delta_dedup`.
3. **MERGE** em `trusted.transactions_scd2` (SCD2 só status).
4. **MERGE** em `refined.transactions_attributes_current` (SCD1 upsert de atributos).
5. **Atualizar** `refined.transactions_current` (view) e **recriar** `refined.transactions_6m`.
6. **Atualizar watermark** em `control.watermarks`.
7. **Limpeza**: TTL/expiração da staging.

> Com essa composição, você tem **histórico enxuto e barato** (apenas status no trusted) e **tabela de consumo rápida** com todos os atributos vigentes no refined.

## 17) Query incremental no Postgres + índices recomendados

**Objetivo:** parar de varrer 6 meses no Postgres e puxar **somente o delta** por `updated_at` usando **watermark composto** (`wm_ts`, `wm_id`). As CTEs ficam restritas ao conjunto do delta para reduzir leitura/CPU.

```sql
WITH delta AS (
  SELECT t.transaction_id, t.updated_at
  FROM transaction t
  WHERE t.updated_at >  :wm_ts - INTERVAL '2 days'
     OR (t.updated_at = :wm_ts AND t.transaction_id > :wm_id)
),
principal_checkout AS (
  SELECT *
  FROM (
    SELECT
      tc.transaction_id,
      tc.checkout_data->>'name'                 AS name,
      (tc.checkout_data->>'amount')::numeric    AS amount,
      ROW_NUMBER() OVER (
        PARTITION BY tc.transaction_id
        ORDER BY tc.checkout_id
      ) AS rn
    FROM transaction_checkout tc
    JOIN delta d USING (transaction_id)
    WHERE (tc.checkout_data->>'checkoutType') <> 'ORDERBUMP'
  ) x
  WHERE rn = 1
),
order_bumps AS (
  SELECT
    tc.transaction_id,
    ROW_NUMBER() OVER (
      PARTITION BY tc.transaction_id
      ORDER BY tc.checkout_id
    ) AS rn,
    tc.checkout_data->>'name'               AS name,
    (tc.checkout_data->>'amount')::numeric  AS amount
  FROM transaction_checkout tc
  JOIN delta d USING (transaction_id)
  WHERE (tc.checkout_data->>'checkoutType') = 'ORDERBUMP'
),
order_bumps_pivot AS (
  SELECT transaction_id,
         MAX(CASE WHEN rn=1 THEN name   END) AS offer_name_ob1,
         MAX(CASE WHEN rn=1 THEN amount END) AS offer_amount_ob1,
         MAX(CASE WHEN rn=2 THEN name   END) AS offer_name_ob2,
         MAX(CASE WHEN rn=2 THEN amount END) AS offer_amount_ob2,
         MAX(CASE WHEN rn=3 THEN name   END) AS offer_name_ob3,
         MAX(CASE WHEN rn=3 THEN amount END) AS offer_amount_ob3,
         MAX(CASE WHEN rn=4 THEN name   END) AS offer_name_ob4,
         MAX(CASE WHEN rn=4 THEN amount END) AS offer_amount_ob4
  FROM order_bumps
  GROUP BY transaction_id
)
SELECT DISTINCT ON (t.transaction_id)
  t.transaction_id,
  t.status                AS transaction_status,
  t.amount_with_tax       AS amount,
  t.amount_assiny,
  t.payment_type,
  COALESCE(t.external_id, '')   AS external_id,
  COALESCE(t.error_message, '') AS error_message,
  t.created_at,
  t.updated_at,
  t.installments,
  COALESCE(t.pix_qr_code, '')    AS pix_qr_code,
  COALESCE(t.boleto_barcode, '') AS boleto_barcode,
  -- JSONB Parameters (mantidos nativos; formatação ocorre no BQ)
  COALESCE(parameters->>'firstName', '')      AS first_name,
  COALESCE(parameters->>'lastName', '')       AS last_name,
  COALESCE(parameters->>'phone', '')          AS phone,
  COALESCE(parameters->>'email', '')          AS email,
  COALESCE(parameters->>'document', '')       AS document,
  COALESCE(parameters->>'eventSourceUrl', '') AS event_source_url,
  COALESCE(parameters->>'userAgent', '')      AS user_agent,
  COALESCE(parameters->>'streetAddress', '')  AS street_address,
  COALESCE(parameters->>'city', '')           AS city,
  COALESCE(parameters->>'state', '')          AS state,
  COALESCE(parameters->>'postalCode', '')     AS postal_code,
  COALESCE(parameters->>'countryCode', '')    AS country_code,
  COALESCE(parameters->>'nodeId', '')         AS node_id,
  -- UTM/Tracking
  COALESCE(parameters->'query'->>'utm_campaign', '') AS utm_campaign,
  COALESCE(parameters->'query'->>'utm_term', '')     AS utm_term,
  COALESCE(parameters->'query'->>'utm_content', '')  AS utm_content,
  COALESCE(parameters->'query'->>'utm_medium', '')   AS utm_medium,
  COALESCE(parameters->'query'->>'utm_source', '')   AS utm_source,
  COALESCE(parameters->'query'->>'trk_src', '')      AS trk_src,
  COALESCE(parameters->'query'->>'trk_cpg', '')      AS trk_cpg,
  COALESCE(parameters->'query'->>'trk_adgp', '')     AS trk_adgp,
  COALESCE(parameters->'query'->>'trk_ad', '')       AS trk_ad,
  -- Relacionamentos
  p.name   AS project_name,
  o.email  AS organization_email,
  o.document AS organization_document,
  o.name  AS organization_name,
  COALESCE(u.name, '')          AS user_name,
  COALESCE(u.email, '')         AS user_email,
  COALESCE(u.phone_number, '')  AS user_phone_number,
  COALESCE(pr.name, '')         AS product_name,
  COALESCE(uw.wallet_id, '')    AS recipient_id,
  -- Ofertas
  COALESCE(pc.name, '')         AS offer_name_pp,
  COALESCE(pc.amount, NULL)     AS offer_amount_pp,
  COALESCE(ob.offer_name_ob1, '')   AS offer_name_ob1,
  COALESCE(ob.offer_amount_ob1, NULL) AS offer_amount_ob1,
  COALESCE(ob.offer_name_ob2, '')   AS offer_name_ob2,
  COALESCE(ob.offer_amount_ob2, NULL) AS offer_amount_ob2,
  COALESCE(ob.offer_name_ob3, '')   AS offer_name_ob3,
  COALESCE(ob.offer_amount_ob3, NULL) AS offer_amount_ob3,
  COALESCE(ob.offer_name_ob4, '')   AS offer_name_ob4,
  COALESCE(ob.offer_amount_ob4, NULL) AS offer_amount_ob4
FROM transaction t
JOIN delta d USING (transaction_id)
JOIN project       p  ON p.id  = t.project_id
JOIN organization  o  ON o.id  = p.organization_id
JOIN "user"       u  ON o.user_id = u.id
JOIN product       pr ON pr.id  = t.product_id
LEFT JOIN user_wallet        uw ON uw.user_id = u.id AND uw.project_id = p.id
LEFT JOIN principal_checkout pc ON pc.transaction_id = t.transaction_id
LEFT JOIN order_bumps_pivot ob  ON ob.transaction_id = t.transaction_id
ORDER BY t.transaction_id, t.updated_at DESC;
```

**Índices recomendados (Postgres):**

```sql
-- Delta por updated_at + desempate por id
CREATE INDEX IF NOT EXISTS ix_transaction_updated_id
  ON transaction (updated_at, transaction_id);

-- Suporte a ordenação e partição por checkout_id nas CTEs
CREATE INDEX IF NOT EXISTS ix_tc_txid_ckid
  ON transaction_checkout (transaction_id, checkout_id);

-- Filtro por tipo no JSONB (expressão)
CREATE INDEX IF NOT EXISTS ix_tc_checkouttype
  ON transaction_checkout ((checkout_data->>'checkoutType'));

-- (Opcional) Se houver outras consultas por campos do JSONB
-- CREATE INDEX ix_tc_jsonb ON transaction_checkout USING gin (checkout_data jsonb_path_ops);
```

**Observações de performance:**

- Trazer **tipos nativos** (TIMESTAMP/NUMERIC) do Postgres e formatar no BigQuery.
- Se o Postgres estiver sob carga, considerar **enviar ofertas “longas”** (não pivotadas) e **pivotar no BigQuery**.
- No BigQuery, mantenha `staging` e `refined` \*\*clusterizadas por \*\*`` para acelerar *joins*.

