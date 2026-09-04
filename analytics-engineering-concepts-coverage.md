# Analytics Engineering Concepts — Coverage Map

**Scope:** the topics to be covered for the *Analytics Engineering Concepts* band (15%, High), grounded in the FpML → Canonical → Semantic model, expressed on an **AWS + Snowflake + dbt** platform, including how each consumer actually reaches the data.

> **Source note.** This is built on `claude/fpml-canonical-semantic-datamodelling-guide.md` — the only FpML doc in the project. If `claude/fpml-schema.md` is a separate file you have locally, add it to the project and this document can be re-cut against it.
>
> **Platform note.** The source guide implements on Databricks + Delta with a Snowflake variant. This document restates the same logical model on the AWS/Snowflake/dbt stack the scorecard names. The logical model does not change — that is itself one of the points to be able to make.

---

## Contents

1. [The three concepts under examination](#1-the-three-concepts-under-examination)
2. [The platform, layer by layer](#2-the-platform-layer-by-layer)
3. [Topic coverage](#3-topic-coverage)
4. [Data access — how each consumer reaches the data](#4-data-access--how-each-consumer-reaches-the-data)
5. [Worked path: one number, end to end](#5-worked-path-one-number-end-to-end)
6. [Platform choice — the honest comparison](#6-platform-choice--the-honest-comparison)
7. [Coverage checklist](#7-coverage-checklist)

---

## 1. The three concepts under examination

The scorecard names exactly three. Everything below is in service of these.

### 1.1 Analytics-ready models

> A model is analytics-ready when a business user can query it directly and get the right answer **without knowing anything about the source system**.

The nine tests:

| Test | In this model |
|---|---|
| Grain declared before columns | *One row per confirmed cash movement, identified by `settlement_id`, attributable to exactly one cashflow of exactly one trade version, in one currency, hitting one account* |
| Conformed dimensions | One `dim_counterparty` serving `fct_settled_cash`, `fct_cashflow`, `fct_valuation`, `fct_trade` |
| Business vocabulary | `settled_amount`, not `stl_amt_val` |
| Additivity classified | `signed_settled_amount` fully additive; cash position semi-additive (LAST over time); `fx_rate_to_base` never summed |
| Signs and units stored | `direction_sign` applied in the model, so no BI tool re-derives direction |
| No null foreign keys | Every dimension carries `Unknown (-1)`; `fail_reason_sk = -1` when no fail |
| History where reportable | SCD2 on counterparty, legal entity, product |
| Tested and documented | Uniqueness on grain, referential integrity, freshness, and reconciliation to the payment messages |
| Shaped for the read pattern | Clustered/partitioned on `(settlement_date, currency_code)` — the MTD filter |

**The trap:** calling analytics-ready "cleaned data". Cleaning is Silver. Analytics-ready is *meaning* — grain, conformance and definition.

### 1.2 Reusable data products

> A data product is a model with an **owner**, a **contract**, an **SLA** and a **named consumer**. Anything less is a report someone saved as a table.

| Property | Concrete form here |
|---|---|
| Discoverable | Registered in Glue Data Catalog / SageMaker Catalog with a business glossary term |
| Addressable | Stable identifier: `GMB.GOLD.FCT_SETTLED_CASH` (Snowflake) or `gold.fct_settled_cash` (Glue) |
| Self-describing | Grain statement, column definitions, units, sign convention, currency handling — in `schema.yml`, surfaced in dbt docs and the catalog |
| Trustworthy | Published test results, freshness SLO, reconciliation status |
| Interoperable | Iceberg format + conformed surrogate keys, so any engine joins it the same way |
| Secure | Lake Formation grants / Snowflake RBAC at the product boundary |
| Owned | Named team, support route, deprecation policy, model version |

**The boundary to name out loud:** the **contract sits at the canonical (Silver) layer**. Source moves on ISDA's release cycle; canonical is the stable promise owned by architecture; the semantic layer above it is free to evolve with consumers.

### 1.3 The analytics engineering mindset

> Software engineering discipline applied to analytics, where the deliverable is an **agreed business definition**, not a pipeline that ran green.

- Transformations are code — version controlled, reviewed, CI-tested, promoted through environments.
- ELT, not ETL — land raw with full fidelity, transform where the logic is visible and testable.
- Define once, reference everywhere — `ref()` and the metric layer, never copy-paste.
- Tests ship with the model, not as a later QA phase.
- Idempotent and reproducible — a rerun or backfill gives the same answer.
- Lineage and documentation are product: a consumer traces a number back to a source message.
- Trust is the metric. "Pipelines are green" is the vanity one.

---

## 2. The platform, layer by layer

The logical model is unchanged from the guide. Only the physical mechanics move.

```
  SOURCES
    FpML 5.13 trade messages          ──┐
    ISO 20022 camt.054 / camt.053     ──┤──►  Kinesis / MSK  ──►  S3 raw zone
    Reference & static data           ──┘                          (partitioned by date, source)

  ┌────────────────────────────────────────────────────────────────────────┐
  │ BRONZE — raw, immutable, source-shaped.  "The tape."                   │
  │   S3 + Iceberg  ·  glue_catalog.bronze.fpml_message                    │
  │   or  Snowflake GMB.BRONZE.BRONZE_FPML_MESSAGE (RAW_PAYLOAD VARIANT)   │
  │   Owner: platform engineering  ·  Retention 7–10y  ·  Never deleted    │
  └───────────────────────────────┬────────────────────────────────────────┘
                                  ▼   dbt staging models
  ┌────────────────────────────────────────────────────────────────────────┐
  │ SILVER — the CANONICAL model.  Where source volatility stops.          │
  │   Parsed, validated, deduplicated, bitemporal, business-key resolved   │
  │   CanonicalTrade · TradeLeg · Product · Party · TradePartyRole ·       │
  │   Account · Cashflow · Payment · Settlement · SettlementInstruction ·  │
  │   TradeEvent · Valuation                                               │
  │   Owner: Enterprise Data Architecture  ·  THE CONTRACT BOUNDARY        │
  └───────────────────────────────┬────────────────────────────────────────┘
                                  ▼   dbt intermediate + marts
  ┌────────────────────────────────────────────────────────────────────────┐
  │ GOLD — the SEMANTIC model.  Where consumer volatility stops.           │
  │   fct_trade · fct_cashflow · fct_settled_cash · fct_valuation ·        │
  │   fct_trade_event  +  conformed dims (date, counterparty, legal_entity,│
  │   product, currency, account, settlement_status, book)                 │
  │   Owner: data product teams  ·  Rebuildable from Silver at any time    │
  └───────────────────────────────┬────────────────────────────────────────┘
                                  ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │ METRIC LAYER — one definition, resolved by every consumer              │
  │   mtd_settled_cash_usd · ltd_settled_cash_usd ·                        │
  │   outstanding_settlement_amount · failed_settlement_amount ·           │
  │   cash_position_by_currency                                            │
  └───────────────────────────────┬────────────────────────────────────────┘
                                  ▼
      Finance   ·   Operations   ·   Risk   ·   Regulatory   ·   Data Science
```

### 2.1 Where each layer physically lives

| Layer | AWS-native option | Snowflake option | Who transforms it |
|---|---|---|---|
| Raw landing | S3 raw zone, partitioned by ingest date | S3 + external stage | Kinesis Firehose / Glue Streaming |
| Bronze | Iceberg table in S3, Glue Data Catalog | `BRONZE.BRONZE_FPML_MESSAGE`, `RAW_PAYLOAD VARIANT` | Glue job / Snowpipe |
| Silver (canonical) | Iceberg tables, Glue Catalog | `SILVER.*` tables, clustered | **dbt** |
| Gold (semantic) | Iceberg tables, Glue Catalog | `GOLD.*` tables, clustered | **dbt** |
| Metrics | dbt semantic layer / Snowflake semantic views | same | **dbt** |
| Catalog & governance | Glue Data Catalog + Lake Formation + SageMaker Catalog | Snowflake RBAC + tags + ACCESS_HISTORY | shared |

**The two sentences worth memorising:** *"Canonical is Silver — it is where source volatility stops. Semantic is Gold — it is where consumer volatility stops. Bronze is the immutable evidence that lets me rebuild both."*

---

## 3. Topic coverage

### 3.1 Layering and model boundaries

Cover:

- Medallion as **data quality states, not team boundaries** — bronze is platform engineering, gold is analytics engineering, silver is shared (engineering owns mechanics, analytics engineering owns definitions).
- The dbt layering convention that enforces it:
  - **staging** — 1:1 with source, rename/cast/clean only, no joins, no business logic
  - **intermediate** — business logic, joins, fan-out control, not exposed to consumers
  - **marts** — the only thing consumers see; nothing enters without a named owner and consumer
- Why the canonical model exists even though FpML *is* a standard: a standard optimises for message interchange, not for querying, and omits what the business needs derived (counterparty perspective, trade status, settled amounts).

### 3.2 Analytics-ready modelling

Cover, with the settled-cash example in each:

- **Grain selection.** Why one-row-per-payment is wrong twice: partial settlements make one payment several settlement facts; netting makes one payment cover several trades. The grain is one row per confirmed cash movement (`settlement_id`), which preserves trade attribution through netting.
- **Conformed dimensions and drill-across** — settled cash *and* MTM *and* trade count for the same counterparty in one query.
- **Surrogate keys and SCD2** — `counterparty_sk` is date-effective; joining on the natural key silently applies today's rating to last year's trades.
- **Additivity** — fully additive (`signed_settled_amount`), semi-additive (cash position: LAST over time, SUM over other dimensions), non-additive (`fx_rate_to_base`).
- **Signed amounts and dual currency** — native amount, base-currency equivalent, *and* the rate, its date and its source, because audit asks.
- **Null handling** — `Unknown (-1)` members so inner joins never silently drop rows.
- **Bitemporality** — the difference between when something was true and when we knew it; how late-arriving settlements and amendments are handled without rewriting history.

### 3.3 Reusable data products

Cover:

- The seven properties (§1.2) and the reuse test: *would a second consumer use it unchanged?*
- **Contract contents** — structure (schema, types, nullability, keys), semantics (grain statement, column definitions, units, sign convention), quality (required tests, freshness SLO, reconciliation rule), lifecycle (version, change policy, deprecation window, owner).
- **Enforcement in dbt** — `contract: {enforced: true}` on the mart model, plus model `versions` so v1 and v2 coexist during migration.
- **Breaking-change protocol** — enumerate consumers from `exposures` and lineage, add rather than change where possible, publish v2 alongside v1 with a sunset date, migrate, remove.
- **Publishing on AWS** — a Gold Iceberg table registered in the catalog, published as an asset in SageMaker Catalog, subscribed to by a consuming project through an approval workflow. That subscription *is* the contract handshake.

### 3.4 Transformation with dbt

**Project structure** (name the layout; it is a common ask):

```
models/
  staging/
    fpml/        stg_fpml__trade.sql, stg_fpml__party.sql,
                 stg_fpml__cashflow.sql, stg_fpml__settlement_instruction.sql,
                 stg_fpml__trade_event.sql
                 _fpml__sources.yml      # + freshness thresholds
                 _fpml__models.yml       # + tests, descriptions
    iso20022/    stg_iso20022__payment.sql, stg_iso20022__settlement.sql,
                 stg_iso20022__statement_entry.sql
  intermediate/
    int_trade__versioned.sql              # bitemporal version resolution
    int_trade__party_role_pivoted.sql     # counterparty perspective derivation
    int_cashflow__expected.sql
    int_payment__cashflow_allocation.sql  # netting: N:M fan-out control
    int_settlement__allocated.sql         # partial settlements
    int_cashflow__settlement_status.sql   # derived status
  marts/
    finance/     fct_settled_cash.sql, fct_cashflow.sql,
                 dim_counterparty.sql, dim_legal_entity.sql, dim_account.sql,
                 dim_currency.sql, dim_date.sql, dim_settlement_status.sql
    risk/        fct_valuation.sql
    shared/      fct_trade.sql, fct_trade_event.sql, dim_product.sql
snapshots/
    snap_counterparty.sql, snap_legal_entity.sql, snap_product.sql
macros/
    signed_amount.sql, to_base_currency.sql, business_days_between.sql
tests/
    assert_signed_amount_consistent.sql
    assert_failed_settlement_has_zero_amount.sql
    assert_allocations_not_exceeding_cashflow.sql
    assert_gold_reconciles_to_statement.sql
```

**Materialisations — and the three questions that decide** (how big, how late does data arrive, how often is it read):

| Model | Materialisation | Why |
|---|---|---|
| `stg_*` | `view` | Thin renames; freshness beats read speed |
| `int_*` | `ephemeral` or `view` | DRY without creating objects consumers can find |
| `fct_settled_cash` | `incremental`, `merge`, `unique_key='settlement_id'` | Large, and settlements arrive late — merge updates in place rather than duplicating |
| `dim_counterparty` | `table` from a `snapshot` | SCD2 history built by dbt snapshots |
| `dim_date` | `table` | Small, static |

**Incremental with late arrivals** — the pattern to be able to describe: filter on `ingestion_timestamp >= (select max(ingestion_timestamp) from {{ this }}) - interval '7 days'` inside `is_incremental()`, so a settlement confirmed a week after its value date still lands, and `merge` on the business key prevents the duplicate.

**Tests — four layers:**

1. **Structural** — `unique` on `settlement_id`, `not_null` on every FK, `relationships` to each dimension, `accepted_values` on `reconciliation_status`, `confirmation_source`, `payment_direction`.
2. **Freshness** — `source freshness` on the FpML and camt feeds, against the SLO.
3. **Business rules** (singular tests) — `signed_settled_amount = settled_amount * direction_sign`; a `FAILED` settlement has `settled_amount = 0`; the sum of allocations against a cashflow never exceeds its expected amount.
4. **Reconciliation** — total settled cash in Gold ties, per `settlement_date` and currency, to the sum of camt.053 statement entries. *This is the one that catches a wrong pipeline rather than a broken one.*

**Deployment** — the question is usually "how does a change get from your laptop to production":

- Feature branch → PR → CI runs `dbt build --select state:modified+ --defer --state prod` against a dev schema, so only affected models and their children run.
- Contract and unit tests fail the build, not a dashboard.
- Merge → docs and lineage regenerate → scheduled production run.
- Environments separated by target (dev/CI/prod), never by editing a production table by hand.
- On Snowflake, zero-copy clone gives CI a full-size dev environment in seconds.

**Adapters** — `dbt-snowflake` for the warehouse path; `dbt-athena` or `dbt-glue` for the Iceberg-on-S3 path. Same project layout, different target — a useful thing to say, because it shows the modelling is platform-independent.

### 3.5 The semantic and metric layer

Cover:

- What it is: a single definition of entities, joins and metrics that every consumer resolves against, so *MTD settled cash* means the same thing in a dashboard, a notebook and an API.
- The **metric definition record**: name, owner, grain, formula, allowed filters and dimensions, additivity class, refresh cadence, certification tier.
- Worked definitions from the guide:

```yaml
metrics:
  - name: mtd_settled_cash_usd
    source: fct_settled_cash
    measure: SUM(signed_settled_amount_base_ccy)
    filters: [settlement_status.counts_as_settled = true, is_reversal = false]
    time_dimension: settlement_date
    grain: fiscal_month_to_date
    dimensions: [legal_entity, counterparty, currency, product, account, book]
    additivity: fully_additive

  - name: outstanding_settlement_amount
    source: vw_cashflow_settlement_status
    measure: SUM(unsettled_amount * direction_sign * fx_rate_to_base)
    filters: [derived_settlement_status IN ('EXPECTED','PARTIALLY_SETTLED','OVERDUE','FAILED'), is_current = true]
    time_dimension: value_date
    additivity: fully_additive

  - name: cash_position_by_currency
    source: fct_settled_cash
    measure: SUM(signed_settled_amount)
    time_dimension: settlement_date
    additivity: semi_additive        # LAST over time, SUM over other dimensions
```

- Why not in the BI tool: unversioned, untested, invisible to lineage, unavailable outside that tool, and it multiplies per tool.
- Implementation options on this stack: dbt Semantic Layer, Snowflake semantic views, or governed secure views in `GOLD` — pick one and say why.

### 3.6 Data quality, testing and reconciliation

- The four test layers above, plus **Glue Data Quality (DQDL)** rules on the AWS path for source-side checks before transformation.
- Reconciliation as a first-class control: `settled_amount` vs the bank statement entry, break statuses (`MATCHED`, `UNMATCHED`, `BREAK`, `AGED_BREAK`), and time-to-close as the health metric.
- Publishing quality signals *with* the product — the catalog entry shows last test run, freshness, and reconciliation status, so trust is visible before use rather than discovered after.

### 3.7 Lineage, catalog and governance

- **Column-level lineage** from `manifest.json` / dbt docs, joined to catalog lineage so a number traces back through `fct_settled_cash` → `int_settlement__allocated` → `stg_iso20022__settlement` → the camt.054 message in bronze.
- **Glue Data Catalog** as the technical metastore; **SageMaker Catalog** (on DataZone lineage) as the business catalog with glossary, domains, projects and publish/subscribe.
- **Ownership model** — bronze: platform engineering; silver/canonical: enterprise data architecture; gold products: the owning data product team.
- Change governance — mapping review, versioning, deprecation with a sunset window.

### 3.8 Performance and cost

- **Snowflake** — micro-partition pruning first; cluster `fct_settled_cash` on `(settlement_date, currency_code)` because that is the MTD filter; warehouse per workload so cost is attributable; auto-suspend and resource monitors; spilling as the signal a warehouse is undersized.
- **Iceberg/Athena** — Athena bills on **data scanned**, so columnar format, partitioning and compression are cost controls; partition projection avoids partition-metadata explosion on date layouts; compaction and snapshot expiry keep tables fast.
- **Model-side** — incremental over full refresh; avoid `SELECT *` on columnar storage; fix a fan-out join rather than adding `DISTINCT`.

---

## 4. Data access — how each consumer reaches the data

### 4.1 Access paths

| Consumer | Interface | Object accessed | Permission mechanism |
|---|---|---|---|
| Finance analyst | BI tool → Snowflake | Semantic view + `mtd_settled_cash_usd` | Snowflake RBAC role, row access policy by legal entity, masking on account number |
| Operations | Ops dashboard | `vw_cashflow_settlement_status` | RBAC role scoped to their book/entity |
| Risk quant | Spark / Python notebook | Gold Iceberg tables on S3 | IAM role assumed via **STS**, Lake Formation table + column grants |
| Regulatory reporting | Athena SQL | Gold Iceberg + bronze evidence | Lake Formation LF-tag policy, read-only |
| Data science | SageMaker Unified Studio project | Subscribed data product | SageMaker Catalog subscription approval → project role |
| Partner / external | Snowflake secure share, or Iceberg REST catalog | Curated share only | Secure Data Sharing / reader account; no data copied |
| Audit | Direct bronze read | `bronze_fpml_message` | Read-only IAM role, S3 Object Lock, no delete path |

### 4.2 The permission stack

Layered, coarse to fine — describe it in this order:

1. **Network** — VPC endpoints and PrivateLink; no public paths to the data.
2. **Identity** — IAM roles rather than users; **STS `AssumeRole`** issues short-lived credentials, which is how an analytics account reads a source account's data without long-lived keys; SSO/SCIM into Snowflake.
3. **Coarse-grained** — S3 bucket and prefix policies; Snowflake database and schema grants.
4. **Fine-grained** — **Lake Formation** for table, column, row and cell permissions plus LF-tag-based access control across the catalog; **Snowflake** masking policies and row access policies, applied through secure views.
5. **Product-level** — catalog publish/subscribe with an approval workflow, so access is granted to a *product* with an owner rather than to a table.
6. **Audit** — CloudTrail, Lake Formation access logs, Snowflake `ACCESS_HISTORY`, and lineage to prove where a number came from.

**The sentence that ties it together:** *access is granted at the data product boundary, not at the table — the product has an owner who approves the subscription, and the fine-grained policy is attached to the product's contract.*

### 4.3 Cross-engine access — one copy, many engines

This is the part worth being crisp on, because it is where the AWS and Snowflake halves of the stack meet.

- Gold tables are written as **Apache Iceberg** in S3 and registered in the **Glue Data Catalog**.
- **Athena**, **EMR/Glue Spark** and **Redshift** read them directly through the Glue Catalog.
- **Snowflake** reads the same tables as **Iceberg tables**, via an *external volume* pointing at the S3 location and a *catalog integration* pointing at Glue. Snowflake can either read externally-managed Iceberg tables (Glue owns the metadata) or manage its own Iceberg tables and write Iceberg back to S3 for the AWS engines to read.
- The trade-off to state: **externally-managed** keeps a single source of truth in Glue but gives up some Snowflake write and optimisation features; **Snowflake-managed** gives full functionality but makes Snowflake the writer. Pick per table by who writes it.
- **Snowflake Secure Data Sharing** for Snowflake-to-Snowflake consumers — no copy, no pipeline, revocable.
- **Athena federated queries** for the sources that never land in the lake.

---

## 5. Worked path: one number, end to end

*"Show me how a Finance analyst gets MTD settled cash."* — the single most useful thing to be able to narrate.

1. A **camt.054** credit notification arrives from the settlement agent, lands on Kinesis, and is written verbatim to the S3 raw zone.
2. It is loaded to **bronze** unchanged, with envelope metadata and a payload hash — immutable, retained 7–10 years, the evidence for any later rebuild.
3. **dbt staging** parses it into `stg_iso20022__settlement`: rename, cast, clean. No business logic.
4. **dbt intermediate** does the hard part — `int_payment__cashflow_allocation` splits a netted payment across the cashflows it covers; `int_settlement__allocated` handles the partial; `int_cashflow__settlement_status` derives the resulting status.
5. **`fct_settled_cash`** materialises incrementally, merged on `settlement_id`, with `signed_settled_amount` computed once and the FX rate, its date and source stored alongside.
6. **Tests** run in the same build: uniqueness on the grain, referential integrity to every dimension, the business-rule assertions, and the reconciliation against camt.053 statement entries.
7. **`mtd_settled_cash_usd`** is defined once in the metric layer — measure, filters, time dimension, additivity, allowed dimensions.
8. The analyst opens their BI tool. Their Snowflake role resolves the semantic view; a **row access policy** limits them to their legal entities; a **masking policy** hides full account numbers.
9. They slice by counterparty and currency. Every consumer slicing the same metric gets the same number, because there is only one definition to resolve.
10. If they question it, **lineage** takes them from the figure back through the mart, the intermediate models and staging to the exact camt.054 message in bronze — and the catalog shows the last reconciliation status beside the product.

---

## 6. Platform choice — the honest comparison

Expect "why would you put Gold in Snowflake rather than on Iceberg?" Have a position, and the conditions that would change it.

| | Snowflake-centric Gold | Iceberg-on-S3 Gold | Hybrid (recommended default) |
|---|---|---|---|
| Query performance for BI | Strongest | Good, engine-dependent | Snowflake for BI |
| Engine neutrality | Weakest | Strongest | Iceberg where many engines read |
| Governance granularity | Masking + row access policies, mature | Lake Formation cell-level + LF-tags | Both, at their own boundaries |
| Cost model | Credits per warehouse-second; predictable, can be expensive at idle | Storage cheap; Athena billed on scan | Right tool per workload |
| ML / Spark access | Requires connector or export | Native | Iceberg for Risk and Data Science |
| Operational burden | Low | Compaction, snapshot expiry, catalog hygiene | Highest, but bounded |

**A defensible position:** canonical Silver as Iceberg in S3 so every engine reads one copy under Lake Formation governance; Gold materialised into Snowflake for BI and Finance where query performance and policy granularity matter most; Risk and Data Science read the Iceberg Gold tables directly. dbt builds both from the same project with two targets.

---

## 7. Coverage checklist

Tick when you can deliver it in ninety seconds — **example → options → trade-off → governance**.

**Analytics-ready models**

- [ ] The definition, in twenty seconds
- [ ] Grain statement for `fct_settled_cash`, and why one-row-per-payment is wrong
- [ ] Conformed dimensions and drill-across
- [ ] Additivity: fully / semi / non, with one example of each
- [ ] SCD2: where, why, and what it costs
- [ ] Null FK handling and the `Unknown (-1)` member

**Reusable data products**

- [ ] The definition, in twenty seconds
- [ ] The four parts of a contract
- [ ] Where the contract boundary sits, and why there
- [ ] Breaking-change protocol, including how you enumerate consumers
- [ ] Publish/subscribe as the access-granting mechanism

**Analytics engineering mindset**

- [ ] The definition, in twenty seconds
- [ ] How a change reaches production
- [ ] The four test layers, and which one catches a *wrong* pipeline
- [ ] Why the deliverable is the definition, not the pipeline

**Platform and access**

- [ ] The layer diagram, drawn in five minutes while narrating
- [ ] dbt project structure and materialisation choices with reasons
- [ ] Incremental merge with a late-arrival window
- [ ] The permission stack, coarse to fine
- [ ] Cross-engine Iceberg access, including the Snowflake external volume trade-off
- [ ] The end-to-end path for one number

---

*Companion to `claude/fpml-canonical-semantic-datamodelling-guide.md`. Metric definitions, the settled-cash grain and column set are taken from that guide; the platform mapping and access model are restated here for an AWS + Snowflake + dbt stack.*
