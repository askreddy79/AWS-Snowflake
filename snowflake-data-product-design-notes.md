# Snowflake Design Notes — Data Mesh / Data Product Platform

**Context:** IBM MQ (MQD1 test, MQBA prod) → Confluent self-managed IBM MQ source connector → Kafka (Raw / Curated / DLQ topics, Schema Registry enforced) → AWS Lambda (raw → processed → curated, DynamoDB for state and reference data) → Confluent managed Snowflake Sink connector → Snowflake (dbt layers: landing, staging, base, access) orchestrated by MWAA.

Scope of this document: the Snowflake side only — storage, compute, query performance, cost observability, and access control (RBAC + ABAC) — framed for a mesh where each data product is domain-owned.

---

## 1. Account and object model (the mesh foundation)

Everything downstream — cost attribution, security, performance isolation — depends on getting this layout right first.

**Account strategy**

| Environment | Snowflake account | Fed from |
|---|---|---|
| Dev | `ORG-DP_DEV` | Synthetic / masked clone |
| Test | `ORG-DP_TEST` | MQD1 via Kafka test cluster |
| Prod | `ORG-DP_PROD` | MQBA via Kafka prod cluster |

Separate accounts (not just separate databases) for prod. In banking/telecom this simplifies the audit story, keeps a runaway dev query off the prod compute pool, and lets you apply different network policies and MFA rules per environment.

**Database per data product, schema per dbt layer**

```
DP_<DOMAIN>_<PRODUCT>              -- e.g. DP_PAYMENTS_SETTLEMENT
├── LANDING     -- Kafka sink target. Append-only, transient, short retention.
├── STAGING     -- Dedup, typing, flattening. Transient. dbt.
├── BASE        -- Conformed, historised entities. Permanent. dbt.
└── ACCESS      -- Output port: secure views + serving tables. dbt.
```

Key mesh rules to enforce as platform policy:

- **The output port is the `ACCESS` schema and the Kafka curated topic — nothing else.** No consumer ever gets `SELECT` on `LANDING`, `STAGING`, or `BASE`. This is what lets the domain refactor internals without breaking consumers.
- Each data product carries a **contract**: schema (from Schema Registry, mirrored as a dbt contract on `ACCESS` models), freshness SLO, retention, and sensitivity classification.
- Cross-domain consumption goes through **secure views / Snowflake shares / Horizon listings**, never a copy. Copying is how a mesh quietly becomes a swamp.
- Tag every database, schema, and warehouse with `data_product`, `domain`, `cost_centre`, `environment`, `criticality`. These tags do double duty: cost attribution (§5) and ABAC (§6).

---

## 2. Ingestion into Snowflake

### 2.1 Connector version matters a lot here

If you are on the Confluent managed Snowflake Sink connector backed by Kafka connector v3 / Snowpipe Streaming *classic*, plan a migration. Version 4.0 (GA April 2026) is a rewrite on Snowflake's high-performance Snowpipe Streaming architecture: up to ~10 GB/s per table, 5–10 s end-to-end ingest-to-query latency, exactly-once and ordered delivery, and flat throughput-based (per-GB) pricing rather than warehouse credits. Snowflake has published a deprecation notice for the classic architecture.

Three v4 features are directly relevant to this design:

- **Server-side schema validation and evolution via PIPE objects.** Moves validation off the connector. Pairs naturally with your Schema Registry — Registry enforces at the topic, PIPE enforces at the table.
- **In-flight transformation using COPY syntax inside the PIPE.** Cheap place to do cast/rename/flatten work that would otherwise burn warehouse credits in `STAGING`.
- **Pre-clustering during ingestion** when the target table has clustering keys defined. This is the single biggest performance lever for large landing tables — data arrives already ordered, so automatic clustering has far less to do.

**Error handling:** use v4 **Error Tables** for server-side validation failures and the connector **DLQ** for converter / client-side validation failures. Reconcile those against your existing Kafka DLQ topics so you have one operational view of "messages that didn't make it," not two.

### 2.2 Landing table design

- **Structured columns over `VARIANT`** wherever the schema is known. VARIANT is convenient but pruning and scan performance on typed columns is materially better. Let the connector schematize.
- **Keep `RECORD_METADATA`.** Topic, partition, offset, and Kafka timestamp are your idempotency key, your replay anchor, and your lineage evidence for audit.
- Carry a **`correlation_id` end-to-end** — MQ message ID → Kafka header → DynamoDB state row → Snowflake column. Without it, "where did this message go?" becomes a three-team investigation.
- **Watch file sizing.** Aggressive flush settings produce many tiny micro-partitions, which destroys pruning on the landing table. Tune connector buffer/flush to trade a little latency for larger, better-pruned partitions, and monitor average partition size.

### 2.3 Late and out-of-order messages

MQ redelivery plus Kafka retries means duplicates and out-of-order arrival are normal, not exceptional. Standard pattern in `STAGING`:

```sql
select *
from {{ ref('landing_events') }}
qualify row_number() over (
          partition by business_key
          order by mq_put_time desc, kafka_offset desc
       ) = 1
```

Combine with the DynamoDB idempotency state so replays are safe. Make replay a **documented, tested runbook**, not a heroic one-off — for a regulated data product you will need it.

---

## 3. Storage optimization

### 3.1 Table types and retention — the cheapest win available

| Layer | Table type | Time Travel | Rationale |
|---|---|---|---|
| `LANDING` | Transient | 0–1 day | Fully replayable from Kafka. Paying Fail-safe on it is pure waste. |
| `STAGING` | Transient | 0–1 day | Fully rebuildable by dbt. |
| `BASE` | Permanent | 7 days | Recovery point for the historised record. |
| `ACCESS` | Permanent | 1–7 days | Rebuildable, but protects consumer-facing state. |

Transient tables have **no Fail-safe**, which removes 7 days of hidden storage charge on your two highest-churn layers. For a high-volume telecom/banking feed this is often the largest single storage saving available and costs you nothing you actually need.

### 3.2 Clustering — use sparingly and measure

- Only consider clustering keys on tables **above roughly 1 TB** with genuinely selective, repeated predicates.
- Order keys **low cardinality first**: `(event_date, entity_bucket)`, not `(transaction_id, ...)`.
- Do **not** put a high-cardinality column in the key. A common failure mode: a transactions table clustered on `(customer_id, txn_date, txn_type)` where maintenance credits balloon and removing one column cuts the cost dramatically with almost no query impact.
- Monitor `AUTOMATIC_CLUSTERING_HISTORY` and `SYSTEM$CLUSTERING_INFORMATION`. If clustering depth is stable and low, you are over-clustering; if credits are climbing month on month, revisit the key.
- Where v4 pre-clustering is available, clustered ingestion often removes the need for aggressive automatic reclustering entirely.

### 3.3 Long-term history

For the multi-year retention that banking and telecom regulation tends to demand, move cold `BASE` history to **Iceberg tables on S3**. Storage moves to your own bucket at S3 rates, the data stays queryable from Snowflake, and it remains readable by other engines — which matters in a mesh where a consuming domain may not be a Snowflake shop. Connector v4 ingests Iceberg tables with no special configuration if you want part of the pipeline to land there directly.

### 3.4 Environment provisioning

Use **zero-copy clones** for dev/test rather than copying prod data. Two things to verify explicitly:

1. Tag-based masking and row access policies must survive the clone (they inherit — but test it, don't assume it).
2. Clones diverge and accrue their own storage. Give them a TTL and drop them on a schedule.

### 3.5 dbt patterns that control storage churn

- Incremental models with `merge` on a business key **plus a partition predicate** on the clustering column. Without the predicate, the merge scans and rewrites far more than it needs to.
- Set `on_schema_change: append_new_columns` so Registry-driven schema evolution doesn't break a run at 3 a.m.
- Ban `--full-refresh` in prod behind an approval step. It is the most common cause of surprise credit spikes.
- Avoid `select *` chains through the layers — every added column propagates rewrite cost through all four schemas.

---

## 4. Compute and warehouse configuration

### 4.1 One warehouse per workload, per data product

Running everything through a shared warehouse makes cost attribution impossible and turns every dbt run into a BI incident.

| Warehouse | Workload | Starting size | Auto-suspend | Scaling |
|---|---|---|---|---|
| `WH_<DP>_TRANSFORM` | dbt via MWAA | S–M | 120 s | Multi-cluster, **Economy** |
| `WH_<DP>_BI` | Dashboards / reporting | S | 60 s (kept warm in business hours) | Multi-cluster, **Standard** |
| `WH_<DP>_ADHOC` | Analyst exploration | XS | 60 s | Single cluster + resource monitor |
| `WH_PLATFORM_OPS` | Monitoring, metadata, governance jobs | XS | 60 s | Single cluster |

- **Scale up for slow single queries; scale out for concurrency.** These are different problems and sizing up a warehouse will not fix a queueing problem.
- Economy scaling for dbt (queueing is fine, throughput is what matters); Standard for BI (latency is what matters).
- Ingestion needs **no warehouse** — Snowpipe Streaming is serverless and billed on volume ingested.

### 4.2 Warehouse generation and Adaptive Compute

- **Move transform warehouses to Gen2** (`GENERATION = '2'`). Gen2 changed how DML is handled — smaller delete files instead of full micro-partition rewrites, which cuts write amplification. Your dbt layer is merge-heavy by construction, so this is where the gain lands. It also reduces the storage cost of Time Travel and replication on those tables.
- **Adaptive Warehouses** are now GA in selected AWS/Azure/GCP regions and remove size, multi-cluster, QAS and suspend/resume configuration entirely — Snowflake routes each query from a shared per-account pool. Snowflake reports up to 1.6x on analytical workloads, 2.2x throughput on concurrent operational analytics, and 3.5x on DML-heavy pipeline work versus Gen1.
  - Requires **Enterprise Edition or higher**.
  - **Check region availability for your account before planning around it** — the GA region list is limited and you are likely in EU West.
  - Sensible pilot order: `ADHOC` first (unpredictable, bursty, low blast radius), then `TRANSFORM`. Keep `BI` on Gen2 or interactive warehouses where you need tight, predictable latency.

### 4.3 Right-sizing method

Don't guess — read it off the query history:

- `BYTES_SPILLED_TO_REMOTE_STORAGE > 0` → warehouse is **undersized**. Size up; this is the highest-value single fix.
- `AVG_QUEUED_LOAD` consistently > 0 → **concurrency** problem. Add a cluster, don't size up.
- Low `EXECUTION_TIME` relative to `TOTAL_ELAPSED_TIME` with no queueing → the time is going on compilation or resume, not compute.

### 4.4 dbt-specific compute control

- Use dbt's `snowflake_warehouse` config to push a handful of heavy models onto a larger warehouse **for those models only**, rather than sizing the whole run up for the worst offender.
- Match dbt `threads` to warehouse size. Too many threads on a small warehouse just queues work; too few on a large one leaves it idle and billing.
- Set `QUERY_TAG` from Airflow on every session:

```sql
alter session set query_tag = '{"dag_id":"dp_payments_daily",
                               "task_id":"dbt_run_base",
                               "run_id":"...",
                               "data_product":"payments_settlement"}';
```

This single step is what makes §5 possible. Without it, `QUERY_HISTORY` is anonymous and cost attribution degrades to guesswork.

---

## 5. Query performance and reporting response times

### 5.1 Pruning is the whole game

Most slow Snowflake queries are scanning partitions they didn't need to.

- Always filter on the clustering / ingest-date column, and **never wrap it in a function**: `where event_date >= '2026-01-01'`, not `where to_char(event_ts,'YYYY-MM') = '2026-01'`.
- Avoid implicit casts in join and filter predicates — they silently disable pruning.
- **Avoid deep view-on-view nesting** in `ACCESS`. Three or four layers of views is where pruning quietly stops working and dashboard response times drift from 2 s to 30 s with no code change.

### 5.2 Search Optimization Service

The classic banking/telecom pattern — *"find every event for this account number / MSISDN / transaction reference"* — is a needle-in-haystack point lookup on a high-cardinality column, which clustering cannot help with. SOS is designed for exactly this.

Apply it **selectively**: named columns on specific tables, not blanket `ADD SEARCH OPTIMIZATION`. It carries both a build and a maintenance cost, so justify each one against measured lookup volume in `QUERY_HISTORY` and track spend in `SEARCH_OPTIMIZATION_HISTORY`.

### 5.3 Dynamic tables vs dbt

You already have dbt + MWAA, and that should remain the backbone — it gives you version control, tests, contracts, and lineage that a mesh governance model depends on.

Use **dynamic tables** as a targeted exception: for the small number of `ACCESS` objects that need sub-minute freshness, where a declarative `TARGET_LAG` beats scheduling a dbt run every two minutes. Keep them few and document them, or you end up with two orchestration systems and no single lineage graph.

### 5.4 Serving layer

- Materialize `ACCESS` as **tables, not views**, for anything a dashboard hits repeatedly. Pre-aggregate at the grain the report actually uses.
- Rely on the **result cache** for repeated identical dashboard queries (24 h, free) — but note it is invalidated by any change to the underlying data, so a streaming table won't benefit much. This is an argument for serving BI from a periodically-refreshed `ACCESS` table rather than pointing dashboards at near-real-time base data.
- Keep the BI warehouse **warm during business hours** (longer auto-suspend, or a keep-alive query) so the local disk cache survives. For telecom or trading peaks, pre-warm ahead of the known daily spike rather than letting the first users pay the cold-start.

---

## 6. Monitoring, utilization and cost optimization

### 6.1 The views that matter

Build a platform observability data product on `SNOWFLAKE.ACCOUNT_USAGE` — treat it as a first-class data product, not a spreadsheet someone maintains.

| View | Answers |
|---|---|
| `QUERY_HISTORY` | Slow queries, spilling, queueing, compile time |
| `QUERY_ATTRIBUTION_HISTORY` | **Credits per query** — the basis of real chargeback |
| `WAREHOUSE_METERING_HISTORY` | Credits per warehouse per hour |
| `WAREHOUSE_LOAD_HISTORY` | Utilization vs queueing — right-sizing evidence |
| `TABLE_STORAGE_METRICS` | Active vs Time Travel vs Fail-safe bytes per table |
| `AUTOMATIC_CLUSTERING_HISTORY` | Clustering maintenance spend |
| `SEARCH_OPTIMIZATION_HISTORY` | SOS spend |
| `ACCESS_HISTORY` | Who read which column — audit **and** unused-object detection |
| `SNOWPIPE_STREAMING_*` | Ingest volume and cost per table |

### 6.2 Controls

- **Budgets** per data product (credit envelope across warehouses and serverless features, with notification).
- **Resource monitors** per warehouse. Notify at 75% and 90%. Suspend at 100% in dev/test only — **never auto-suspend a prod-critical warehouse**; alert a human instead. An automated suspend during a regulatory reporting window is a worse outcome than an overspend.
- Tag-driven **showback per domain**, joining warehouse and database tags to metering and attribution. In a mesh, domains that can't see their own consumption have no incentive to optimize it.

### 6.3 Alerts to wire up

| Signal | Source | Why |
|---|---|---|
| Ingest lag: curated topic → landing table | Connector metrics + max load time | Freshness SLO breach |
| DLQ / Error Table depth rising | Kafka + Snowflake error tables | Contract violation upstream |
| Remote spill on any warehouse | `QUERY_HISTORY` | Undersized warehouse |
| Sustained queueing | `WAREHOUSE_LOAD_HISTORY` | Need another cluster |
| dbt model runtime regression > 50% w/w | Query tags | Data volume or plan change |
| Table with zero reads in 90 days | `ACCESS_HISTORY` | Storage to reclaim |
| Clustering credits trending up | `AUTOMATIC_CLUSTERING_HISTORY` | Bad clustering key |

### 6.4 Quick wins, in rough order of return

1. Convert `LANDING` and `STAGING` to transient tables with minimal Time Travel.
2. Split shared warehouses into per-workload warehouses and set auto-suspend to 60 s.
3. Set `QUERY_TAG` from Airflow on every session.
4. Fix the top ten spilling queries (size up or rewrite).
5. Move transform warehouses to Gen2.
6. Review or remove clustering keys with rising maintenance credits.
7. Drop or archive tables with no reads in 90 days.

---

## 7. Access control: RBAC + ABAC

### 7.1 Role model

Use the standard **access role / functional role** split — it is the only pattern that stays maintainable as data products multiply.

```
ACCOUNTADMIN (break-glass only, MFA, 2 named humans)
└── SYSADMIN
    └── DP_PLATFORM_ADMIN
        └── DP_<PRODUCT>_OWNER          -- domain team owns its own database
            ├── DP_<PRODUCT>_ACCESS_R   -- SELECT on ACCESS schema only
            ├── DP_<PRODUCT>_ACCESS_RW
            └── DP_<PRODUCT>_ENG        -- full control of LANDING/STAGING/BASE
```

Access roles hold the object grants. **Functional roles** (`FR_RISK_ANALYST`, `FR_NETWORK_OPS`, `FR_FINANCE_REPORTING`) are granted the access roles, and users are granted only functional roles — provisioned from Entra ID or Okta via **SCIM**, so joiners/movers/leavers are handled by HR/IdP, not by tickets to a platform team.

Two things that are easy to get wrong:

- **Warehouse `USAGE` is a separate grant from data access.** Grant it deliberately, per workload — it is how you stop an analyst running exploratory queries on the transform warehouse.
- Domain owners should be able to grant on **their own** database without a central ticket. Central control over who can grant is the bottleneck that kills mesh adoption; central control over *the tag taxonomy and policies* is the part worth keeping.

### 7.2 ABAC via tag-based policies

Snowflake's ABAC model attaches a data protection policy to a **tag** with `ALTER TAG`. When the tag is applied to a database, schema, table or column, the policy applies automatically and inherits downward. Available on **Enterprise Edition or higher**. Four policy types:

| Policy | Controls |
|---|---|
| Masking | Column values returned |
| Row access | Which rows are returned |
| Projection | Whether a column can be selected at all |
| Join | Whether a column can be used as a join key |

The projection and join policies are worth calling out for banking: they let you permit a column to be *used* (aggregated, joined behind the scenes) without allowing it to be *read* — useful for account identifiers and for privacy-preserving analytics on customer data.

**Tag taxonomy** — define once at platform level, apply per data product:

```
GOVERNANCE.TAGS.SENSITIVITY      = PUBLIC | INTERNAL | CONFIDENTIAL | RESTRICTED
GOVERNANCE.TAGS.PII_TYPE         = NAME | EMAIL | PHONE | MSISDN | IMSI | DOB | NATIONAL_ID
GOVERNANCE.TAGS.PCI              = PAN | CVV | EXPIRY
GOVERNANCE.TAGS.JURISDICTION     = UK | EU | US | APAC
GOVERNANCE.TAGS.DATA_PRODUCT     = <product name>
GOVERNANCE.TAGS.COST_CENTRE      = <cost centre>
```

Write the policy once, attach it to the tag once, and it covers every object the tag lands on. Attributes can come from your IdP through SCIM, so the policy can evaluate the user's department, location or clearance without you materialising a role for each combination — which is the whole point of ABAC over RBAC.

### 7.3 Row-level access via an entitlements table

For jurisdiction and legal-entity filtering, an entitlements table is easier to audit than policy logic:

```sql
create row access policy rap_entity_jurisdiction
as (legal_entity varchar, jurisdiction varchar) returns boolean ->
  exists (
    select 1
    from governance.security.user_entitlements e
    where e.snowflake_user  = current_user()
      and e.legal_entity    = legal_entity
      and e.jurisdiction    = jurisdiction
  )
  or is_role_in_session('DP_PLATFORM_ADMIN');
```

Keep the entitlements table small and clustered — the policy is evaluated per query. Populate it from the IdP, never by hand.

### 7.4 The schema-evolution gap (important)

Your Schema Registry allows compatible schema evolution, so **new columns will appear in landing tables without a human reviewing them**. A newly added `customer_email` column would land completely unprotected.

Three defences, use all three:

1. **Apply tags at the schema level, not just the column level** — new columns inherit the parent's protection by default.
2. Run Snowflake **sensitive data classification / auto-tagging** on a schedule against `LANDING` and `BASE`, and alert on any column classified as sensitive but carrying no tag.
3. Gate new-column promotion into `ACCESS` behind a dbt contract change, so a human explicitly approves exposure. `ACCESS` should be **allow-list**, never "whatever base has."

### 7.5 Service accounts and audit

- MWAA, dbt and the Confluent sink connector each get their **own** service user with a dedicated role, **key-pair or OAuth authentication** (not passwords), and a network policy restricted to the relevant VPC/NAT egress. Rotate keys on a schedule.
- Least privilege per component: the sink connector needs `INSERT` on `LANDING` and nothing else. It does not need `SELECT` on `BASE`.
- **`ACCESS_HISTORY`** gives you column-level read audit — this is normally the artefact a banking or telecom regulator actually asks for, so retain it beyond the default account usage window by copying it into your observability data product.
- Use `POLICY_REFERENCES` and `TAG_REFERENCES` to produce evidence that every column tagged `RESTRICTED` is covered by a policy. Make it a scheduled test with an alert, not a quarterly manual review.

---

## 8. Open questions to resolve

1. **Snowflake edition** — ABAC tag-based policies and Adaptive Warehouses both need Enterprise or higher. If you're on Standard, §7.2 and §4.2 need rework.
2. **Region** — Adaptive Compute GA region list is limited; confirm before designing around it.
3. **Kafka connector version** — v3 or v4? Determines whether §2.1 is a design decision or a migration project.
4. **Freshness SLO per data product** — this decides dbt schedule vs dynamic tables, and warehouse warm strategy. Worth pinning down before sizing anything.
5. **Regulatory retention** — drives the Iceberg/cold-storage decision in §3.3.
6. **Does the mesh cross Snowflake accounts?** If consuming domains are in separate accounts, output ports need shares or listings rather than grants, and cross-region replication cost enters the picture.
