# FpML → Canonical → Semantic → Physical
## An Enterprise Data Architect's Guide to Modelling OTC Derivatives and FX

**Author's framing:** this document is written the way a Senior Enterprise Data Architect would actually work a greenfield "FX and OTC derivatives on the Enterprise Data Platform" engagement — starting from the industry message standard, deriving an internal canonical model, projecting a semantic layer for consumers, and landing it physically on a lakehouse. One trade, **TRD10001**, is traced from FpML XML through to a Finance MTD settled-cash number.

**Date of writing:** September 2026
**FpML baseline used:** 5.13 Recommendation (build 2), with forward-looking notes on 5.14 Last Call Working Draft

---

## Table of contents

| Part | Subject |
|---|---|
| [1](#part-1--fpml-overview) | FpML overview — what it is, why banks use it, how it models trade/party/cashflow/event |
| [2](#part-2--realistic-business-scenario) | Realistic business scenario — five trades through the full lifecycle |
| [3](#part-3--conceptual-data-model) | Conceptual data model — business entities, relationships, cardinalities |
| [4](#part-4--canonical-data-model) | Canonical data model — 10 canonical entities with keys, attributes, FpML provenance |
| [5](#part-5--fpml--canonical-mapping) | FpML → canonical mapping — 40+ mappings with business meaning |
| [6](#part-6--semantic-data-model) | Semantic data model — facts, dimensions, and the three-model distinction |
| [7](#part-7--the-settled-cash-model) | **Settled cash** — the trade → expected cashflow → payment → settlement → settled cash chain |
| [8](#part-8--mtd-and-ltd-finance-example) | MTD / LTD Finance calculations with worked SQL |
| [9](#part-9--physical-data-model) | Physical model — Databricks + Delta Lake and Snowflake |
| [10](#part-10--end-to-end-data-architecture) | End-to-end architecture — Kafka, Schema Registry, Unity Catalog, DQ, lineage |
| [11](#part-11--data-architect-interview-perspective) | Interview perspective — 16 questions with architect-level answers |

---

# PART 1 — FpML OVERVIEW

## 1.1 What FpML is

**FpML (Financial products Markup Language)** is an XML-based, open message standard for the electronic exchange of information about **OTC (over-the-counter) derivatives and structured products**. It is owned and maintained by **ISDA** (International Swaps and Derivatives Association).

Three things it is worth being precise about, because interviewers probe exactly here:

1. **FpML is a *message* standard, not a database schema.** It describes *what one party tells another party about a transaction at a point in time*. It is a wire format designed for interoperability between institutions, not for storage, querying, or aggregation.

2. **FpML is a *product description* language first.** Its centre of gravity is the ability to describe the economic terms of an arbitrarily complex derivative — a Bermudan swaption with a step-up notional schedule, an amortising cross-currency basis swap, an FX target redemption forward — with enough precision that both parties can independently value and confirm it. The trade "wrapper" around that product is comparatively thin.

3. **FpML is deliberately recursive, choice-heavy and deeply nested.** That expressiveness is a feature for confirmation matching and a serious problem for analytics. This tension is the entire reason canonical modelling exists, and it is the answer to the most common interview question in this space (see Part 11, Q1).

### Positioning against neighbouring standards

| Standard | Domain | Where it sits relative to FpML |
|---|---|---|
| **FIX** | Pre-trade / execution, order flow, listed & FX | Upstream — order and execution messaging. FIX often *carries* an FpML block for complex OTC products |
| **FpML** | OTC derivatives post-trade: confirmation, lifecycle events, reporting | The subject of this document |
| **SWIFT MT** (MT300, MT202, MT103, MT950) | Legacy FX confirmation and cash payment/statement messaging | **Downstream** — FpML says a payment *should* happen; MT202/MT103 makes it happen; MT950 says it *did* |
| **ISO 20022** (pacs.008, pacs.009, camt.053, camt.054) | Modern payments and cash reporting | Downstream — the modern replacement for MT. **This is where "settled cash" truth actually comes from** |
| **CDM (Common Domain Model)** | ISDA's object/behavioural model for trade lifecycle | A *sibling and successor concept*. FpML describes state in XML; CDM describes state **and the functions that transition state** in a machine-executable model. FpML remains the dominant production wire format; CDM is increasingly used for lifecycle logic |
| **ISO 4217 / ISO 17442 / ISO 10383** | Currency codes / LEI / MIC | Reference data vocabularies FpML *references* via coding schemes |

> **Architect's point to make in an interview:** FpML never tells you cash actually moved. It tells you cash was *contractually due*. The gap between those two statements is Part 7 of this document and is where most enterprise settled-cash models go wrong.

---

## 1.2 Why financial institutions use FpML

| Driver | What FpML solves |
|---|---|
| **Confirmation automation** | Before FpML, an interest rate swap confirmation was a fax and a lawyer. FpML enables automated electronic confirmation matching (DTCC Deriv/SERV, MarkitWire/OSTTRA, Bloomberg VCON), taking confirmation from T+5 days to T+minutes |
| **Regulatory reporting** | EMIR REFIT, CFTC Part 43/45, SFTR, MiFIR, ASIC/MAS/HKMA rewrites. Trade repositories (DTCC GTR, UnaVista, REGIS-TR) accept FpML-derived submissions. The **Recordkeeping** and **Transparency** views exist for exactly this |
| **Clearing** | Submission of trades to CCPs (LCH SwapClear, CME, Eurex) and clearing status notifications back |
| **Portfolio reconciliation & collateral** | Daily inter-dealer portfolio recs (TriOptima/OSTTRA triReduce, triResolve) exchange FpML-based portfolio and valuation data |
| **Internal STP** | Front office → middle office → back office → risk → finance, without every hop inventing its own file format |
| **Vendor interoperability** | A common language across Murex, Calypso, Summit, Openlink, Bloomberg TOMS, ION, FXall, 360T, Refinitiv |
| **Non-proprietary and free** | No licence cost, open specification, ISDA-governed with industry working groups |

---

## 1.3 Current FpML version and the major schema areas

As of September 2026:

| Version | Status | Notes |
|---|---|---|
| **FpML 5.14** | **Last Call Working Draft** (build 1 published 30 Jan 2026) | The forward-looking version. Plan for it, do not build production on it |
| **FpML 5.13** | **Recommendation 2** | **The version to build against today.** Stable, republished as build 8 |
| FpML 5.12 | Recommendation | Widely deployed; still the production baseline at many firms |
| FpML 5.11 | Recommendation 3 | Legacy but still live in some TR and CCP interfaces |
| FpML 4.x | Superseded | Occasionally still seen in old vendor adapters — a real migration risk to check for |
| Architecture Specification 3.1 | Recommendation | Governs versioning, extension, validation and coding-scheme mechanics |

**Practical guidance:** target **5.13** as the canonical inbound contract, keep the parser version-aware, and treat 5.14 as a schema-evolution rehearsal (see Part 11, Q10).

### The six schema "views"

FpML 5.x is not a single monolithic schema. It is published as **views** — subsets of the full model tailored to a business purpose. Understanding views is a strong signal of real-world FpML experience.

| View | Purpose | What an architect uses it for |
|---|---|---|
| **Confirmation** | Full economic detail of a contract and its post-trade business events | **The primary ingestion source.** This is where trade capture, amendments, novations, terminations, exercises live |
| **Reporting** | Reporting of trading activity, positions, valuations, cashflows | Source for **valuation** and **position** feeds; contains `valuationReport`, `positionReport`, `cashflows` |
| **Recordkeeping** | Primary Economic Terms (PET) for SDR/trade repository recordkeeping | Regulatory reporting pipeline |
| **Transparency** | Public price-forming post-trade transparency (real-time public reporting) | MiFIR/CFTC public tape submissions — note it is **anonymised and reduced** |
| **Pretrade** | Pre-trade terms, credit limit checks | RFQ/quote and credit check flows |
| **Legal** | Legal documentation, e.g. Standard CSA representation | Collateral/legal agreement data |

```
                          FpML 5.13 model
                                |
    +---------+----------+------+------+-----------+---------+
    |         |          |             |           |         |
Confirmation Reporting Recordkeeping Transparency Pretrade  Legal
    |         |          |             |           |         |
  Trade &   Valuation  PET for       Public      RFQ /    CSA /
 lifecycle  Position   SDR / TR      tape       credit   legal docs
  events    Cashflows                            check
    |         |
    |         |
    v         v
  [ Enterprise ingestion — these two carry ~90% of the data platform's needs ]
```

### Major structural building blocks (independent of view)

| Block | Element(s) | Role |
|---|---|---|
| Message envelope | `<FpML>` root + `<header>` | Message identity, routing, sequencing, correlation |
| Trade wrapper | `<trade>`, `<tradeHeader>` | Identity, dates, party trade information |
| Product | `<fxSingleLeg>`, `<fxSwap>`, `<swap>`, `<creditDefaultSwap>`, `<swaption>`, ... | The economics. **Polymorphic** — this is the modelling challenge |
| Parties | `<party>`, `<account>` | Legal entities and their accounts, referenced by ID |
| Business event | `<tradeAmended>`, `<terminationNotification>`, ... | Lifecycle state transitions |
| Valuation | `<valuation>`, `<assetValuation>`, `<quotation>` | MTM, PV01, accruals |
| Coding schemes | `xxxScheme` attributes | Controlled vocabularies (currency, LEI, product type, settlement method) |

---

## 1.4 How FpML represents the core concepts

### Trade

A `<trade>` = a `<tradeHeader>` (identity and dates) plus **exactly one product element**.

```xml
<trade>
  <tradeHeader>
    <partyTradeIdentifier>
      <partyReference href="GMB"/>
      <tradeId tradeIdScheme="http://gmb.com/coding-scheme/trade-id">TRD10001</tradeId>
    </partyTradeIdentifier>
    <partyTradeIdentifier>
      <partyReference href="ABCBANK"/>
      <tradeId tradeIdScheme="http://abcbank.com/trade-id">ABC-FX-889231</tradeId>
    </partyTradeIdentifier>
    <partyTradeInformation>...</partyTradeInformation>
    <tradeDate>2026-09-01</tradeDate>
  </tradeHeader>
  <fxSingleLeg>...</fxSingleLeg>   <!-- the product -->
</trade>
```

**Critical modelling observation #1:** `partyTradeIdentifier` is **repeating**. Each party has its *own* trade ID for the *same* economic trade. There is no global trade key in FpML. Your canonical model must therefore mint an internal surrogate key and keep an *alias table* of all party identifiers — a classic identity-resolution problem, not a mapping problem.

**Critical modelling observation #2:** the product is a **substitution-group choice**. `<fxSingleLeg>`, `<fxSwap>`, `<swap>`, `<creditDefaultSwap>` are siblings, each with a completely different internal shape. A relational canonical model cannot naively flatten this; Part 4 shows the supertype/subtype pattern that solves it.

### Product

FpML product structures for FX:

| FpML element | Covers | Key children |
|---|---|---|
| `<fxSingleLeg>` | FX Spot, FX Forward, NDF | `exchangedCurrency1`, `exchangedCurrency2`, `valueDate`, `exchangeRate`, `nonDeliverableSettlement` |
| `<fxSwap>` | FX Swap | `nearLeg`, `farLeg` (each an `FxSwapLeg`, structurally an `fxSingleLeg`) |
| `<fxOption>` | FX vanilla option | `putCurrencyAmount`, `callCurrencyAmount`, `strike`, `expiryDateTime`, `premium` |
| `<fxDigitalOption>` | FX exotic/digital | trigger/barrier structures |
| `<termDeposit>` | FX-adjacent money market | `principal`, `fixedRate`, `maturityDate` |

> **Note there is no separate "FX Forward" element.** Spot vs Forward is *derived* from the relationship between `valueDate` and the currency pair's spot date. Source systems carry a product code; the standard does not. Your canonical model must decide which is authoritative. **Recommendation: store the source product code AND derive a canonical `product_subtype`, and reconcile the two as a data-quality rule.**

### Party and Counterparty

```xml
<party id="GMB">
  <partyId partyIdScheme="http://www.fpml.org/coding-scheme/external/iso17442">213800GLOBALMKTS001</partyId>
  <partyName>Global Markets Bank plc</partyName>
</party>
<party id="ABCBANK">
  <partyId partyIdScheme="http://www.fpml.org/coding-scheme/external/iso17442">5493001ABCBANK00001</partyId>
  <partyName>ABC Bank Limited</partyName>
</party>
```

**Crucially, FpML has no `<counterparty>` element.** "Counterparty" is a *perspective*, not a structure. Both sides are just `<party>`. Which one is "us" and which is "them" is determined by:

- `<header><sentBy>` / `<sendTo>`, or
- `<partyTradeInformation><relatedParty><role>` values, or
- `<partyTradeInformation>` with `partyReference` pointing at the reporting party, or
- `<onBehalfOf>`

This is one of the highest-value transformations the canonical layer performs: **converting a symmetric, perspective-free industry message into an asymmetric, house-view record with an explicit `booking_entity_id` and `counterparty_id`.** Say this in an interview and you will sound like someone who has done it.

### Payment and Cashflow

FpML's `Payment` complex type is the workhorse:

```xml
<exchangedCurrency1>
  <payerPartyReference href="GMB"/>
  <receiverPartyReference href="ABCBANK"/>
  <paymentAmount>
    <currency currencyScheme="http://www.fpml.org/ext/iso4217">USD</currency>
    <amount>13200000.00</amount>
  </paymentAmount>
</exchangedCurrency1>
```

Note the direction is expressed as **payer/receiver party references**, not as a sign or a `DR/CR` flag. Converting `(payer, receiver)` → `payment_direction ∈ {PAY, RECEIVE}` **relative to the booking entity** is again a canonical-layer job.

Where cashflows are *calculated* rather than stated (interest rate swaps), FpML uses:

```xml
<cashflows>
  <cashflowsMatchParameters>true</cashflowsMatchParameters>
  <paymentCalculationPeriod>
    <adjustedPaymentDate>2026-12-03</adjustedPaymentDate>
    <calculationPeriod>
      <adjustedStartDate>2026-09-03</adjustedStartDate>
      <adjustedEndDate>2026-12-03</adjustedEndDate>
      <notionalAmount>50000000.00</notionalAmount>
      <floatingRateDefinition>
        <calculatedRate>0.0432500</calculatedRate>
      </floatingRateDefinition>
    </calculationPeriod>
    <discountFactor>0.98923</discountFactor>
    <forecastPaymentAmount>
      <currency>USD</currency><amount>546631.94</amount>
    </forecastPaymentAmount>
  </paymentCalculationPeriod>
</cashflows>
```

> **Key insight:** for FX products the cashflows are *explicit and terminal* (two legs, one value date). For IRS/CDS they are *derived from a schedule and forecast from a curve*. The canonical `CanonicalCashflow` entity must serve both — which is why it carries a `cashflow_basis` attribute of `CONTRACTUAL` / `FORECAST` / `RESET_FIXED` / `ACTUAL`.

### Settlement

```xml
<settlementInformation>
  <settlementInstruction>
    <settlementMethod settlementMethodScheme="http://www.fpml.org/coding-scheme/settlement-method">CLS</settlementMethod>
    <correspondentInformation>
      <correspondentBank><routingIds><routingId routingIdCodeScheme="...swift">CITIUS33XXX</routingId></routingIds></correspondentBank>
    </correspondentInformation>
    <beneficiaryBank>
      <routingIds><routingId routingIdCodeScheme="...swift">ABCBGB2LXXX</routingId></routingIds>
    </beneficiaryBank>
    <beneficiary><routingIds><routingId routingIdCodeScheme="...accountNumber">GB29ABCB60161331926819</routingId></routingIds></beneficiary>
  </settlementInstruction>
</settlementInformation>
```

Also available: `<standingSettlementInstruction>` (an SSI reference rather than inline details) and `<splitSettlement>` (one payment split across multiple accounts — the reason your model needs settlement-level granularity below payment).

**But note what is missing: there is no settlement *status*.** No `SETTLED`, no `FAILED`, no `settledAmount`, no `actualSettlementDate`. FpML describes *instructions*. Confirmation of actual settlement arrives from a different universe entirely (camt.053/camt.054, CLS reports, nostro recs). **Part 7 is built on this fact.**

### Currency

Always via ISO 4217 with an explicit coding scheme:

```xml
<currency currencyScheme="http://www.fpml.org/ext/iso4217">GBP</currency>
```

The `currencyScheme` attribute matters — FpML permits non-ISO schemes (e.g. `iso4217-2001-08-15` or proprietary metal codes XAU/XAG). Your reference-data layer should validate the scheme, not just the value.

### Business events

The lifecycle events, expressed as message root elements in the Confirmation view:

| Event | FpML message / element | Canonical `event_type` |
|---|---|---|
| New trade executed | `executionNotification`, `tradeCreated` | `NEW` |
| Confirmation requested | `requestConfirmation` | `CONFIRM_REQUESTED` |
| Confirmed | `confirmationStatus` (status=`confirmed`) | `CONFIRMED` |
| Amendment | `amendmentNotification`, `requestAmendmentConfirmation` (`<amendment>` with `<originalTrade>` and `<amendedTrade>`) | `AMEND` |
| Increase (notional up) | `increaseNotification` | `INCREASE` |
| Partial/full termination | `terminationNotification` (`<termination>`, `<changeInNotional>`) | `TERMINATE` / `PARTIAL_TERM` |
| Novation | `novationNotification` (`<novation>` with transferor/transferee/remaining party) | `NOVATE` |
| Option exercise | `optionExerciseNotification` | `EXERCISE` |
| Allocation (block → funds) | `allocationNotification` | `ALLOCATE` |
| Clearing | `clearingStatus`, `clearingConfirmed` | `CLEARED` |
| Withdrawal/cancellation | `withdrawalNotification` | `CANCEL` |
| Valuation | `valuationReport` (Reporting view) | `VALUATION` |

Every one of these carries `<header>` with `messageId`, `sentBy`, `sendTo`, `creationTimestamp`, `correlationId`, `sequenceNumber`, `inReplyTo` — **these are your idempotency and ordering keys** (Part 11, Q7).

---

## 1.5 FpML (industry standard) vs internal enterprise data model

This table is the intellectual core of the whole exercise.

| Dimension | **FpML** (industry messaging/schema standard) | **Enterprise Canonical / Semantic Model** |
|---|---|---|
| **Purpose** | Interoperability between two counterparties | Aggregation, analytics, control and reporting across an institution |
| **Unit of information** | A *message* about an event | A *state* of an entity, plus its history |
| **Perspective** | Symmetric — neither party is privileged | Asymmetric — always the house view (booking entity vs counterparty) |
| **Shape** | Deeply nested, recursive XML with choices and substitution groups; 1,500+ types | Flat, normalised (canonical) then dimensional (semantic); ~30 core entities |
| **Identity** | Multiple party-scoped IDs; no global key | Single surrogate key per entity + alias/xref table |
| **Optionality** | Vast majority of elements optional | Canonical enforces mandatory business attributes and nullability contracts |
| **Product coverage** | Universal, extensible, polymorphic | Supertype/subtype with a curated attribute set per asset class |
| **Time model** | Message timestamp only; state implied by event sequence | Explicit bitemporality: business effective date + system valid-from/valid-to |
| **Cashflow** | Contractual/forecast only | Contractual → expected → paid → **settled**, with variance |
| **Settlement** | Instruction only, no status | Full status machine with partials, fails, retries, reconciliation |
| **Reference data** | Referenced via coding schemes, values embedded per-message | Mastered centrally; messages carry keys, not descriptions |
| **Volume/query pattern** | One document at a time, streamed | Billions of rows, columnar, scanned and aggregated |
| **Change management** | Version + view; annual-ish releases | Backwards-compatible schema evolution; consumers isolated from source change |
| **Consumers** | Counterparties, CCPs, TRs, confirmation platforms | Finance, Risk, Treasury, Regulatory, Product Control, BI, Data Science |

```
              FpML                              Enterprise Model
        ------------------                 -------------------------
        "Here is what I am                 "Here is what the firm
         telling you about                  believes to be true about
         this contract right                 this trade, its cash, and
         now."                               its P&L, as at any point
                                             in time."

        Verb: COMMUNICATE                  Verb: KNOW
        Optimised for: fidelity            Optimised for: aggregation
        Grain: message                     Grain: entity-version
        Time: now                          Time: bitemporal
```

---

## 1.6 A simplified FpML trade message — TRD10001

This is the message we will trace through every subsequent part. Simplified for readability, but structurally faithful to FpML 5.13 Confirmation view.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<FpML xmlns="http://www.fpml.org/FpML-5/confirmation"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      fpmlVersion="5-13"
      xsi:type="ExecutionNotification">

  <!-- ============ 1. MESSAGE ENVELOPE ============ -->
  <header>
    <messageId messageIdScheme="http://gmb.com/msg-id">MSG-20260901-000451</messageId>
    <sentBy messageAddressScheme="http://www.fpml.org/coding-scheme/external/iso17442">213800GLOBALMKTS001</sentBy>
    <sendTo messageAddressScheme="http://www.fpml.org/coding-scheme/external/iso17442">5493001ABCBANK00001</sendTo>
    <creationTimestamp>2026-09-01T14:32:07Z</creationTimestamp>
  </header>
  <isCorrection>false</isCorrection>
  <correlationId correlationIdScheme="http://gmb.com/correlation">CORR-TRD10001</correlationId>
  <sequenceNumber>1</sequenceNumber>

  <!-- ============ 2. THE BUSINESS EVENT ============ -->
  <trade>

    <!-- 2a. Trade header: identity + dates -->
    <tradeHeader>
      <partyTradeIdentifier>
        <partyReference href="GMB"/>
        <tradeId tradeIdScheme="http://gmb.com/coding-scheme/trade-id">TRD10001</tradeId>
      </partyTradeIdentifier>
      <partyTradeIdentifier>
        <partyReference href="ABCBANK"/>
        <tradeId tradeIdScheme="http://abcbank.com/coding-scheme/trade-id">ABC-FX-889231</tradeId>
      </partyTradeIdentifier>
      <partyTradeInformation>
        <partyReference href="GMB"/>
        <relatedParty>
          <partyReference href="GMBLDN"/>
          <role>BookingParty</role>
        </relatedParty>
        <trader>J.HARRIS</trader>
        <executionDateTime>2026-09-01T14:31:58Z</executionDateTime>
        <timestamps>
          <submittedForConfirmation>2026-09-01T14:32:07Z</submittedForConfirmation>
        </timestamps>
        <reportingRegime>
          <name>EMIR</name>
          <supervisorRegistration><supervisoryBody>ESMA</supervisoryBody></supervisorRegistration>
          <reportingRole>Principal</reportingRole>
        </reportingRegime>
        <executionVenueType>OffFacility</executionVenueType>
        <isAccountingHedge>false</isAccountingHedge>
      </partyTradeInformation>
      <tradeDate>2026-09-01</tradeDate>
    </tradeHeader>

    <!-- 2b. The PRODUCT -->
    <fxSingleLeg>
      <productType productTypeScheme="http://www.fpml.org/coding-scheme/product-taxonomy">ForeignExchange:Forward</productType>

      <!-- Leg 1: we RECEIVE GBP -->
      <exchangedCurrency1>
        <payerPartyReference href="ABCBANK"/>
        <receiverPartyReference href="GMB"/>
        <paymentAmount>
          <currency currencyScheme="http://www.fpml.org/ext/iso4217">GBP</currency>
          <amount>10000000.00</amount>
        </paymentAmount>
        <settlementInformation>
          <settlementInstruction>
            <settlementMethod settlementMethodScheme="http://www.fpml.org/coding-scheme/settlement-method">CLS</settlementMethod>
            <beneficiaryBank>
              <routingIds><routingId routingIdCodeScheme="http://www.fpml.org/coding-scheme/external/bic">GMBKGB2LXXX</routingId></routingIds>
            </beneficiaryBank>
            <beneficiary>
              <routingIds><routingId routingIdCodeScheme="http://gmb.com/account">ACC-GBP-01</routingId></routingIds>
            </beneficiary>
          </settlementInstruction>
        </settlementInformation>
      </exchangedCurrency1>

      <!-- Leg 2: we PAY USD -->
      <exchangedCurrency2>
        <payerPartyReference href="GMB"/>
        <receiverPartyReference href="ABCBANK"/>
        <paymentAmount>
          <currency currencyScheme="http://www.fpml.org/ext/iso4217">USD</currency>
          <amount>13200000.00</amount>
        </paymentAmount>
        <settlementInformation>
          <settlementInstruction>
            <settlementMethod settlementMethodScheme="http://www.fpml.org/coding-scheme/settlement-method">CLS</settlementMethod>
            <correspondentInformation>
              <correspondentBank>
                <routingIds><routingId routingIdCodeScheme="http://www.fpml.org/coding-scheme/external/bic">CITIUS33XXX</routingId></routingIds>
              </correspondentBank>
            </correspondentInformation>
            <beneficiaryBank>
              <routingIds><routingId routingIdCodeScheme="http://www.fpml.org/coding-scheme/external/bic">ABCBUS33XXX</routingId></routingIds>
            </beneficiaryBank>
          </settlementInstruction>
        </settlementInformation>
      </exchangedCurrency2>

      <valueDate>2026-09-03</valueDate>

      <exchangeRate>
        <quotedCurrencyPair>
          <currency1 currencyScheme="http://www.fpml.org/ext/iso4217">GBP</currency1>
          <currency2 currencyScheme="http://www.fpml.org/ext/iso4217">USD</currency2>
          <quoteBasis>Currency2PerCurrency1</quoteBasis>
        </quotedCurrencyPair>
        <rate>1.3200</rate>
        <spotRate>1.3198</spotRate>
        <forwardPoints>0.0002</forwardPoints>
      </exchangeRate>
    </fxSingleLeg>
  </trade>

  <!-- ============ 3. PARTIES & ACCOUNTS ============ -->
  <party id="GMB">
    <partyId partyIdScheme="http://www.fpml.org/coding-scheme/external/iso17442">213800GLOBALMKTS001</partyId>
    <partyName>Global Markets Bank plc</partyName>
  </party>
  <party id="GMBLDN">
    <partyId partyIdScheme="http://gmb.com/coding-scheme/booking-entity">GMB-LDN</partyId>
    <partyName>Global Markets Bank plc, London Branch</partyName>
  </party>
  <party id="ABCBANK">
    <partyId partyIdScheme="http://www.fpml.org/coding-scheme/external/iso17442">5493001ABCBANK00001</partyId>
    <partyName>ABC Bank Limited</partyName>
  </party>

  <account id="ACC-GBP-01">
    <accountId accountIdScheme="http://gmb.com/coding-scheme/account-id">ACC-GBP-01</accountId>
    <accountName>GMB London GBP Nostro</accountName>
    <accountBeneficiary href="GMB"/>
  </account>
</FpML>
```

### Element-by-element explanation of what matters

| Element | Why an architect cares |
|---|---|
| `fpmlVersion="5-13"` | **Version-aware parsing.** Store this on the bronze record; it drives parser dispatch and is your schema-evolution audit trail |
| `xsi:type="ExecutionNotification"` | The message type. Maps to canonical `event_type = NEW` |
| `header/messageId` | **Idempotency key.** Together with `sentBy` this uniquely identifies the physical message |
| `header/creationTimestamp` | System time of the message — *not* the business event time. Do not confuse the two |
| `correlationId` | **Links all messages about the same business event chain.** Essential for stitching request → response → status |
| `sequenceNumber` | **Ordering key** for out-of-order delivery. Higher sequence wins for the same correlation |
| `isCorrection` | `true` means "replace what I sent before", not "here is a new thing". Drives UPSERT-vs-INSERT logic |
| `partyTradeIdentifier` (×2) | Our ID *and* theirs. Both are stored; ours becomes `source_trade_id`, theirs becomes a row in the trade-alias table |
| `partyTradeInformation/partyReference` | Identifies **whose view this is** → becomes `booking_entity_id`; the other party becomes `counterparty_id` |
| `relatedParty/role = BookingParty` | Branch/desk-level booking entity — distinct from the LEI-level legal entity. Both are needed for Finance |
| `executionDateTime` | **Business event time** (with timezone). Used for MiFIR timestamp obligations and for event ordering |
| `reportingRegime` | Drives which regulatory pipeline the trade enters |
| `tradeDate` | Business date of execution — the date Finance and Risk book to |
| `productType` | ISDA taxonomy string. Parse it, do not store it as a blob: `ForeignExchange:Forward` → asset class + subtype |
| `exchangedCurrency1/2` | The **two contractual cashflows**. This is the seed of the entire settled-cash chain |
| `payerPartyReference` / `receiverPartyReference` | Direction. Resolve against `booking_entity_id` to get PAY/RECEIVE |
| `settlementInformation/settlementMethod = CLS` | Determines the settlement rail and therefore the expected confirmation source and timing |
| `valueDate` | Contractual settlement date. Compare with the pair's spot date to derive SPOT vs FORWARD |
| `exchangeRate/rate`, `spotRate`, `forwardPoints` | `rate = spotRate + forwardPoints` — a **free arithmetic data-quality check**: 1.3198 + 0.0002 = 1.3200 ✓ |
| `quoteBasis` | `Currency2PerCurrency1` means the rate is USD per GBP. Getting this wrong inverts every rate in your warehouse |
| `<party>` / `<account>` blocks | Referenced by `href` from anywhere in the document. Your parser must resolve these IDREFs, and they are **document-scoped, not global** |

> **⚠ Modelling note on TRD10001.** Trade date 01-Sep-2026 with value date 03-Sep-2026 is T+2, which for GBP/USD *is the spot date*. Booked as "FX Forward" by the front-office system, it is economically a spot trade. This is exactly the kind of discrepancy a canonical layer must surface: **store `source_product_type` as given, derive `canonical_product_subtype` from `value_date` vs `spot_date`, and raise a DQ warning when they disagree.** Do not silently overwrite the source. (For clarity in Part 2 we treat it as booked; TRD10005 is a genuine longer-dated forward.)

---
# PART 2 — REALISTIC BUSINESS SCENARIO

## 2.1 The institution

**Global Markets Bank plc (GMB)** — a UK-headquartered bank running a G10 FX and rates franchise.

| Attribute | Value |
|---|---|
| Group LEI | `213800GLOBALMKTS001` |
| Booking entities | `GMB-LDN` (London Branch), `GMB-NY` (Global Markets Bank NA, LEI `549300GLOBALMKTSUS1`) |
| FX trading platform | Murex MX.3 (front office) + 360T / FXall (e-venues) |
| Trade processing | In-house Trade Processing Platform (TPP) |
| Confirmations | OSTTRA MarkitWire / DTCC Deriv/SERV |
| Settlement rails | CLS (eligible pairs), CHAPS (GBP), Fedwire/CHIPS (USD), TARGET2 (EUR), BOJ-NET (JPY) |
| Data platform | Kafka → Databricks Lakehouse (Delta, Unity Catalog) |

### Nostro accounts

| Account ID | Currency | Agent | BIC | Purpose |
|---|---|---|---|---|
| `ACC-GBP-01` | GBP | Bank of England (direct CHAPS) | `GMBKGB2LXXX` | GBP nostro |
| `ACC-USD-01` | USD | Citibank NA, New York | `CITIUS33XXX` | USD nostro |
| `ACC-EUR-01` | EUR | Deutsche Bank AG, Frankfurt | `DEUTDEFFXXX` | EUR nostro |
| `ACC-JPY-01` | JPY | MUFG Bank, Tokyo | `BOTKJPJTXXX` | JPY nostro |
| `ACC-AUD-01` | AUD | ANZ, Sydney | `ANZBAU3MXXX` | AUD nostro |

### Counterparties

| ID | Name | LEI | Type | Sector |
|---|---|---|---|---|
| `CP-001` | ABC Bank Limited | `5493001ABCBANK00001` | Bank / FC | Credit institution |
| `CP-002` | XYZ Asset Management LLP | `213800XYZAM0000001` | Buy-side / NFC+ | Investment fund manager |

---

## 2.2 The flow

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  FRONT OFFICE SYSTEMS                                            │
  │  360T / FXall  →  Murex MX.3 FX  (trade capture, pricing, risk)  │
  └────────────────────────────┬─────────────────────────────────────┘
                               │  FpML 5.13 (Confirmation view)
                               │  executionNotification / tradeAmended /
                               │  withdrawalNotification, over Kafka
                               v
  ┌──────────────────────────────────────────────────────────────────┐
  │  TRADE PROCESSING PLATFORM (TPP)                                 │
  │  • FpML schema validation      • SSI enrichment                  │
  │  • Confirmation orchestration  • Cashflow generation             │
  │  • Netting & payment creation  • Settlement instruction release  │
  └────────────────────────────┬─────────────────────────────────────┘
                               │  FpML + internal events + ISO 20022 in-bound
                               v
  ┌──────────────────────────────────────────────────────────────────┐
  │  ENTERPRISE DATA PLATFORM  (Kafka → Bronze → Silver → Gold)      │
  └───┬────────┬─────────┬──────────┬──────────┬─────────────────────┘
      │        │         │          │          │
      v        v         v          v          v
  ┌───────┐┌────────┐┌──────────┐┌──────┐┌─────────┐┌──────────────┐
  │ Trade ││Cashflow││Settlement││ Risk ││ Finance ││  Regulatory  │
  │ Data  ││        ││ & Cash   ││ VaR  ││ GL, MTD ││ EMIR/MiFIR/  │
  │       ││        ││          ││ PV01 ││ LTD, P&L││ CFTC         │
  └───────┘└────────┘└──────────┘└──────┘└─────────┘└──────────────┘
```

**Inbound streams into the platform (this is the part candidates usually forget):**

| Stream | Format | Source | Carries |
|---|---|---|---|
| `fx.trade.events` | FpML 5.13 Confirmation | Murex via TPP | New, amend, cancel, novate |
| `fx.confirmation.status` | FpML 5.13 | OSTTRA | Confirmation matched/affirmed |
| `fx.valuation` | FpML 5.13 Reporting | Risk engine | Daily MTM, PV01 |
| `cash.payment.events` | Internal / pain.001-like | TPP payments | Payment created / released / cancelled |
| `cash.settlement.confirm` | **ISO 20022 camt.054** | Nostro agents | Actual debit/credit notifications |
| `cash.statement` | **ISO 20022 camt.053** | Nostro agents | EOD account statements |
| `cls.settlement.report` | CLS proprietary | CLS Bank | Settlement session outcome |
| `refdata.*` | Internal | MDM | Parties, accounts, currencies, calendars, products |

> **The single most important architectural statement in this document:** trade truth arrives as **FpML**; cash truth arrives as **ISO 20022 / CLS**. Settled cash is the *reconciliation* of the two. A settled-cash model built only from FpML is a forecast, not a fact.

---

## 2.3 The trade portfolio

All trades executed **01-Sep-2026**, booked in `GMB-LDN`.

| Trade ID | Product | We buy | We sell | Rate | Trade date | Value date | Counterparty | Lifecycle story |
|---|---|---|---|---|---|---|---|---|
| **TRD10001** | FX Forward | GBP 10,000,000 | USD 13,200,000 | 1.3200 | 2026-09-01 | 2026-09-03 | ABC Bank | **Clean** — CLS settled in full |
| **TRD10002** | FX Spot | USD 5,450,000 | EUR 5,000,000 | 1.0900 | 2026-09-01 | 2026-09-03 | XYZ AM | **Failed** EUR leg → settled 04-Sep + interest claim |
| **TRD10003** | FX Swap | near: USD 20,000,000 vs JPY 3,000,000,000<br>far: JPY 2,974,000,000 vs USD 20,000,000 | | near 150.000<br>far 148.700 | 2026-09-01 | near 2026-09-03<br>far 2026-12-03 | ABC Bank | **Partial** JPY settlement on near leg; far leg **outstanding** |
| **TRD10005** | FX Forward | GBP 8,000,000 | USD 10,600,000 | 1.3250 | 2026-09-01 | 2026-12-03 | ABC Bank | **Amended** 02-Sep to GBP 6,000,000 / USD 7,950,000 → version 2 |
| **TRD10006** | FX Spot | AUD 3,000,000 | USD 1,995,000 | 0.6650 | 2026-09-01 | 2026-09-03 | XYZ AM | **Cancelled** 02-Sep (booking error / trade bust) |

**Arithmetic checks (build these as DQ rules):**

- TRD10001: 10,000,000 GBP × 1.3200 = 13,200,000 USD ✓; spot 1.3198 + fwd points 0.0002 = 1.3200 ✓
- TRD10002: 5,000,000 EUR × 1.0900 = 5,450,000 USD ✓
- TRD10003 near: 20,000,000 USD × 150.000 = 3,000,000,000 JPY ✓
- TRD10003 far: 20,000,000 USD × 148.700 = 2,974,000,000 JPY ✓ (swap points −1.300, correct sign since USD yields > JPY yields; JPY 26,000,000 net cost over 91 days ≈ 3.5% annualised differential)
- TRD10005 v1: 8,000,000 × 1.3250 = 10,600,000 ✓; v2: 6,000,000 × 1.3250 = 7,950,000 ✓
- TRD10006: 3,000,000 AUD × 0.6650 = 1,995,000 USD ✓

---

## 2.4 TRD10001 traced end to end

### Stage-by-stage timeline

| # | Timestamp (UTC) | Stage | System | Message / artefact | What is created |
|---|---|---|---|---|---|
| 1 | 01-Sep 14:31:58 | **Execution** | 360T → Murex | FIX `ExecutionReport` (8=FIX.4.4, 35=8) | Execution record; `execution_timestamp` |
| 2 | 01-Sep 14:32:07 | **Booking & publish** | Murex → Kafka | **FpML `executionNotification`**, `fpmlVersion=5-13`, `MSG-20260901-000451` | `CanonicalTrade` v1, `CanonicalTradeEvent` `NEW` |
| 3 | 01-Sep 14:32:11 | **Validation & enrichment** | TPP | Schema + business validation; SSI lookup | `CanonicalSettlementInstruction` resolved |
| 4 | 01-Sep 14:33:02 | **Confirmation request** | TPP → OSTTRA | FpML `requestConfirmation` | `TradeEvent` `CONFIRM_REQUESTED` |
| 5 | 01-Sep 14:47:15 | **Confirmed** | OSTTRA → TPP | FpML `confirmationStatus` = `confirmed` | `trade_status` NEW → CONFIRMED |
| 6 | 01-Sep 15:02:00 | **Cashflow generation** | TPP | — | **2 × `CanonicalCashflow`** (see below) |
| 7 | 01-Sep 15:05:00 | **Netting decision** | TPP | Bilateral netting vs ABC Bank for VD 03-Sep | GBP leg nets to a single CLS pay-in |
| 8 | 01-Sep 22:00:00 | **CLS submission** | Settlements → CLS | CLS input for VD 03-Sep | `CanonicalPayment` × 2, status `INSTRUCTED` |
| 9 | 02-Sep 06:00:00 | **CLS matched** | CLS → TPP | Match report | Payment status `MATCHED` |
| 10 | 02-Sep 21:00:00 | **Valuation** | Risk → Kafka | FpML `valuationReport` (Reporting view) | `CanonicalValuation` MTM USD 49,994 |
| 11 | 03-Sep 05:30–07:00 CET | **Pay-in / settlement session** | CLS | Multilateral net pay-ins funded | — |
| 12 | 03-Sep 09:12:41 | **Settlement confirmed** | CLS → TPP | CLS settlement report | **`CanonicalSettlement`** × 2, status `SETTLED` |
| 13 | 03-Sep 09:35 / 10:02 | **Cash confirmed** | Nostro agents | **ISO 20022 `camt.054`** credit (GBP) / debit (USD) | `settled_amount`, `settled_datetime` populated |
| 14 | 03-Sep 23:15 | **EOD statement** | Nostro agents | **ISO 20022 `camt.053`** | Nostro reconciliation |
| 15 | 04-Sep 06:00 | **Settled cash finalised** | Data platform | — | `gold_fact_settled_cash` rows, `rec_status = MATCHED` |
| 16 | 04-Sep 06:30 | **Accounting** | Finance | GL journal | Nostro DR/CR, FX position, realised P&L |
| 17 | 01-Sep (T+1 by 23:59) | **Regulatory** | Reg reporting | EMIR REFIT submission to TR | `UTI`, action type `NEWT` |

### The two cashflows

```
                       TRD10001  (FX Forward)
                              │
              ┌───────────────┴───────────────┐
              │                               │
     CF-10001-01                        CF-10001-02
     RECEIVE GBP 10,000,000             PAY USD 13,200,000
     value date 2026-09-03              value date 2026-09-03
     into ACC-GBP-01                    from ACC-USD-01
              │                               │
        PMT-90001                        PMT-90002
        CLS pay-in leg                   CLS pay-in leg
              │                               │
        STL-90001                        STL-90002
        SETTLED 2026-09-03 09:12         SETTLED 2026-09-03 09:12
        GBP 10,000,000                   USD 13,200,000
              │                               │
              └───────────────┬───────────────┘
                              v
                   gold_fact_settled_cash
                   2 rows, MTD-September-2026
```

### The same trade at each modelling layer

| Layer | Representation | Example |
|---|---|---|
| **Source (FpML)** | One XML document, 120 lines, nested, symmetric | `<fxSingleLeg><exchangedCurrency1>...` |
| **Bronze** | 1 row: raw XML + envelope metadata | `bronze_fpml_message` |
| **Canonical (Silver)** | 1 `silver_trade` + 1 `silver_trade_leg` pair + 2 `silver_cashflow` + 2 `silver_payment` + 2 `silver_settlement` + 1 `silver_valuation` + n `silver_trade_event` | 9+ rows |
| **Semantic (Gold)** | 1 `gold_fact_trade` + 2 `gold_fact_cashflow` + 2 `gold_fact_settled_cash` + 1 `gold_fact_valuation`, joined to conformed dimensions | Star schema |
| **Consumption** | "GMB London's MTD net USD settled cash with ABC Bank" | A single number |

---

## 2.5 The other four trades — where it gets interesting

### TRD10002 — a failed settlement

| Time | Event | Effect on model |
|---|---|---|
| 03-Sep 06:00 | EUR pay-in due (TARGET2) | `PMT-90004` status `RELEASED` |
| 03-Sep 15:45 | TARGET2 cut-off missed — EUR nostro under-funded | `STL-90004` status **`FAILED`**, `fail_reason = INSUFFICIENT_FUNDS`, `settled_amount = 0` |
| 03-Sep 16:10 | Fail investigated; funding arranged | `TradeEvent` `SETTLEMENT_FAILED` |
| 04-Sep 08:20 | Re-instructed | `PMT-90004-R1` (retry, `parent_payment_id = PMT-90004`) |
| 04-Sep 10:33 | Settled | `STL-90005` **`SETTLED`** EUR 5,000,000, `settlement_date = 2026-09-04` ≠ `value_date = 2026-09-03` |
| 15-Sep 11:00 | Interest claim from XYZ AM paid | `STL-90009` **`SETTLED`** EUR 298.61, `cashflow_type = COMPENSATION_CLAIM` |

Claim arithmetic: EUR 5,000,000 × 2.15% × 1/360 = **EUR 298.61**.

> **Modelling consequence:** `value_date` and `settlement_date` are **different columns and must never be conflated**. `value_date` is contractual; `settlement_date` is factual. `settlement_date − value_date > 0` is the definition of a fail, and the difference drives the compensation claim.

### TRD10003 — partial settlement and an outstanding leg

Near leg, VD 03-Sep: the JPY 3,000,000,000 pay leg exceeded the intraday liquidity cap on the BOJ-NET queue and was released in two tranches.

| Settlement | Amount | Settlement date | Status |
|---|---|---|---|
| `STL-90007` | JPY 2,000,000,000 | 2026-09-03 | `SETTLED` |
| `STL-90008` | JPY 1,000,000,000 | 2026-09-04 | `SETTLED` |

At close of 03-Sep, `CF-10003-02` shows `expected_amount = 3,000,000,000`, `settled_amount = 2,000,000,000`, `unsettled_amount = 1,000,000,000`, `settlement_status = PARTIALLY_SETTLED`.

Far leg, VD 03-Dec: `CF-10003-03` (PAY USD 20,000,000) and `CF-10003-04` (RECEIVE JPY 2,974,000,000) remain `EXPECTED` — **outstanding, not failed**. Distinguishing "not yet due" from "due and unpaid" is a common modelling error.

### TRD10005 — an amendment

```
02-Sep 10:14  FpML  <requestAmendmentConfirmation>
                       <amendment>
                         <originalTrade>  ... GBP  8,000,000 / USD 10,600,000
                         <amendedTrade>   ... GBP  6,000,000 / USD  7,950,000
```

| Object | v1 | v2 |
|---|---|---|
| `silver_trade` | `trade_version=1`, `valid_to = 2026-09-02T10:14Z`, `is_current=false` | `trade_version=2`, `valid_from = 2026-09-02T10:14Z`, `is_current=true` |
| Cashflows | `CF-10005-01/02` → `cashflow_status = SUPERSEDED` | `CF-10005-03/04` → `EXPECTED` |
| Payments | none created (VD is 03-Dec, far out) | none yet |
| Trade event | — | `TradeEvent` `AMEND`, `event_qualifier = NOTIONAL_DECREASE` |

Because no payment had been instructed, the amendment is clean. **If a payment had already been released, the model must produce a cancel + re-issue pair, not an in-place update** — you cannot un-send a SWIFT message.

### TRD10006 — a cancellation

```
02-Sep 09:05  FpML  <withdrawalNotification>  (or <tradeCancelled>)
```

| Object | Treatment |
|---|---|
| `silver_trade` | `trade_status = CANCELLED`, **row retained**, `is_current = true`, `cancellation_datetime` set |
| Cashflows | `cashflow_status = CANCELLED`, `expected_amount` retained for audit |
| `gold_fact_trade` | Excluded from active-population measures via `trade_status <> 'CANCELLED'`, but **still present** |

> **Never physically delete.** A cancelled trade must remain queryable: EMIR requires an `EROR`/`REVI` action type submission, Finance needs to reverse any accrual, and Audit needs the trail. Cancellation is a *state*, not a *delete*.

---

## 2.6 Consolidated cash ledger — September 2026

Every settlement produced by the portfolio:

| Settlement ID | Trade | Cashflow | Ccy | Dir | Expected | Settled | Value date | Settlement date | Status |
|---|---|---|---|---|---|---|---|---|---|
| STL-90001 | TRD10001 | CF-10001-01 | GBP | RECEIVE | 10,000,000.00 | 10,000,000.00 | 2026-09-03 | 2026-09-03 | SETTLED |
| STL-90002 | TRD10001 | CF-10001-02 | USD | PAY | 13,200,000.00 | 13,200,000.00 | 2026-09-03 | 2026-09-03 | SETTLED |
| STL-90003 | TRD10002 | CF-10002-01 | USD | RECEIVE | 5,450,000.00 | 5,450,000.00 | 2026-09-03 | 2026-09-03 | SETTLED |
| STL-90004 | TRD10002 | CF-10002-02 | EUR | PAY | 5,000,000.00 | 0.00 | 2026-09-03 | — | **FAILED** |
| STL-90005 | TRD10002 | CF-10002-02 | EUR | PAY | 5,000,000.00 | 5,000,000.00 | 2026-09-03 | 2026-09-04 | SETTLED (late) |
| STL-90006 | TRD10003 | CF-10003-01 | USD | RECEIVE | 20,000,000.00 | 20,000,000.00 | 2026-09-03 | 2026-09-03 | SETTLED |
| STL-90007 | TRD10003 | CF-10003-02 | JPY | PAY | 3,000,000,000 | 2,000,000,000 | 2026-09-03 | 2026-09-03 | **PARTIAL** |
| STL-90008 | TRD10003 | CF-10003-02 | JPY | PAY | 3,000,000,000 | 1,000,000,000 | 2026-09-03 | 2026-09-04 | SETTLED (residual) |
| STL-90009 | TRD10002 | CF-10002-05 | EUR | PAY | 298.61 | 298.61 | 2026-09-15 | 2026-09-15 | SETTLED (claim) |

Outstanding at month end (30-Sep-2026):

| Cashflow | Trade | Ccy | Dir | Amount | Value date | Status |
|---|---|---|---|---|---|---|
| CF-10003-03 | TRD10003 far | USD | PAY | 20,000,000.00 | 2026-12-03 | EXPECTED |
| CF-10003-04 | TRD10003 far | JPY | RECEIVE | 2,974,000,000 | 2026-12-03 | EXPECTED |
| CF-10005-03 | TRD10005 v2 | GBP | RECEIVE | 6,000,000.00 | 2026-12-03 | EXPECTED |
| CF-10005-04 | TRD10005 v2 | USD | PAY | 7,950,000.00 | 2026-12-03 | EXPECTED |

Cancelled / superseded (retained, excluded from measures):

| Cashflow | Trade | Ccy | Amount | Status |
|---|---|---|---|---|
| CF-10005-01 / 02 | TRD10005 v1 | GBP / USD | 8,000,000 / 10,600,000 | SUPERSEDED |
| CF-10006-01 / 02 | TRD10006 | AUD / USD | 3,000,000 / 1,995,000 | CANCELLED |

These nine settlements and four outstanding cashflows are the dataset used in Part 8.

---
# PART 3 — CONCEPTUAL DATA MODEL

The conceptual model answers one question only: **what things does the business talk about, and how are they related?** No keys, no datatypes, no nullability. If a Head of Product Control cannot read it, it is not a conceptual model.

## 3.1 Entity definitions

| # | Entity | Business definition (the kind of definition that belongs in a glossary) |
|---|---|---|
| 1 | **Legal Entity** | A legally incorporated body that can enter into contracts, hold assets and be identified by an LEI. Includes our own booking entities and every counterparty |
| 2 | **Party** | A participant in a trade or in the processing of a trade, in a specific *role* — executing party, booking party, clearing broker, guarantor, beneficiary, agent. A Party is a Legal Entity *playing a role* |
| 3 | **Counterparty** | The Party on the *other* side of a trade from our booking entity. **A perspective, not an independent thing** |
| 4 | **Account** | A record maintained by a servicing institution against which cash or securities movements are posted. Includes nostro, vostro, client, and internal accounts |
| 5 | **Product** | The financial instrument or contract type traded, with its defining economic terms and taxonomy classification |
| 6 | **Currency** | A medium of exchange identified by ISO 4217, with its market conventions (settlement lag, holiday calendar, decimal precision, CLS eligibility) |
| 7 | **Trade** | A legally binding bilateral agreement between two parties to exchange economic value under agreed terms. The unit of execution, risk, confirmation and regulatory reporting |
| 8 | **Trade Event** | A discrete, dated occurrence that creates, modifies, or ends a Trade, or records a change in its processing state |
| 9 | **Cashflow** | An amount of a single currency contractually due to move between two parties on a stated date as a consequence of a Trade. **Intent, not fact** |
| 10 | **Payment** | An instruction issued to move a specific amount of cash on a specific date via a specific method, satisfying one or more Cashflows. **Action, not fact** |
| 11 | **Settlement** | The confirmed, final and irrevocable transfer of cash for a Payment, evidenced by an external source. **Fact** |
| 12 | **Settlement Instruction (SSI)** | The standing or trade-specific directions describing where and how cash for a given party, currency and product should be delivered |
| 13 | **Valuation** | A measurement of the economic worth or risk of a Trade or Position, at a valuation date, under a stated methodology |
| 14 | **Position** | An aggregation of Trades and/or Cashflows along a set of dimensions (entity, counterparty, currency, book, tenor) representing net exposure at a point in time |

## 3.2 The conceptual diagram

```
                        ┌──────────────────┐
                        │   LEGAL ENTITY   │
                        │  (LEI, country,  │
                        │   sector, parent)│
                        └───┬──────────┬───┘
                            │ 1        │ 1
                  is played │          │ owns
                     by as  │          │
                            │ N        │ N
                     ┌──────v─────┐  ┌─v────────┐
                     │   PARTY    │  │ ACCOUNT  │◄──────┐
                     │  (+ role)  │  │ (nostro, │       │
                     └──┬──────┬──┘  │  client) │       │
                        │      │     └──────────┘       │
       executes / books │      │ is counterparty to     │
                        │ N    │ N                      │
                     ┌──v──────v────────────────┐       │
      ┌──────────┐   │                          │       │
      │ PRODUCT  │──►│          TRADE           │       │
      │ (type,   │ 1 │  (id, version, dates,    │       │
      │ taxonomy)│  N│   status, notional)      │       │
      └──────────┘   └──┬────────┬─────────┬────┘       │
            ▲           │ 1      │ 1       │ 1          │
            │           │        │         │            │
   ┌────────┴──────┐    │ N      │ N       │ N          │
   │   CURRENCY    │  ┌─v─────┐┌─v──────┐┌─v─────────┐  │
   │ (ISO 4217,    │  │ TRADE ││CASHFLOW││ VALUATION │  │
   │  conventions) │  │ EVENT ││        ││ (MTM,PV01)│  │
   └───────┬───────┘  └───────┘└───┬────┘└───────────┘  │
           │                    1  │                    │
           │ denominates           │ N                  │
           │                  ┌────v────┐               │
           │                  │ PAYMENT │───────────────┤ debits/
           ├─────────────────►│         │               │ credits
           │                  └────┬────┘               │
           │                    1  │                    │
           │                       │ N                  │
           │                 ┌─────v──────┐             │
           └────────────────►│ SETTLEMENT │─────────────┘
                             │  (actual)  │
                             └─────┬──────┘
                                   │ N
                                   │ rolls up to
                                   v
                             ┌───────────┐
                             │ POSITION  │
                             └───────────┘

           ┌────────────────────────┐
           │ SETTLEMENT INSTRUCTION │  governs how Payments
           │        (SSI)           │──────► are constructed
           └────────────────────────┘        (Party × Currency × Product)
```

## 3.3 Relationships and cardinalities

| # | Relationship | Cardinality | Explanation and the trap it avoids |
|---|---|---|---|
| 1 | Legal Entity → Party | 1 : N | One legal entity plays many roles across trades. `ABC Bank` is *counterparty* on TRD10001 and could be *guarantor* elsewhere |
| 2 | Legal Entity → Account | 1 : N | GMB owns GBP, USD, EUR, JPY, AUD nostros |
| 3 | Party → Trade (executes) | 1 : N | A party executes many trades |
| 4 | Trade → Party | N : 2 (minimum) | **Every trade has exactly two principal parties.** Allocations, novations and give-ups add more roles — so implement as N:M through a `trade_party_role` associative entity, not two FK columns. This is a real design decision worth defending |
| 5 | Product → Trade | 1 : N | Many trades of the same product type |
| 6 | **Trade → Cashflow** | **1 : N** | FX Spot/Forward = 2. FX Swap = 4. 5y quarterly IRS = ~40. Plus fees, premiums, claims |
| 7 | **Trade → Trade Event** | **1 : N** | NEW → CONFIRMED → AMEND → ... → MATURED. Append-only |
| 8 | Trade → Valuation | 1 : N | One per valuation date per measure type |
| 9 | **Cashflow → Payment** | **1 : N** | One cashflow can generate multiple payments (split settlement, retry after fail, tranching) |
| 10 | Payment → Cashflow | N : M in practice | **Netting** — one payment can satisfy many cashflows across many trades. Model with a `payment_cashflow_allocation` bridge |
| 11 | **Payment → Settlement** | **1 : N** | Partial settlements produce multiple settlement records per payment |
| 12 | Party → Account | 1 : N | Same as (2), from the role perspective |
| 13 | Account → Settlement | 1 : N | Every settlement debits or credits exactly one account |
| 14 | Currency → Cashflow / Payment / Settlement | 1 : N | Every monetary object is denominated in exactly one currency |
| 15 | SSI → Payment | 1 : N | An SSI is a template applied to many payments |
| 16 | Trade / Cashflow → Position | N : 1 | Positions are aggregates |
| 17 | Trade → Trade (self-referencing) | 0..1 : N | Novation, allocation, compression and give-up create parent/child trade lineage. **Do not forget this edge** |

### The cardinality that decides your architecture

```
Trade  ──1:N──►  Cashflow  ──N:M──►  Payment  ──1:N──►  Settlement
  │                                     ▲
  │                                     │
  └─────── netting collapses many ──────┘
           cashflows into one payment
```

If you model `Cashflow → Payment` as 1:1 you will be unable to represent bilateral netting, CLS multilateral netting, or split settlement — and your settled cash will never reconcile to the nostro. **This is the single most common flaw in FX data models.** Insist on the bridge table.

## 3.4 The conceptual model applied to TRD10001

| Entity | Instance |
|---|---|
| Legal Entity | Global Markets Bank plc (`213800GLOBALMKTS001`); ABC Bank Limited (`5493001ABCBANK00001`) |
| Party | GMB-LDN as *BookingParty*; GMB as *ExecutingParty*; ABC Bank as *Counterparty* |
| Product | FX Forward (`ForeignExchange:Forward`) |
| Trade | TRD10001, version 1, trade date 2026-09-01, status CONFIRMED |
| Trade Event | NEW (14:32:07), CONFIRM_REQUESTED (14:33:02), CONFIRMED (14:47:15), SETTLED (03-Sep 09:12) |
| Cashflow | CF-10001-01 RECEIVE GBP 10,000,000 @ 2026-09-03; CF-10001-02 PAY USD 13,200,000 @ 2026-09-03 |
| Payment | PMT-90001 (GBP, CLS); PMT-90002 (USD, CLS) |
| Settlement | STL-90001 (GBP 10,000,000 settled 03-Sep); STL-90002 (USD 13,200,000 settled 03-Sep) |
| SSI | GMB GBP CLS SSI; ABC Bank USD SSI via CITIUS33 |
| Account | ACC-GBP-01 (credited); ACC-USD-01 (debited) |
| Currency | GBP, USD |
| Valuation | MTM USD +49,994 as at 2026-09-02 |
| Position | GMB-LDN vs ABC Bank, GBP long 10,000,000 / USD short 13,200,000 for VD 2026-09-03 |

---

# PART 4 — CANONICAL DATA MODEL

## 4.1 What the canonical model is for

The canonical model is a **source-system-independent, consumer-independent, normalised representation of the business facts.** Its job is to be *the one place where the meaning of a trade is decided*, so that:

- **N sources × M consumers becomes N + M integrations, not N × M.** Murex, Calypso, the e-FX platform and the bond desk all map *into* canonical; Finance, Risk, Reg and BI all map *out of* it.
- Source-system quirks are absorbed once. When Murex is replaced by a new platform in 2029, the canonical contract does not change and no downstream consumer is touched.
- Business rules that define meaning (what counts as a counterparty, what direction means, what a version is) live in one place and are testable.
- History and lineage are guaranteed, because canonical is where bitemporality is implemented.

### Canonical design principles

| Principle | Rule |
|---|---|
| **Third normal form, mostly** | Normalise to 3NF; denormalise only for the semantic layer |
| **Surrogate keys everywhere** | Never use a source natural key as a primary key. Sources reuse and recycle IDs |
| **Alias tables for identity** | Every external identifier lives in an `*_alias`/xref table, not in the entity |
| **Supertype/subtype for products** | A common trade core + asset-class-specific extension tables |
| **Bitemporal** | `business_effective_from/to` (when it was true) + `valid_from/to` (when we knew it) |
| **Explicit direction and perspective** | Every monetary record carries direction *relative to the booking entity* |
| **Amounts are `DECIMAL(28,10)`** | Never floats. JPY at 3bn and a 10-decimal FX rate must coexist |
| **Codes, not descriptions** | Store `currency_code = 'GBP'`, not `'Pound Sterling'`. Descriptions live in reference data |
| **Append-only where legally required** | Trade events, settlements, and valuations are never updated in place |
| **Nullability is a contract** | Mandatory canonical attributes are enforced at the boundary, not hoped for |

## 4.2 Canonical entity catalogue

---

### 4.2.1 `CanonicalTrade`

**Business definition:** a version of a legally binding bilateral agreement between our booking entity and a counterparty, at a point in its lifecycle.

**Grain:** one row per **trade version** (SCD Type 2 on the business key).
**Primary key:** `trade_sk` (surrogate)
**Business key:** `(source_system, source_trade_id, trade_version)`

| Attribute | Type | Description | FpML source |
|---|---|---|---|
| `trade_sk` | BIGINT | Surrogate PK | *derived* |
| `trade_id` | STRING | Enterprise trade identifier (stable across versions) | *derived / mastered* |
| `source_trade_id` | STRING | ID as issued by the booking system | `tradeHeader/partyTradeIdentifier/tradeId` (where `partyReference` = our party) |
| `counterparty_trade_id` | STRING | Their reference | `tradeHeader/partyTradeIdentifier/tradeId` (their party) |
| `uti` / `usi` | STRING | Unique Trade Identifier for regulatory reporting | `partyTradeIdentifier/tradeId[@tradeIdScheme='...unique-transaction-identifier']` |
| `trade_version` | INT | Monotonic version number | derived from event sequence / `<versionedTradeId>/<version>` |
| `trade_date` | DATE | Business date of execution | `tradeHeader/tradeDate` |
| `execution_timestamp` | TIMESTAMP | Precise execution time (UTC) | `partyTradeInformation/executionDateTime` |
| `effective_date` | DATE | Start of economic life | `fxSingleLeg/valueDate` (FX) / `swapStream/effectiveDate` |
| `settlement_date` | DATE | Contractual value date (single-leg products) | `fxSingleLeg/valueDate` |
| `maturity_date` | DATE | End of economic life | `farLeg/valueDate`, `terminationDate` |
| `product_sk` | BIGINT | FK → `CanonicalProduct` | `productType`, product element name |
| `asset_class` | STRING | `FX`, `RATES`, `CREDIT`, `EQUITY`, `COMMODITY` | derived from `productType` |
| `product_type` | STRING | `FX_SPOT`, `FX_FORWARD`, `FX_SWAP`, `FX_OPTION`, `NDF` | derived |
| `booking_entity_sk` | BIGINT | FK → `CanonicalParty`. **Our side** | `partyTradeInformation/partyReference` + `relatedParty[role=BookingParty]` |
| `counterparty_sk` | BIGINT | FK → `CanonicalParty`. **Their side** | the `<party>` that is not us |
| `trader_id` | STRING | Executing trader | `partyTradeInformation/trader` |
| `portfolio_id` / `book_id` | STRING | Risk book | `partyTradeInformation/category` or enrichment |
| `trade_status` | STRING | `NEW`, `CONFIRMED`, `AMENDED`, `CANCELLED`, `TERMINATED`, `NOVATED`, `MATURED`, `SETTLED` | derived from event stream |
| `confirmation_status` | STRING | `UNCONFIRMED`, `PENDING`, `CONFIRMED`, `DISPUTED` | `confirmationStatus` message |
| `clearing_status` | STRING | `BILATERAL`, `PENDING_CLEARING`, `CLEARED`, `REJECTED` | `clearingStatus` |
| `execution_venue` | STRING | MIC or `OffFacility` | `executionVenueType`, `<executionVenue>` |
| `is_intragroup` | BOOLEAN | Internal trade flag | derived from party hierarchy |
| `master_agreement_type` | STRING | `ISDA2002`, `IFEMA`, etc. | `documentation/masterAgreement/masterAgreementType` |
| `reporting_regimes` | ARRAY&lt;STRING&gt; | `EMIR`, `MiFIR`, `CFTC` | `partyTradeInformation/reportingRegime/name` |
| `parent_trade_sk` | BIGINT | Self-FK for novation/allocation/compression lineage | `novation/originalTrade`, `allocation` |
| `source_system` | STRING | `MUREX_FX`, `CALYPSO`, ... | ingestion metadata |
| `fpml_version` | STRING | e.g. `5-13` | `FpML/@fpmlVersion` |
| `source_message_id` | STRING | Provenance | `header/messageId` |
| `business_effective_from` / `_to` | TIMESTAMP | When this version was economically true | event time |
| `valid_from` / `valid_to` | TIMESTAMP | When the platform believed it (SCD2) | ingestion time |
| `is_current` | BOOLEAN | Current-version flag | derived |
| `created_timestamp` / `updated_timestamp` | TIMESTAMP | Audit | ingestion |

**Why it belongs in canonical:** the trade is the atomic unit of the business. Risk, Finance, Regulatory and Collateral all need the *same* trade with the *same* identity and the *same* notion of "current". Without a canonical trade, four departments produce four trade counts and reconcile forever.

**Sample row — TRD10001:**

| trade_id | source_trade_id | ver | trade_date | product_type | booking_entity | counterparty | settlement_date | status | is_current |
|---|---|---|---|---|---|---|---|---|---|
| `T-000010001` | `TRD10001` | 1 | 2026-09-01 | `FX_FORWARD` | `LE-GMB-LDN` | `CP-001` | 2026-09-03 | `SETTLED` | true |

---

### 4.2.2 `CanonicalTradeLeg` *(the supertype/subtype resolution)*

**Business definition:** one currency side of an FX trade, or one stream of a multi-stream derivative. This entity is what makes a single canonical model able to hold FX Spot, FX Forward, FX Swap and IRS without a 400-column trade table.

**Grain:** one row per trade version per leg.
**Primary key:** `trade_leg_sk`
**Business key:** `(trade_sk, leg_number)`

| Attribute | Type | Description | FpML source |
|---|---|---|---|
| `trade_leg_sk` | BIGINT | PK | derived |
| `trade_sk` | BIGINT | FK → `CanonicalTrade` | — |
| `leg_number` | INT | 1, 2 (FX); 1..n (swap streams) | position |
| `leg_type` | STRING | `NEAR`, `FAR`, `EXCHANGED_CCY_1`, `EXCHANGED_CCY_2`, `FIXED`, `FLOAT` | element name |
| `currency_code` | STRING | ISO 4217 | `paymentAmount/currency` |
| `notional_amount` | DECIMAL(28,10) | Leg amount | `paymentAmount/amount` |
| `payer_party_sk` | BIGINT | FK → party | `payerPartyReference/@href` |
| `receiver_party_sk` | BIGINT | FK → party | `receiverPartyReference/@href` |
| `direction` | STRING | `PAY` / `RECEIVE` **relative to booking entity** | *derived* |
| `value_date` | DATE | Leg value date (may differ — split value dates) | `valueDate`, `currency1ValueDate` |
| `exchange_rate` | DECIMAL(18,10) | All-in rate | `exchangeRate/rate` |
| `spot_rate` | DECIMAL(18,10) | Spot component | `exchangeRate/spotRate` |
| `forward_points` | DECIMAL(18,10) | Forward points | `exchangeRate/forwardPoints` |
| `quote_basis` | STRING | `Currency2PerCurrency1` / `Currency1PerCurrency2` | `quotedCurrencyPair/quoteBasis` |
| `currency_pair` | STRING | e.g. `GBP/USD` | derived from `quotedCurrencyPair` |
| `settlement_type` | STRING | `DELIVERABLE`, `NON_DELIVERABLE` | presence of `nonDeliverableSettlement` |
| `settlement_currency` | STRING | For NDFs | `nonDeliverableSettlement/settlementCurrency` |

**Why it belongs in canonical:** FX Swap has four cashflows across two legs at two value dates and two rates. Cramming that into `CanonicalTrade` forces `near_leg_ccy1_amount`, `far_leg_ccy2_rate`-style columns that break the moment you add a third product. The leg entity is the difference between a model that scales and one that doesn't.

**Sample rows — TRD10003 (FX Swap):**

| trade_leg_sk | trade | leg | leg_type | ccy | notional | direction | value_date | rate |
|---|---|---|---|---|---|---|---|---|
| 3001 | TRD10003 | 1 | NEAR | USD | 20,000,000 | RECEIVE | 2026-09-03 | 150.000 |
| 3002 | TRD10003 | 1 | NEAR | JPY | 3,000,000,000 | PAY | 2026-09-03 | 150.000 |
| 3003 | TRD10003 | 2 | FAR | USD | 20,000,000 | PAY | 2026-12-03 | 148.700 |
| 3004 | TRD10003 | 2 | FAR | JPY | 2,974,000,000 | RECEIVE | 2026-12-03 | 148.700 |

---

### 4.2.3 `CanonicalProduct`

**Business definition:** the classified instrument type traded, carrying taxonomy, regulatory classification and processing characteristics.

**Grain:** one row per product definition (SCD2).
**Primary key:** `product_sk` · **Business key:** `product_code`

| Attribute | Type | Description | FpML source |
|---|---|---|---|
| `product_sk` | BIGINT | PK | — |
| `product_code` | STRING | `FX_FORWARD` | derived |
| `product_name` | STRING | `Foreign Exchange Forward` | — |
| `asset_class` | STRING | `FX` | `productType` prefix |
| `product_family` | STRING | `FX_LINEAR`, `FX_OPTION` | derived |
| `product_subtype` | STRING | `OUTRIGHT_FORWARD`, `SPOT`, `SWAP`, `NDF` | derived |
| `isda_taxonomy_v2` | STRING | `ForeignExchange:Forward` | `productType[@productTypeScheme='...product-taxonomy']` |
| `cfi_code` | STRING | ISO 10962, e.g. `SFCXFX` | reference data |
| `upi` | STRING | **Unique Product Identifier** (ANNA DSB) — mandatory under EMIR REFIT / CFTC rewrite | `productId` / enrichment |
| `is_deliverable` | BOOLEAN | Physical vs cash settled | derived |
| `is_cleared_eligible` | BOOLEAN | CCP eligibility | reference data |
| `default_cashflow_count` | INT | 2 for spot/forward, 4 for swap | derived |
| `valuation_model` | STRING | `FX_FORWARD_DISCOUNT`, `GARMAN_KOHLHAGEN` | reference data |
| `fpml_product_element` | STRING | `fxSingleLeg`, `fxSwap` | XML element name |
| `mifir_instrument_class` | STRING | Regulatory class | reference data |

**Why it belongs in canonical:** FpML's `productType` is a free-ish string under a coding scheme, and different source systems will send `FXFWD`, `FX Outright`, `ForeignExchange:Forward` and `Forward`. Canonical resolves all of them to one `product_code`, and it is the join point for Risk models, Finance product hierarchies and regulatory classification. **Product is the dimension that makes cross-asset reporting possible.**

---

### 4.2.4 `CanonicalParty`

**Business definition:** a legal entity or an organisational unit that participates in trading or processing, together with its identifiers, classification and hierarchy.

**Grain:** one row per party version (SCD2).
**Primary key:** `party_sk` · **Business key:** `party_id`

| Attribute | Type | Description | FpML source |
|---|---|---|---|
| `party_sk` | BIGINT | PK | — |
| `party_id` | STRING | Enterprise party identifier | mastered |
| `lei` | STRING | ISO 17442 Legal Entity Identifier | `partyId[@partyIdScheme='...iso17442']` |
| `party_name` | STRING | Legal name | `partyName` |
| `party_type` | STRING | `LEGAL_ENTITY`, `BRANCH`, `FUND`, `DESK` | derived |
| `party_role` | STRING | see `CanonicalTradePartyRole` — **not stored here** | — |
| `is_internal` | BOOLEAN | Our entity vs external | mastered |
| `parent_party_sk` | BIGINT | Self-FK — legal hierarchy | mastered |
| `ultimate_parent_lei` | STRING | Group parent for exposure aggregation | GLEIF |
| `country_of_incorporation` | STRING | ISO 3166 | GLEIF / mastered |
| `jurisdiction` | STRING | Regulatory jurisdiction | mastered |
| `sector_classification` | STRING | `CREDIT_INSTITUTION`, `INVESTMENT_FIRM`, `NFC_PLUS`, `UCITS` | EMIR classification |
| `emir_classification` | STRING | `FC`, `NFC+`, `NFC-` | mastered |
| `credit_rating` | STRING | External rating | reference data |
| `bic` | STRING | SWIFT BIC | `routingIds/routingId[@...bic]` |
| `is_cls_member` | BOOLEAN | CLS settlement member | reference data |
| `netting_agreement_id` | STRING | Applicable netting agreement | mastered |
| `valid_from` / `valid_to` / `is_current` | | SCD2 | — |

> **Design decision worth defending:** *role is not an attribute of Party.* It is an attribute of the *relationship* between a Party and a Trade. Put `party_role` on `CanonicalParty` and ABC Bank needs a duplicate row every time it plays a different role. Use the associative entity below.

---

### 4.2.5 `CanonicalTradePartyRole` *(associative entity)*

**Grain:** one row per trade version per party per role.
**Primary key:** `trade_party_role_sk` · **Business key:** `(trade_sk, party_sk, party_role)`

| Attribute | Description | FpML source |
|---|---|---|
| `trade_sk` | FK → trade | — |
| `party_sk` | FK → party | `<party>` |
| `party_role` | `EXECUTING_PARTY`, `BOOKING_PARTY`, `COUNTERPARTY`, `CLEARING_BROKER`, `BENEFICIARY`, `GUARANTOR`, `CLEARING_ORGANIZATION`, `SETTLEMENT_AGENT`, `TRANSFEROR`, `TRANSFEREE` | `relatedParty/role`, position in the doc |
| `party_reference_href` | The document-scoped `@id` (`GMB`, `ABCBANK`) — kept for lineage/debugging | `@href` |
| `is_reporting_party` | Whose regulatory obligation | `partyTradeInformation/partyReference` |

**Why it belongs in canonical:** novations (transferor / transferee / remaining party), give-ups (executing broker / clearing broker), allocations (block / fund) and cleared trades (CCP as legal counterparty to both sides) all require more than two parties on a trade. This table is what lets the model represent them without schema change.

---

### 4.2.6 `CanonicalAccount`

**Business definition:** an account at a servicing institution against which cash is debited or credited.

**Primary key:** `account_sk` · **Business key:** `account_id`

| Attribute | Description | FpML source |
|---|---|---|
| `account_sk` | PK | — |
| `account_id` | Enterprise account identifier | `account/accountId` |
| `account_number` / `iban` | External identifier | `beneficiary/routingIds/routingId` |
| `account_name` | Descriptive name | `account/accountName` |
| `account_type` | `NOSTRO`, `VOSTRO`, `CLIENT`, `INTERNAL`, `SUSPENSE`, `CLS` | mastered |
| `currency_code` | Denomination currency | derived |
| `owner_party_sk` | FK → party (the beneficiary) | `accountBeneficiary/@href` |
| `servicing_party_sk` | FK → party (the agent bank) | `servicingParty/@href` |
| `agent_bic` | Agent BIC | `routingIds` |
| `gl_account_code` | Link into the general ledger | Finance mastering |
| `cost_centre` | Finance dimension | mastering |
| `is_active` | Status | mastered |

**Why it belongs in canonical:** the account is where the settled-cash model meets the general ledger. Without a canonical account carrying `gl_account_code`, Finance cannot post from your settlement data and you have built a reporting toy rather than a system of record.

---

### 4.2.7 `CanonicalCashflow`

**Business definition:** an amount of one currency contractually or forecast due to move between our booking entity and a counterparty on a stated date as a consequence of a trade. **This is expectation, not fact.**

**Grain:** one row per cashflow per trade version.
**Primary key:** `cashflow_sk` · **Business key:** `(trade_sk, leg_number, cashflow_seq)`

| Attribute | Type | Description | FpML source |
|---|---|---|---|
| `cashflow_sk` | BIGINT | PK | — |
| `cashflow_id` | STRING | Business identifier, e.g. `CF-10001-01` | derived |
| `trade_sk` | BIGINT | FK → trade version | — |
| `trade_leg_sk` | BIGINT | FK → leg | — |
| `cashflow_type` | STRING | `PRINCIPAL_EXCHANGE`, `INTEREST`, `PREMIUM`, `FEE`, `COMPENSATION_CLAIM`, `COLLATERAL_INTEREST` | element context |
| `cashflow_basis` | STRING | `CONTRACTUAL`, `FORECAST`, `RESET_FIXED`, `ACTUAL` | derived |
| `currency_code` | STRING | ISO 4217 | `paymentAmount/currency` |
| `expected_amount` | DECIMAL(28,10) | Amount due — **always positive** | `paymentAmount/amount` |
| `payment_direction` | STRING | `PAY` / `RECEIVE` relative to booking entity | derived from payer/receiver |
| `signed_amount` | DECIMAL(28,10) | `expected_amount × (PAY ? -1 : +1)` | derived |
| `value_date` | DATE | **Contractual** date cash is due | `valueDate` / `adjustedPaymentDate` |
| `unadjusted_payment_date` | DATE | Before business-day adjustment | `unadjustedPaymentDate` |
| `payer_party_sk` / `receiver_party_sk` | BIGINT | FKs → party | `payerPartyReference` / `receiverPartyReference` |
| `booking_entity_sk` / `counterparty_sk` | BIGINT | Denormalised for query | derived |
| `account_sk` | BIGINT | FK → the account expected to be hit | SSI resolution |
| `settlement_instruction_sk` | BIGINT | FK → SSI applied | SSI resolution |
| `cashflow_status` | STRING | `EXPECTED`, `INSTRUCTED`, `PARTIALLY_SETTLED`, `SETTLED`, `FAILED`, `CANCELLED`, `SUPERSEDED` | derived |
| `settled_amount` | DECIMAL(28,10) | Running total from settlements | aggregated |
| `unsettled_amount` | DECIMAL(28,10) | `expected − settled` | derived |
| `netting_group_id` | STRING | Which netting set this cashflow joined | TPP netting |
| `is_netted` | BOOLEAN | Netted vs gross settled | TPP |
| `discount_factor` | DECIMAL(18,12) | For forecast cashflows | `discountFactor` |
| `source_system` / `valid_from` / `valid_to` / `is_current` | | | |

**Why it belongs in canonical:** cashflow is the **bridge between the trade world and the cash world**. Risk stops at the trade; Treasury and Finance start at the cashflow. It is also the level at which "what should have happened" is stated, without which "what did happen" has nothing to reconcile against.

**Sample rows:**

| cashflow_id | trade | type | ccy | expected_amount | dir | signed | value_date | status | settled |
|---|---|---|---|---|---|---|---|---|---|
| CF-10001-01 | TRD10001 | PRINCIPAL_EXCHANGE | GBP | 10,000,000.00 | RECEIVE | +10,000,000.00 | 2026-09-03 | SETTLED | 10,000,000.00 |
| CF-10001-02 | TRD10001 | PRINCIPAL_EXCHANGE | USD | 13,200,000.00 | PAY | −13,200,000.00 | 2026-09-03 | SETTLED | 13,200,000.00 |
| CF-10003-02 | TRD10003 | PRINCIPAL_EXCHANGE | JPY | 3,000,000,000.00 | PAY | −3,000,000,000.00 | 2026-09-03 | SETTLED | 3,000,000,000.00 |
| CF-10003-03 | TRD10003 | PRINCIPAL_EXCHANGE | USD | 20,000,000.00 | PAY | −20,000,000.00 | 2026-12-03 | EXPECTED | 0.00 |
| CF-10005-01 | TRD10005 v1 | PRINCIPAL_EXCHANGE | GBP | 8,000,000.00 | RECEIVE | +8,000,000.00 | 2026-12-03 | SUPERSEDED | 0.00 |

---

### 4.2.8 `CanonicalPayment`

**Business definition:** an instruction to move a specified amount of cash on a specified date through a specified mechanism, discharging one or more cashflows. **This is action, not fact.**

**Grain:** one row per payment instruction (including retries).
**Primary key:** `payment_sk` · **Business key:** `payment_id`

| Attribute | Type | Description |
|---|---|---|
| `payment_sk` | BIGINT | PK |
| `payment_id` | STRING | e.g. `PMT-90002` |
| `parent_payment_sk` | BIGINT | Self-FK — set on a retry after a fail |
| `payment_type` | STRING | `CLIENT_PAYMENT`, `NOSTRO_TRANSFER`, `CLS_PAYIN`, `CLAIM`, `RETURN` |
| `currency_code` | STRING | ISO 4217 |
| `payment_amount` | DECIMAL(28,10) | Instructed amount (may be a **net** of many cashflows) |
| `payment_direction` | STRING | `PAY` / `RECEIVE` |
| `value_date` | DATE | Requested value date |
| `payment_method` | STRING | `CLS`, `SWIFT_MT202`, `SWIFT_MT103`, `PACS_009`, `CHAPS`, `TARGET2`, `FEDWIRE`, `BOOK_TRANSFER` |
| `settlement_system` | STRING | `CLS`, `T2`, `FEDWIRE`, `CHIPS`, `BOJNET`, `CHAPS` |
| `debit_account_sk` | BIGINT | FK → account (for PAY) |
| `credit_account_sk` | BIGINT | FK → account (for RECEIVE) |
| `counterparty_sk` | BIGINT | FK → party |
| `beneficiary_bank_bic` / `correspondent_bic` | STRING | Routing |
| `payment_reference` | STRING | End-to-end reference sent on the wire (**the key you reconcile on**) |
| `uetr` | STRING | SWIFT gpi Unique End-to-end Transaction Reference (UUIDv4) |
| `payment_status` | STRING | `CREATED`, `PENDING_APPROVAL`, `INSTRUCTED`, `RELEASED`, `MATCHED`, `SETTLED`, `PARTIALLY_SETTLED`, `FAILED`, `CANCELLED`, `RETURNED` |
| `netting_group_id` | STRING | Netting set |
| `is_net_payment` | BOOLEAN | True if it discharges >1 cashflow |
| `instructed_timestamp` / `released_timestamp` | TIMESTAMP | |
| `cancellation_reason` | STRING | |
| `source_system` | STRING | |

Plus the bridge:

### `CanonicalPaymentCashflowAllocation`

| Attribute | Description |
|---|---|
| `payment_sk` | FK |
| `cashflow_sk` | FK |
| `allocated_amount` | Portion of the payment attributable to this cashflow |
| `allocation_method` | `FULL`, `NET`, `PRORATA`, `SPLIT` |

**Why it belongs in canonical:** this bridge is what makes netting representable and what allows a *net* nostro movement to be attributed back to *gross* trade-level cashflows. Every Finance question of the form "which trades made up this GBP 47m debit?" is answered here.

---

### 4.2.9 `CanonicalSettlement`

**Business definition:** the confirmed, final and irrevocable movement of cash, evidenced by an authoritative external source (CLS report, camt.054, camt.053, nostro rec). **This is fact.**

**Grain:** one row per confirmed movement (a payment may have several — partials, tranches).
**Primary key:** `settlement_sk` · **Business key:** `settlement_id`
**Append-only.**

| Attribute | Type | Description |
|---|---|---|
| `settlement_sk` | BIGINT | PK |
| `settlement_id` | STRING | e.g. `STL-90002` |
| `payment_sk` | BIGINT | FK → payment |
| `cashflow_sk` | BIGINT | FK → cashflow (denormalised for query performance) |
| `trade_sk` | BIGINT | FK → trade (denormalised) |
| `account_sk` | BIGINT | FK → account actually hit |
| `currency_code` | STRING | ISO 4217 |
| `settled_amount` | DECIMAL(28,10) | **Actual** amount moved |
| `settlement_direction` | STRING | `PAY` / `RECEIVE` |
| `signed_settled_amount` | DECIMAL(28,10) | Signed for summation |
| `value_date` | DATE | Contractual date (carried from cashflow) |
| `settlement_date` | DATE | **Actual** date of finality |
| `settlement_timestamp` | TIMESTAMP | Time of finality |
| `settlement_status` | STRING | `SETTLED`, `PARTIALLY_SETTLED`, `FAILED`, `CANCELLED`, `REVERSED`, `RETURNED` |
| `settlement_method` | STRING | `CLS`, `PVP`, `RTGS_GROSS`, `NET`, `BOOK_TRANSFER` |
| `fail_reason_code` | STRING | `INSUFFICIENT_FUNDS`, `SSI_MISMATCH`, `CUTOFF_MISSED`, `COMPLIANCE_HOLD`, `INVALID_ACCOUNT` |
| `days_late` | INT | `settlement_date − value_date` (business days) |
| `confirmation_source` | STRING | `CLS_REPORT`, `CAMT054`, `CAMT053`, `MT950`, `MANUAL` |
| `confirmation_reference` | STRING | External reference from the confirming message |
| `bank_statement_id` | STRING | Link to the camt.053 statement entry |
| `reversal_of_settlement_sk` | BIGINT | Self-FK for reversals |
| `is_reversed` | BOOLEAN | |
| `reconciliation_status` | STRING | `UNMATCHED`, `MATCHED`, `BREAK`, `AGED_BREAK` |
| `created_timestamp` | TIMESTAMP | |

**Why it belongs in canonical:** because *nothing else in the model is true*. Trades, cashflows and payments are all statements of intent. Settlement is the only entity backed by an external, independent, auditable source. It is the anchor for Treasury liquidity, Finance cash accounting, nostro reconciliation and settlement-risk reporting.

---

### 4.2.10 `CanonicalSettlementInstruction` (SSI)

**Business definition:** the standing or trade-specific directions determining where and how cash for a party, currency and product should be delivered.

**Primary key:** `ssi_sk` · **Business key:** `(party_sk, currency_code, product_family, effective_date)` — SCD2

| Attribute | Description | FpML source |
|---|---|---|
| `ssi_sk` | PK | — |
| `ssi_id` | Business identifier | `standingSettlementInstruction` |
| `party_sk` | FK → party the SSI belongs to | `partyReference` |
| `currency_code` | ISO 4217 | — |
| `product_family` | `FX`, `MM`, `DERIV` — SSIs are product-scoped | — |
| `settlement_method` | `CLS`, `SWIFT`, `RTGS`, `BOOK` | `settlementMethod` |
| `beneficiary_account_number` / `iban` | Where the money goes | `beneficiary/routingIds` |
| `beneficiary_bank_bic` | Beneficiary bank | `beneficiaryBank/routingIds` |
| `intermediary_bank_bic` | Intermediary | `intermediaryInformation/routingIds` |
| `correspondent_bank_bic` | Correspondent | `correspondentInformation/routingIds` |
| `is_split_settlement` | Split across accounts | `splitSettlement` |
| `effective_from` / `effective_to` | SSI validity window | `effectiveDate` |
| `source` | `SWIFT_SSI`, `ALERT` (DTCC), `MANUAL`, `TRADE_SPECIFIC` | — |
| `is_verified` | Dual-control verified | internal |

**Why it belongs in canonical:** SSI errors are a leading cause of settlement fails and of fraud loss. Modelling SSI as first-class, versioned, and dual-control-verified — rather than as five string columns copied onto each payment — is what lets you answer "which payments used the SSI that changed on 12-Aug?" after an incident.

---

### 4.2.11 `CanonicalTradeEvent`

**Business definition:** an immutable record of a discrete occurrence affecting a trade's economics or processing state.

**Grain:** one row per event. **Append-only. Never updated.**
**Primary key:** `trade_event_sk` · **Business key:** `(source_system, source_message_id)`

| Attribute | Type | Description | FpML source |
|---|---|---|---|
| `trade_event_sk` | BIGINT | PK | — |
| `event_id` | STRING | Business event identifier | `eventIdentifier/eventId` |
| `trade_sk` | BIGINT | FK → the trade version this event produced | — |
| `trade_id` | STRING | Stable trade identifier | — |
| `event_type` | STRING | `NEW`, `CONFIRM_REQUESTED`, `CONFIRMED`, `AMEND`, `INCREASE`, `PARTIAL_TERMINATION`, `TERMINATE`, `NOVATE`, `EXERCISE`, `ALLOCATE`, `CLEARED`, `CANCEL`, `MATURED`, `SETTLEMENT_FAILED`, `SETTLED`, `VALUATION` | message root element |
| `event_qualifier` | STRING | `NOTIONAL_DECREASE`, `RATE_CHANGE`, `DATE_CHANGE` | diff of original vs amended |
| `event_datetime` | TIMESTAMP | **Business time** of the event | `executionDateTime`, `eventDate` |
| `effective_date` | DATE | Business date the event takes effect | `effectiveDate` |
| `message_timestamp` | TIMESTAMP | **System time** the message was created | `header/creationTimestamp` |
| `ingestion_timestamp` | TIMESTAMP | When we received it | platform |
| `resulting_trade_version` | INT | Version produced | derived |
| `previous_trade_version` | INT | Version superseded | derived |
| `is_correction` | BOOLEAN | Replaces a previous message | `isCorrection` |
| `correlation_id` | STRING | Groups the message chain | `correlationId` |
| `sequence_number` | INT | Ordering within a correlation | `sequenceNumber` |
| `source_message_id` | STRING | **Idempotency key** | `header/messageId` |
| `sent_by` / `sent_to` | STRING | Message routing | `header/sentBy`, `sendTo` |
| `raw_payload_ref` | STRING | Pointer to the bronze record | platform |

**Why it belongs in canonical:** this is your event-sourcing spine. Given the event table you can rebuild every trade version, answer "what did we believe on 15-Sep at 14:00?", replay after a bug, and satisfy any audit or regulator question about when the firm knew what. **Without it, your SCD2 trade table is an assertion; with it, it is a derivation.**

**Sample rows — TRD10001:**

| event_id | event_type | event_datetime | resulting_ver | correlation_id | seq | message_id |
|---|---|---|---|---|---|---|
| EV-0001 | NEW | 2026-09-01T14:31:58Z | 1 | CORR-TRD10001 | 1 | MSG-20260901-000451 |
| EV-0002 | CONFIRM_REQUESTED | 2026-09-01T14:33:02Z | 1 | CORR-TRD10001 | 2 | MSG-20260901-000478 |
| EV-0003 | CONFIRMED | 2026-09-01T14:47:15Z | 1 | CORR-TRD10001 | 3 | MSG-20260901-000512 |
| EV-0004 | VALUATION | 2026-09-02T21:00:00Z | 1 | CORR-VAL-0902 | 1 | MSG-20260902-004411 |
| EV-0005 | SETTLED | 2026-09-03T09:12:41Z | 1 | CORR-STL-0903 | 1 | CLS-RPT-20260903-0088 |

---

### 4.2.12 `CanonicalValuation`

**Business definition:** a measurement of the economic value or risk sensitivity of a trade or position at a valuation date, under a stated methodology and currency.

**Grain:** one row per trade per valuation date per measure type per source. **Append-only.**
**Primary key:** `valuation_sk` · **Business key:** `(trade_sk, valuation_date, measure_type, valuation_source)`

| Attribute | Type | Description | FpML source |
|---|---|---|---|
| `valuation_sk` | BIGINT | PK | — |
| `trade_sk` | BIGINT | FK → trade version | — |
| `valuation_date` | DATE | Business date of the valuation | `valuationDate` |
| `valuation_timestamp` | TIMESTAMP | Snapshot time | `valuationDateTime` |
| `measure_type` | STRING | `NPV`, `MARKET_VALUE`, `PV01`, `DELTA`, `GAMMA`, `VEGA`, `ACCRUED_INTEREST`, `CLEAN_PRICE` | `quotation/measureType` |
| `measure_value` | DECIMAL(28,10) | The number | `quotation/value` |
| `valuation_currency` | STRING | Currency of the measure | `quotation/currency` |
| `base_currency_value` | DECIMAL(28,10) | Converted to reporting currency | derived |
| `fx_rate_to_base` | DECIMAL(18,10) | Rate used | market data |
| `valuation_source` | STRING | `RISK_ENGINE`, `COUNTERPARTY`, `CCP`, `INDEPENDENT_PRICE_VERIFICATION` | `quotation/informationSource` |
| `valuation_side` | STRING | `Mid`, `Bid`, `Ask` | `quotation/side` |
| `pricing_model` | STRING | `FX_FORWARD_DISCOUNT` | internal |
| `market_data_snapshot_id` | STRING | Which curve/fixing set was used — **essential for reproducibility** | internal |
| `is_official` | BOOLEAN | Official EOD mark vs intraday | internal |

**Why it belongs in canonical:** Risk, Finance (fair value / IFRS 13), Collateral (margin calls) and Regulatory (EMIR daily valuation reporting) all consume valuations and all need to agree. Storing measure-type as a *row* rather than a *column* means adding a new Greek does not require a schema change — a pattern that repeatedly proves itself.

**Sample row — TRD10001:**

| trade | valuation_date | measure_type | value | ccy | source | is_official |
|---|---|---|---|---|---|---|
| TRD10001 | 2026-09-02 | `NPV` | 49,994.00 | USD | RISK_ENGINE | true |

*(GBP/USD forward moved 1.3200 → 1.3250; long GBP 10,000,000 gains USD 50,000, discounted one day at 4.35% ≈ USD 49,994.)*

---

### 4.2.13 `CanonicalPosition` *(derived, included for completeness)*

**Grain:** one row per position date per position key.
**Business key:** `(position_date, booking_entity_sk, counterparty_sk, currency_code, product_sk, value_date_bucket)`

| Attribute | Description |
|---|---|
| `position_date` | As-at business date |
| `booking_entity_sk` / `counterparty_sk` / `currency_code` / `product_sk` | Position dimensions |
| `value_date_bucket` | `T0`, `T1`, `T2`, `1W`, `1M`, `3M`, `6M`, `1Y`, `>1Y` |
| `gross_long_amount` / `gross_short_amount` / `net_amount` | Notional aggregates |
| `net_amount_base_ccy` | Converted to reporting currency |
| `trade_count` | Constituent trades |
| `mtm_value_base_ccy` | Aggregated NPV |

---

## 4.3 Canonical model — entity relationship summary

```
 CanonicalParty ──1:N──┬── CanonicalTradePartyRole ──N:1── CanonicalTrade
        │              │                                        │ 1
        │ 1            └── CanonicalAccount ◄──┐                 │
        │ N                    │ 1             │                 │ N
 CanonicalSettlementInstruction│               │        CanonicalTradeLeg
        │ 1                    │ N             │                 │ 1
        │ N                    │               │                 │ N
        └────────────► CanonicalCashflow ──────┘        CanonicalCashflow
                              │ N                                │
                              │                                  │
              CanonicalPaymentCashflowAllocation                 │
                              │                                  │
                              │ N                                │
                       CanonicalPayment                          │
                              │ 1                                │
                              │ N                                │
                      CanonicalSettlement ◄──── account ─────────┘

 CanonicalTrade ──1:N──► CanonicalTradeEvent    (append-only spine)
 CanonicalTrade ──1:N──► CanonicalValuation     (append-only)
 CanonicalTrade ──N:1──► CanonicalProduct
```

## 4.4 Why *these* entities, and not others

| Entity | Justification for canonical status |
|---|---|
| `CanonicalTrade` | The atomic contractual unit; every consumer needs it |
| `CanonicalTradeLeg` | Makes multi-leg products representable without schema explosion |
| `CanonicalProduct` | The conformed classification that enables cross-asset reporting |
| `CanonicalParty` | Identity resolution and exposure aggregation depend on one party master |
| `CanonicalTradePartyRole` | Supports >2 parties: novation, clearing, allocation, give-up |
| `CanonicalAccount` | The junction between market operations and the general ledger |
| `CanonicalCashflow` | The bridge from trade world to cash world; the "expected" side of settled cash |
| `CanonicalPayment` | Where netting happens; the unit that goes on the wire |
| `CanonicalSettlement` | The only externally-verified fact in the model |
| `CanonicalSettlementInstruction` | Operational risk control and fail root-cause analysis |
| `CanonicalTradeEvent` | Bitemporality, replay, audit, idempotency |
| `CanonicalValuation` | Fair value, margin, EMIR daily valuation, P&L attribution |

**The test to apply:** an entity belongs in canonical if (a) more than one source produces it, **or** (b) more than one consumer needs it, **and** (c) it has an independent business identity and lifecycle. `Netting group`, by that test, is an attribute — not an entity. `Settlement` passes on all three counts.

---
# PART 5 — FpML → CANONICAL MAPPING

Mapping is where the design becomes real. A mapping specification is not a two-column list; a production-grade one has **five** columns: source XPath, target attribute, transformation rule, business meaning, and the failure mode when it goes wrong.

## 5.1 Mapping notation used

```
FpML XPath
      ↓  [transformation rule]
Canonical attribute
      ↓
Business meaning
```

## 5.2 Trade identity and header (mappings 1–10)

| # | FpML XPath | → Canonical attribute | Transformation rule | Business meaning |
|---|---|---|---|---|
| 1 | `FpML/@fpmlVersion` | `CanonicalTrade.fpml_version` | Direct | Which schema contract this message honours; drives parser dispatch and schema-evolution audit |
| 2 | `FpML/@xsi:type` or root element name | `CanonicalTradeEvent.event_type` | Lookup: `ExecutionNotification`→`NEW`, `RequestAmendmentConfirmation`→`AMEND`, `WithdrawalNotification`→`CANCEL` | The lifecycle transition being asserted |
| 3 | `header/messageId` | `CanonicalTradeEvent.source_message_id` | Direct; **UNIQUE with `sent_by`** | Physical message identity — the idempotency key. Redelivery of the same messageId must be a no-op |
| 4 | `header/creationTimestamp` | `CanonicalTradeEvent.message_timestamp` | Parse to UTC | *System* time. Never use this as the business event date |
| 5 | `header/sentBy` | `CanonicalTradeEvent.sent_by` | Direct (usually an LEI) | Who asserted this. Determines whose "view" the message carries |
| 6 | `correlationId` | `CanonicalTradeEvent.correlation_id` | Direct | Threads request → response → status into one business conversation |
| 7 | `sequenceNumber` | `CanonicalTradeEvent.sequence_number` | Integer | Ordering key for out-of-order delivery. Within a `correlation_id`, highest sequence wins |
| 8 | `isCorrection` | `CanonicalTradeEvent.is_correction` | Boolean | `true` = replace prior assertion (UPSERT); `false` = new assertion (INSERT). Getting this wrong double-counts trades |
| 9 | `tradeHeader/partyTradeIdentifier[partyReference/@href = $ourPartyRef]/tradeId` | `CanonicalTrade.source_trade_id` | Select the identifier whose `partyReference` resolves to our LEI | **Our** booking reference — how the front office and operations refer to the trade |
| 10 | `tradeHeader/partyTradeIdentifier[partyReference/@href != $ourPartyRef]/tradeId` | `CanonicalTrade.counterparty_trade_id` **and** a row in `trade_id_alias` | Select the other identifier(s) | **Their** reference — required for confirmation matching, dispute resolution and reconciliation |

> **Failure mode for #9/#10:** naively taking `partyTradeIdentifier[1]` works until the day the counterparty's system emits identifiers in the other order. Always resolve by `partyReference`, never by position. This one line has caused production incidents at real banks.

## 5.3 Dates, versions and status (mappings 11–18)

| # | FpML XPath | → Canonical attribute | Transformation rule | Business meaning |
|---|---|---|---|---|
| 11 | `tradeHeader/tradeDate` | `CanonicalTrade.trade_date` | `xs:date` → `DATE` | Business date of execution. **The date Finance and Risk book to.** Drives T+n calculations and regulatory reporting deadlines |
| 12 | `tradeHeader/partyTradeInformation/executionDateTime` | `CanonicalTrade.execution_timestamp` | Parse with timezone → UTC | Precise moment of execution. MiFIR RTS 25 clock-sync obligation; used to order same-day events |
| 13 | `tradeHeader/partyTradeInformation/timestamps/submittedForConfirmation` | `CanonicalTrade.submitted_for_confirmation_ts` | Parse to UTC | Confirmation-timeliness KPI (EMIR Art. 11 confirmation deadlines) |
| 14 | `tradeHeader/partyTradeIdentifier/versionedTradeId/version`, else derived from event sequence | `CanonicalTrade.trade_version` | Monotonic integer per `trade_id` | Which economic version of the contract this is. **The SCD2 driver** |
| 15 | `fxSingleLeg/valueDate` | `CanonicalTrade.settlement_date`, `CanonicalTradeLeg.value_date`, `CanonicalCashflow.value_date` | `xs:date` → `DATE` | **Contractual** date on which cash is due. Propagates to every downstream cash object |
| 16 | `fxSwap/nearLeg/valueDate` | `CanonicalTradeLeg.value_date` (leg 1) | Direct | Near-leg settlement date |
| 17 | `fxSwap/farLeg/valueDate` | `CanonicalTradeLeg.value_date` (leg 2), `CanonicalTrade.maturity_date` | Direct | Far-leg settlement date; the trade's economic end |
| 18 | *(no FpML source)* | `CanonicalTrade.trade_status` | Derived state machine over `CanonicalTradeEvent` | The firm's view of where the trade is. **Not in FpML** — FpML sends events, not states. Deriving state from events is a canonical-layer responsibility |

## 5.4 Parties and perspective (mappings 19–26)

| # | FpML XPath | → Canonical attribute | Transformation rule | Business meaning |
|---|---|---|---|---|
| 19 | `party/partyId[@partyIdScheme='http://www.fpml.org/coding-scheme/external/iso17442']` | `CanonicalParty.lei` | Validate ISO 17442 (20 chars, ISO 7064 MOD 97-10 checksum); look up in GLEIF | The globally unique legal identifier. **The single best join key in capital markets** |
| 20 | `party/partyName` | `CanonicalParty.party_name` | Direct, but **do not master on it** | Human-readable name. Use for display only — names change, LEIs don't |
| 21 | `party/@id` (e.g. `GMB`, `ABCBANK`) | `CanonicalTradePartyRole.party_reference_href` | Direct, **document-scoped only** | The XML anchor other elements point at. Resolve all `@href` against it, then discard for keys. It is *not* an enterprise identifier |
| 22 | `tradeHeader/partyTradeInformation/partyReference/@href` | `CanonicalTrade.booking_entity_sk` | Resolve `@href` → `<party>` → LEI → party master | **This is us.** Establishes the perspective the whole record is written from |
| 23 | The `<party>` that is neither `booking_entity` nor an ancillary role | `CanonicalTrade.counterparty_sk` | Set complement | **This is them.** The counterparty concept FpML does not have |
| 24 | `partyTradeInformation/relatedParty[role='BookingParty']/partyReference` | `CanonicalTrade.booking_entity_sk` (branch/desk level) | Resolve `@href` | The booking branch — GMB-LDN vs GMB-NY. **Finance and regulatory reporting differ by branch, not just by group LEI** |
| 25 | `partyTradeInformation/relatedParty[role='ClearingOrganization' \| 'Guarantor' \| ...]` | rows in `CanonicalTradePartyRole` | One row per related party | Supports cleared, guaranteed, given-up and novated trades without schema change |
| 26 | `partyTradeInformation/trader` | `CanonicalTrade.trader_id` | Map to HR/desk master | Attribution of P&L and of conduct-risk monitoring |

**The perspective transformation, spelled out:**

```
FpML (symmetric):                    Canonical (asymmetric):
  party GMB      ─┐                    booking_entity_sk = LE-GMB-LDN
  party ABCBANK  ─┤ neither is         counterparty_sk   = CP-001 (ABC Bank)
                  │ privileged
  partyTradeInformation/partyReference = "GMB"  ──► "GMB is us"
```

## 5.5 Product (mappings 27–32)

| # | FpML XPath | → Canonical attribute | Transformation rule | Business meaning |
|---|---|---|---|---|
| 27 | Product element name (`fxSingleLeg` / `fxSwap` / `fxOption` / `swap`) | `CanonicalProduct.fpml_product_element`, `CanonicalTrade.asset_class` | XML local-name → lookup | The structural product family; determines which subtype/leg pattern applies |
| 28 | `fxSingleLeg/productType[@productTypeScheme='...product-taxonomy']` | `CanonicalProduct.isda_taxonomy_v2` and, split on `:`, `asset_class` + `product_subtype` | `ForeignExchange:Forward` → `FX` + `OUTRIGHT_FORWARD` | The ISDA taxonomy classification used across the industry and by trade repositories |
| 29 | *(derived)* `valueDate` vs the pair's spot date | `CanonicalTrade.product_type` = `FX_SPOT` \| `FX_FORWARD` | `value_date <= spot_date(pair, trade_date)` → SPOT | The **economic** classification, independent of what the source system labelled it. Raise a DQ warning on disagreement with #28 |
| 30 | `fxSingleLeg/nonDeliverableSettlement` (presence) | `CanonicalTradeLeg.settlement_type` = `NON_DELIVERABLE` | Existence test | NDFs settle net in one currency — **completely different cashflow generation.** Missing this creates phantom cashflows in restricted currencies |
| 31 | `fxSingleLeg/nonDeliverableSettlement/settlementCurrency` | `CanonicalTradeLeg.settlement_currency` | Direct | Which currency the net difference actually moves in |
| 32 | `productId` / enriched | `CanonicalProduct.upi` | ANNA DSB lookup | Unique Product Identifier — mandatory field under EMIR REFIT and the CFTC rewrite |

## 5.6 Cashflow and amounts (mappings 33–41)

| # | FpML XPath | → Canonical attribute | Transformation rule | Business meaning |
|---|---|---|---|---|
| 33 | `fxSingleLeg/exchangedCurrency1/paymentAmount/amount` | `CanonicalCashflow.expected_amount` (cashflow 1), `CanonicalTradeLeg.notional_amount` | `DECIMAL(28,10)`; **always positive** | The gross amount of currency 1 due to move |
| 34 | `fxSingleLeg/exchangedCurrency1/paymentAmount/currency` | `CanonicalCashflow.currency_code` | Validate against ISO 4217; check `@currencyScheme` | Denomination. Drives holiday calendar, cut-off time, decimal precision, CLS eligibility |
| 35 | `fxSingleLeg/exchangedCurrency2/paymentAmount/amount` | `CanonicalCashflow.expected_amount` (cashflow 2) | As #33 | The gross amount of currency 2 |
| 36 | `exchangedCurrency*/payerPartyReference/@href` | `CanonicalCashflow.payer_party_sk` | Resolve `@href` → party | Who owes the money |
| 37 | `exchangedCurrency*/receiverPartyReference/@href` | `CanonicalCashflow.receiver_party_sk` | Resolve `@href` → party | Who is owed the money |
| 38 | *(derived from #36/#37)* | `CanonicalCashflow.payment_direction` | `IF payer = booking_entity THEN 'PAY' ELSE 'RECEIVE'` | **Direction from the firm's point of view.** The single most business-critical derivation in the whole mapping |
| 39 | *(derived from #33/#38)* | `CanonicalCashflow.signed_amount` | `expected_amount × CASE direction WHEN 'PAY' THEN -1 ELSE 1 END` | Allows `SUM()` to produce a net cash position without a CASE expression in every query |
| 40 | `cashflows/paymentCalculationPeriod/forecastPaymentAmount/amount` (IRS) | `CanonicalCashflow.expected_amount`, `cashflow_basis='FORECAST'` | Direct | Projected coupon — an *estimate* that will change with each fixing. Must be distinguishable from contractual FX amounts |
| 41 | `cashflows/paymentCalculationPeriod/adjustedPaymentDate` | `CanonicalCashflow.value_date` | Direct | Business-day-adjusted payment date. Prefer `adjusted` over `unadjusted` for cash operations; store both |

## 5.7 Rates (mappings 42–46)

| # | FpML XPath | → Canonical attribute | Transformation rule | Business meaning |
|---|---|---|---|---|
| 42 | `fxSingleLeg/exchangeRate/rate` | `CanonicalTradeLeg.exchange_rate` | `DECIMAL(18,10)` | The all-in dealt rate — what the client got. Drives P&L versus mid |
| 43 | `exchangeRate/spotRate` | `CanonicalTradeLeg.spot_rate` | Direct | Spot component. Separating spot from points is how FX P&L is attributed between spot desk and forwards desk |
| 44 | `exchangeRate/forwardPoints` | `CanonicalTradeLeg.forward_points` | Direct | Interest-rate differential component. **DQ rule: `spot_rate + forward_points = rate` within tolerance** |
| 45 | `exchangeRate/quotedCurrencyPair/quoteBasis` | `CanonicalTradeLeg.quote_basis` | Enum | `Currency2PerCurrency1` vs `Currency1PerCurrency2`. **Ignoring this inverts every rate in the warehouse.** Normalise to a house convention and keep the original |
| 46 | `quotedCurrencyPair/currency1` + `currency2` | `CanonicalTradeLeg.currency_pair` | Concatenate in FpML order | The pair as quoted. Note FpML's currency1/2 is not necessarily market convention order — normalise for reporting, retain as dealt for confirmation |

## 5.8 Settlement instruction (mappings 47–53)

| # | FpML XPath | → Canonical attribute | Transformation rule | Business meaning |
|---|---|---|---|---|
| 47 | `settlementInformation/settlementInstruction/settlementMethod` | `CanonicalPayment.payment_method`, `CanonicalSettlementInstruction.settlement_method` | Map scheme value → house enum (`CLS`→`CLS`, `Gross`→`RTGS_GROSS`) | The rail. Determines settlement risk (PVP vs free-of-payment), timing, cut-offs and which confirmation source to expect |
| 48 | `settlementInstruction/beneficiary/routingIds/routingId[@routingIdCodeScheme='...accountNumber' \| '...iban']` | `CanonicalSettlementInstruction.beneficiary_account_number` / `iban` | Validate IBAN check digits | Where the money lands |
| 49 | `settlementInstruction/beneficiaryBank/routingIds/routingId[@...bic]` | `CanonicalSettlementInstruction.beneficiary_bank_bic` | Validate BIC (ISO 9362) | The beneficiary's bank |
| 50 | `settlementInstruction/correspondentInformation/correspondentBank/routingIds/routingId` | `CanonicalSettlementInstruction.correspondent_bank_bic` | Validate BIC | The correspondent in the payment chain |
| 51 | `settlementInstruction/intermediaryInformation/...` | `CanonicalSettlementInstruction.intermediary_bank_bic` | Validate BIC | Additional hop; drives fees and settlement lag |
| 52 | `settlementInstruction/splitSettlement` | `CanonicalSettlementInstruction.is_split_settlement` = true; N payment rows | Existence test → fan out | One cashflow settling across multiple accounts. **The reason `Cashflow → Payment` must be 1:N** |
| 53 | `standingSettlementInstruction/@href` or `ssiId` | `CanonicalCashflow.settlement_instruction_sk` | Resolve to the SSI master, version-aware **as at value date** | Which SSI version was actually applied — the audit trail for a fail or a fraud investigation |

## 5.9 Business events (mappings 54–59)

| # | FpML XPath | → Canonical attribute | Transformation rule | Business meaning |
|---|---|---|---|---|
| 54 | `amendment/originalTrade` vs `amendment/amendedTrade` | `CanonicalTradeEvent.event_qualifier` | Structural diff; classify what changed | Not just "an amendment" — a *notional decrease*, a *date roll*, a *rate correction*. Finance and Risk treat these differently |
| 55 | `terminationNotification/termination/changeInNotional/amount` | `CanonicalTradeEvent` + new `CanonicalTrade` version with reduced notional | Compute remaining notional | Partial termination reduces the contract; full termination ends it. Must trigger cashflow supersession |
| 56 | `novation/transferor` / `transferee` / `remainingParty` | 3 rows in `CanonicalTradePartyRole`; `parent_trade_sk` on the new trade | Create stepped-out and stepped-in trades | Counterparty exposure moves from one entity to another. Credit risk and collateral must both see it |
| 57 | `withdrawalNotification` / `tradeCancelled` | `CanonicalTrade.trade_status = 'CANCELLED'`, all cashflows `CANCELLED` | **Soft delete only** | The trade never existed economically but must remain in the record for audit and for the regulatory error/cancel submission |
| 58 | `confirmationStatus/status` | `CanonicalTrade.confirmation_status` | Enum map | Legal certainty of terms. Unconfirmed trades over the EMIR deadline are a reportable breach |
| 59 | `clearingStatus/status`, `clearingConfirmed` | `CanonicalTrade.clearing_status`, plus CCP row in `CanonicalTradePartyRole` | Enum map | On clearing, the original bilateral trade is replaced by two trades facing the CCP. **Two new trades, not an update** |

## 5.10 Valuation (mappings 60–64)

| # | FpML XPath | → Canonical attribute | Transformation rule | Business meaning |
|---|---|---|---|---|
| 60 | `valuationReport/assetValuation/quotation/value` | `CanonicalValuation.measure_value` | `DECIMAL(28,10)` | The measurement itself |
| 61 | `assetValuation/quotation/measureType` | `CanonicalValuation.measure_type` | Enum map (`NPV`, `PV01`, `Delta`) | What is being measured — **stored as a row value, not a column** |
| 62 | `assetValuation/quotation/currency` | `CanonicalValuation.valuation_currency` | ISO 4217 | Denomination of the measure |
| 63 | `assetValuation/quotation/valuationDate` | `CanonicalValuation.valuation_date` | `DATE` | As-at date. The grain of the valuation fact |
| 64 | `assetValuation/quotation/side` | `CanonicalValuation.valuation_side` | Enum (`Mid`/`Bid`/`Ask`) | Independent price verification compares *like for like*; mixing bid and mid creates fake breaks |

## 5.11 Worked mapping — TRD10001, source to canonical

**FpML fragment → `CanonicalTrade` row**

| FpML | Value | → Canonical attribute | Value |
|---|---|---|---|
| `@fpmlVersion` | `5-13` | `fpml_version` | `5-13` |
| `@xsi:type` | `ExecutionNotification` | *(event)* `event_type` | `NEW` |
| `header/messageId` | `MSG-20260901-000451` | `source_message_id` | `MSG-20260901-000451` |
| `partyTradeIdentifier[GMB]/tradeId` | `TRD10001` | `source_trade_id` | `TRD10001` |
| `partyTradeIdentifier[ABCBANK]/tradeId` | `ABC-FX-889231` | `counterparty_trade_id` | `ABC-FX-889231` |
| `tradeDate` | `2026-09-01` | `trade_date` | `2026-09-01` |
| `executionDateTime` | `2026-09-01T14:31:58Z` | `execution_timestamp` | `2026-09-01 14:31:58 UTC` |
| `productType` | `ForeignExchange:Forward` | `asset_class` / `product_type` | `FX` / `FX_FORWARD` |
| `partyTradeInformation/partyReference` | `GMB` | `booking_entity_sk` | `LE-GMB-LDN` (LEI `213800GLOBALMKTS001`) |
| `relatedParty[BookingParty]` | `GMBLDN` | `booking_branch_id` | `GMB-LDN` |
| *(complement)* | `ABCBANK` | `counterparty_sk` | `CP-001` (LEI `5493001ABCBANK00001`) |
| `valueDate` | `2026-09-03` | `settlement_date` | `2026-09-03` |
| `trader` | `J.HARRIS` | `trader_id` | `J.HARRIS` |
| *(derived from events)* | — | `trade_status` | `SETTLED` (as at 04-Sep) |
| *(derived)* | — | `trade_version` / `is_current` | `1` / `true` |

**→ `CanonicalTradeLeg` rows**

| leg | leg_type | ccy | notional | payer | receiver | direction | value_date | rate | spot | fwd pts | quote_basis |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `EXCHANGED_CCY_1` | GBP | 10,000,000.00 | CP-001 | LE-GMB-LDN | **RECEIVE** | 2026-09-03 | 1.3200 | 1.3198 | 0.0002 | `Currency2PerCurrency1` |
| 2 | `EXCHANGED_CCY_2` | USD | 13,200,000.00 | LE-GMB-LDN | CP-001 | **PAY** | 2026-09-03 | 1.3200 | 1.3198 | 0.0002 | `Currency2PerCurrency1` |

**→ `CanonicalCashflow` rows**

| cashflow_id | leg | type | basis | ccy | expected | direction | signed | value_date | account | ssi | status |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `CF-10001-01` | 1 | `PRINCIPAL_EXCHANGE` | `CONTRACTUAL` | GBP | 10,000,000.00 | RECEIVE | +10,000,000.00 | 2026-09-03 | `ACC-GBP-01` | `SSI-GMB-GBP-CLS` | `SETTLED` |
| `CF-10001-02` | 2 | `PRINCIPAL_EXCHANGE` | `CONTRACTUAL` | USD | 13,200,000.00 | PAY | −13,200,000.00 | 2026-09-03 | `ACC-USD-01` | `SSI-ABC-USD-CLS` | `SETTLED` |

**→ `CanonicalPayment` rows** *(no FpML source — created by the TPP)*

| payment_id | ccy | amount | direction | value_date | method | system | debit_acct | credit_acct | reference | status |
|---|---|---|---|---|---|---|---|---|---|---|
| `PMT-90001` | GBP | 10,000,000.00 | RECEIVE | 2026-09-03 | `CLS` | `CLS` | — | `ACC-GBP-01` | `GMB260903GBP0001` | `SETTLED` |
| `PMT-90002` | USD | 13,200,000.00 | PAY | 2026-09-03 | `CLS` | `CLS` | `ACC-USD-01` | — | `GMB260903USD0002` | `SETTLED` |

**→ `CanonicalSettlement` rows** *(no FpML source — created from CLS report + camt.054)*

| settlement_id | payment | ccy | settled | dir | value_date | settlement_date | status | source | days_late |
|---|---|---|---|---|---|---|---|---|---|
| `STL-90001` | `PMT-90001` | GBP | 10,000,000.00 | RECEIVE | 2026-09-03 | 2026-09-03 | `SETTLED` | `CLS_REPORT` + `CAMT054` | 0 |
| `STL-90002` | `PMT-90002` | USD | 13,200,000.00 | PAY | 2026-09-03 | 2026-09-03 | `SETTLED` | `CLS_REPORT` + `CAMT054` | 0 |

> **Look carefully at the last two tables.** `CanonicalPayment` and `CanonicalSettlement` have **no FpML source column at all.** FpML gave us the trade, the cashflows and the instruction. It could not give us the payment or the settlement, because those are internal actions and external facts respectively. **This is the whole argument for a canonical model in one observation.**

## 5.12 Attributes that exist in canonical but have no FpML source

| Canonical attribute | Where it comes from | Why FpML cannot supply it |
|---|---|---|
| `trade_status` | Derived state machine over events | FpML sends events, not states |
| `payment_id`, `payment_status`, `uetr` | TPP / payment engine | Payments are internal actions taken *after* the trade |
| `settled_amount`, `settlement_date`, `settlement_status` | camt.054 / camt.053 / CLS | Settlement is an external fact confirmed by a different network |
| `fail_reason_code`, `days_late` | Payment rails + ops workflow | Not a contractual concept at all |
| `gl_account_code`, `cost_centre` | Finance master data | Accounting is an internal construct |
| `emir_classification`, `sector_classification` | Party/reference master | Regulatory classification of the counterparty, not of the trade |
| `book_id`, `portfolio_id`, `desk` | Risk hierarchy master | Organisational, not contractual |
| `is_current`, `valid_from`, `valid_to` | Platform bitemporality | A message has no concept of "current" |
| `market_data_snapshot_id` | Risk engine | Reproducibility metadata |
| `reconciliation_status` | Rec engine | An assertion about agreement between two systems |

**In an interview, use this table.** It demonstrates that you understand FpML is roughly 55–65% of a trade-and-cash model, and that the rest — the part Finance and Treasury actually care about — has to be designed.

## 5.13 Mapping governance

A mapping specification is a controlled artefact. In production it should be:

| Practice | Implementation |
|---|---|
| **Machine-readable** | Mappings held as YAML/JSON, not Confluence tables. Generate the transformation code and the documentation from the same source |
| **Version-controlled** | Every mapping change is a PR with a reviewer from the business |
| **Test-covered** | Golden FpML files + expected canonical output; run on every build. Include the nasty ones: split value dates, NDF, negative-notional amendment, out-of-order sequence |
| **Coverage-measured** | Track "% of FpML elements received that the mapping consumes"; unmapped elements land in a `raw_unmapped` map column and raise an alert when a new one appears |
| **Lineage-published** | Push field-level mappings into the catalogue (Unity Catalog / Collibra / Purview) so a Finance user can click `gold_fact_settled_cash.settled_amount` and see the XPath it came from |

---
# PART 6 — SEMANTIC DATA MODEL

## 6.1 The three models, precisely distinguished

This is the section to be able to recite. Interviewers ask it directly, and most candidates blur the boundaries.

| | **Source model (FpML)** | **Canonical model** | **Semantic model** |
|---|---|---|---|
| **Question it answers** | "What am I telling you about this contract?" | "What does the firm believe is true about this trade?" | "What number does the business need?" |
| **Owned by** | ISDA / the industry | Enterprise Data Architecture | Data Product / consuming domain |
| **Optimised for** | Fidelity and interoperability | Correctness, completeness, history | Query performance and comprehension |
| **Shape** | Hierarchical XML, recursive, polymorphic | Normalised (3NF-ish), bitemporal, source-agnostic | Dimensional (star/constellation), denormalised, conformed |
| **Grain** | One message | One entity version | One business event at a declared grain |
| **Naming** | `exchangedCurrency1/paymentAmount/amount` | `expected_amount` | `Settled Amount (Reporting Ccy)` |
| **Nulls** | Pervasive (almost everything optional) | Controlled by contract | Eliminated — defaults and "Unknown" dimension members |
| **History** | None (a message is a point in time) | Full bitemporal | Type 2 dimensions + point-in-time facts |
| **Audience** | Systems | Engineers and data stewards | Analysts, Finance, Risk, Regulators, BI tools |
| **Change driver** | ISDA release cycle | Business meaning changes | Consumer requirement changes |
| **Stability** | Annual-ish version bumps | Very stable — the insulation layer | Evolves freely; additive |
| **Physical home** | Kafka / files | Silver (Delta / Snowflake) | Gold (Delta / Snowflake) |
| **Rough size for TRD10001** | 1 XML document | 9+ normalised rows | 5 fact rows + dimension keys |

```
   SOURCE                CANONICAL                SEMANTIC
   ──────                ─────────                ────────
   fidelity              correctness              comprehension
   "as sent"             "as true"                "as needed"
      │                       │                        │
   FpML XML  ──parse──►  silver_trade  ──model──►  gold_fact_trade
             validate     silver_cashflow            gold_fact_cashflow
             resolve      silver_payment             gold_fact_settled_cash
             enrich       silver_settlement          gold_dim_counterparty
             version      silver_valuation           gold_dim_product ...
      │                       │                        │
   Breaks when            Breaks when              Breaks when
   ISDA changes           the business             a consumer's
   the schema             changes what             question
                          a trade means            changes
```

**The insulation argument (memorise this):** consumers should never break because ISDA published FpML 5.14, and the canonical model should never change because Finance wants a new slice-by. Canonical absorbs *source* volatility; semantic absorbs *consumer* volatility. Between them sits a stable contract. **Remove either and you get N×M coupling.**

## 6.2 The lineage chain for our trade

```
FpML executionNotification (MSG-20260901-000451)
   │
   ├──► silver_trade (TRD10001 v1)          ──► gold_fact_trade
   ├──► silver_trade_leg (2 rows)                    │
   ├──► silver_cashflow (CF-10001-01/02)    ──► gold_fact_cashflow
   ├──► silver_trade_event (5 rows)         ──► gold_fact_trade_event
   │
CLS report + camt.054
   ├──► silver_payment (PMT-90001/2)
   └──► silver_settlement (STL-90001/2)     ──► gold_fact_settled_cash
                                                     │
FpML valuationReport                                 │
   └──► silver_valuation                    ──► gold_fact_valuation
                                                     │
                                                     v
                                         ┌───────────────────────┐
                                         │ gold_dim_date         │
                                         │ gold_dim_counterparty │
                                         │ gold_dim_legal_entity │
                                         │ gold_dim_product      │
                                         │ gold_dim_currency     │
                                         │ gold_dim_account      │
                                         │ gold_dim_settlement_  │
                                         │      status           │
                                         └───────────────────────┘
```

## 6.3 Semantic design principles

| Principle | Rule | Why |
|---|---|---|
| **Declare the grain first** | Write the grain statement before any column | Ambiguous grain is the root cause of double counting |
| **Conformed dimensions** | One `gold_dim_counterparty` shared by trade, cashflow, settlement and valuation facts | Enables drill-across: "settled cash *and* MTM *and* trade count for ABC Bank" |
| **Additive measures where possible** | `signed_settled_amount` sums correctly across every dimension | Semi-additive (positions, balances) must be flagged; non-additive (rates) must never be summed |
| **Signed amounts, always** | Store the sign; never make BI tools apply direction logic | Direction logic in 40 dashboards = 40 chances to get it wrong |
| **Currency + base currency together** | Native amount *and* reporting-currency amount, plus the rate used | Finance reports in one currency; Ops works in native |
| **No surprises in nulls** | Every dimension has an `Unknown` (-1) member; facts never have null FKs | A null FK silently drops rows in an inner join |
| **Business names** | `settled_amount`, not `stl_amt_val` | The semantic layer is the layer humans read |
| **Type 2 for anything reportable historically** | Counterparty rating, entity hierarchy, product classification | "Show me exposure as it was classified in September" must be answerable |

---

## 6.4 Fact tables

### 6.4.1 `gold_fact_trade`

**Grain:** one row per **trade version** per booking entity.
**Type:** transaction (accumulating snapshot characteristics via the milestone dates).

| Column | Type | Business meaning |
|---|---|---|
| `trade_sk` | BIGINT | Degenerate/surrogate key of the trade version |
| `trade_id` | STRING | The number Ops and the trader quote to each other |
| `trade_version` | INT | Which version; combine with `is_current_version` for "as now" reporting |
| `is_current_version` | BOOLEAN | Filter for current-state reporting |
| `trade_date_sk` | INT | FK → `gold_dim_date`. **The date the business books the trade to** |
| `value_date_sk` | INT | FK → `gold_dim_date`. Contractual settlement date |
| `maturity_date_sk` | INT | FK → `gold_dim_date`. End of economic life |
| `product_sk` | BIGINT | FK → `gold_dim_product` |
| `legal_entity_sk` | BIGINT | FK → `gold_dim_legal_entity`. **Our booking entity** |
| `counterparty_sk` | BIGINT | FK → `gold_dim_counterparty` |
| `book_sk` | BIGINT | FK → `gold_dim_book` (desk/portfolio hierarchy) |
| `trade_status_sk` | BIGINT | FK → `gold_dim_trade_status` |
| `buy_currency_sk` / `sell_currency_sk` | BIGINT | FK → `gold_dim_currency` |
| **`buy_amount`** | DECIMAL(28,2) | Notional we receive. *Additive within a currency only* |
| **`sell_amount`** | DECIMAL(28,2) | Notional we pay |
| **`notional_base_ccy`** | DECIMAL(28,2) | Notional in the reporting currency (USD) — **the additive one** |
| `deal_rate` | DECIMAL(18,10) | All-in rate. **Non-additive — never SUM, only weighted-average** |
| `spot_rate` / `forward_points` | DECIMAL(18,10) | P&L attribution between spot and forwards desks |
| **`trade_count`** | INT | Always 1. The classic "count fact" that makes COUNT(*) explicit and safe |
| `execution_timestamp` | TIMESTAMP | MiFIR timestamp |
| `confirmation_lag_minutes` | INT | Confirmed − executed. Confirmation-timeliness KPI |
| `is_intragroup` / `is_cleared` / `is_reportable_emir` | BOOLEAN | Regulatory and reporting flags |
| `source_system` | STRING | Provenance |

**Sample row — TRD10001:**

| trade_id | ver | current | trade_date | value_date | product | entity | cpty | buy_ccy | buy_amt | sell_ccy | sell_amt | notional_usd | rate | count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| TRD10001 | 1 | true | 2026-09-01 | 2026-09-03 | FX Forward | GMB-LDN | ABC Bank | GBP | 10,000,000.00 | USD | 13,200,000.00 | 13,200,000.00 | 1.3200 | 1 |

---

### 6.4.2 `gold_fact_cashflow`

**Grain:** one row per **expected cashflow** per trade version.

| Column | Type | Business meaning |
|---|---|---|
| `cashflow_sk` | BIGINT | PK |
| `trade_sk` | BIGINT | FK → fact_trade / dim_trade |
| `trade_id` | STRING | Degenerate dimension for drill-through |
| `value_date_sk` | INT | FK → dim_date. **When the money is due** |
| `currency_sk` | BIGINT | FK → dim_currency |
| `legal_entity_sk` / `counterparty_sk` / `product_sk` / `account_sk` | BIGINT | Conformed FKs |
| `cashflow_type_sk` | BIGINT | FK → dim_cashflow_type (`PRINCIPAL_EXCHANGE`, `INTEREST`, `FEE`, `CLAIM`) |
| `payment_direction` | STRING | `PAY` / `RECEIVE` |
| **`expected_amount`** | DECIMAL(28,2) | Gross amount due, always positive |
| **`signed_expected_amount`** | DECIMAL(28,2) | Negative for PAY. **The additive measure** |
| **`settled_amount`** | DECIMAL(28,2) | Cumulative settled to date |
| **`unsettled_amount`** | DECIMAL(28,2) | `expected − settled`. Drives the "outstanding" KPI |
| `expected_amount_base_ccy` / `settled_amount_base_ccy` | DECIMAL(28,2) | Reporting-currency equivalents |
| `fx_rate_to_base` | DECIMAL(18,10) | Rate used and its source — needed for audit |
| `settlement_status_sk` | BIGINT | FK → dim_settlement_status |
| `is_current` | BOOLEAN | Excludes SUPERSEDED versions |
| **`cashflow_count`** | INT | Always 1 |

---

### 6.4.3 `gold_fact_settled_cash` — the centrepiece

**Grain:** one row per **confirmed settlement movement** (one payment may produce several — partials, tranches, reversals).
**Type:** transaction fact, insert-only.

Detailed in Part 7. Summary of measures:

| Measure | Additivity | Meaning |
|---|---|---|
| `expected_amount` | Additive within currency | What was due |
| `settled_amount` | Additive within currency | What actually moved |
| `signed_settled_amount` | **Fully additive** | Signed for direction — the measure you build every KPI on |
| `settled_amount_base_ccy` | **Fully additive** | Reporting-currency equivalent |
| `variance_amount` | Additive | `settled − expected` |
| `days_late` | **Non-additive** | Average or max only |
| `settlement_count` | Additive | Count fact |

---

### 6.4.4 `gold_fact_valuation`

**Grain:** one row per trade per valuation date per measure type per source.
**Type:** periodic snapshot.

| Column | Business meaning |
|---|---|
| `trade_sk`, `valuation_date_sk` | Grain keys |
| `measure_type_sk` | FK → dim_measure_type (`NPV`, `PV01`, `DELTA`) |
| `currency_sk` | Currency of the measure |
| **`measure_value`** | The number. **Semi-additive** — additive across trades, *not* across dates |
| **`measure_value_base_ccy`** | Reporting-currency equivalent |
| `valuation_source_sk`, `is_official` | IPV comparison and official-mark identification |
| `market_data_snapshot_id` | Reproducibility |
| `legal_entity_sk`, `counterparty_sk`, `product_sk`, `book_sk` | Conformed FKs for slicing |

> **The classic interview trap on this table:** MTM is *semi-additive*. Summing NPV across September gives a meaningless number. Sum across trades on a date; take last/average across dates. Say this unprompted and you will stand out.

---

### 6.4.5 `gold_fact_trade_event`

**Grain:** one row per trade lifecycle event.

| Column | Business meaning |
|---|---|
| `trade_event_sk`, `trade_sk`, `trade_id` | Keys |
| `event_date_sk`, `event_timestamp` | When |
| `event_type_sk` | FK → dim_event_type |
| `resulting_version`, `previous_version` | Version transition |
| `processing_lag_seconds` | Ingestion − event time. **Operational SLA and late-arrival monitoring** |
| `event_count` | Count fact |

Powers: STP rate, amendment rate by desk, confirmation-timeliness, "how many times was this trade touched", and operational-risk reporting.

---

## 6.5 Dimension tables

### `gold_dim_date`

**Grain:** one row per calendar date. The most under-appreciated table in any financial warehouse.

| Column | Business meaning |
|---|---|
| `date_sk` | INT `YYYYMMDD` — the surrogate everyone can read |
| `calendar_date` | The date |
| `day_of_week`, `day_name` | |
| `is_weekend` | |
| **`is_business_day_gbp` / `_usd` / `_eur` / `_jpy` / `_aud`** | **Per-currency holiday calendars.** Essential: is 03-Sep a good business day in *both* legs' currencies? |
| `is_cls_settlement_day` | CLS operates on its own calendar |
| `month_number`, `month_name`, `month_start_date`, `month_end_date` | **MTD boundaries** |
| `quarter`, `quarter_start_date`, `quarter_end_date` | QTD |
| `year`, `year_start_date`, `year_end_date` | YTD |
| `fiscal_month`, `fiscal_quarter`, `fiscal_year` | **Finance's fiscal calendar ≠ the calendar year.** Non-negotiable for MTD reporting |
| `is_month_end`, `is_quarter_end`, `is_year_end` | Period-close flags |
| `prior_business_day`, `next_business_day` | T+n arithmetic |
| `relative_day_offset` | Days from today — enables rolling windows without date math in every query |

### `gold_dim_counterparty` — **SCD Type 2**

| Column | Business meaning |
|---|---|
| `counterparty_sk` | Surrogate PK (changes on Type 2 change) |
| `counterparty_id`, `lei` | Durable business keys |
| `counterparty_name`, `short_name` | Display |
| `counterparty_type` | `BANK`, `ASSET_MANAGER`, `CORPORATE`, `CCP`, `FUND` |
| `emir_classification` | `FC` / `NFC+` / `NFC-` — **drives clearing and margin obligations, and it changes over time** |
| `sector`, `industry_classification` | Concentration reporting |
| `country_of_incorporation`, `region` | Country-risk reporting |
| `ultimate_parent_lei`, `ultimate_parent_name` | **Group-level exposure aggregation** — regulators ask for group, not entity |
| `credit_rating`, `internal_credit_grade` | Credit risk. **Type 2 — a downgrade must not rewrite history** |
| `is_cls_member` | Determines settlement risk profile |
| `netting_agreement_id`, `has_csa` | Collateral eligibility |
| `is_internal` | Intragroup elimination |
| `valid_from`, `valid_to`, `is_current` | SCD2 mechanics |

### `gold_dim_legal_entity` — **SCD Type 2**

| Column | Business meaning |
|---|---|
| `legal_entity_sk`, `legal_entity_id`, `lei` | Keys |
| `entity_name`, `entity_short_name` | Display |
| `entity_type` | `BANK`, `BRANCH`, `SUBSIDIARY`, `SPV` |
| `booking_location` | `LONDON`, `NEW_YORK`, `SINGAPORE` |
| `regulatory_jurisdiction` | `UK`, `US`, `EU` — determines which regime reports |
| `functional_currency` | **The entity's reporting currency for the GL** |
| `consolidation_group`, `parent_entity_sk` | Group consolidation hierarchy |
| `gl_company_code`, `cost_centre_hierarchy` | Direct join into the general ledger |
| `is_active`, `valid_from`, `valid_to`, `is_current` | |

### `gold_dim_product` — **SCD Type 2**

| Column | Business meaning |
|---|---|
| `product_sk`, `product_code` | Keys |
| `product_name` | `Foreign Exchange Forward` |
| `asset_class` | `FX` — top of the reporting hierarchy |
| `product_family` | `FX_LINEAR` |
| `product_subtype` | `OUTRIGHT_FORWARD` |
| `isda_taxonomy` | `ForeignExchange:Forward` |
| `upi`, `cfi_code` | Regulatory product identifiers |
| `is_deliverable`, `is_cleared_eligible`, `is_exotic` | Processing and capital treatment |
| `default_cashflow_count` | **DQ expectation:** an FX Forward with 3 cashflows is a defect |
| `risk_category`, `capital_treatment` | Finance/Risk hierarchies |
| `product_hierarchy_l1/l2/l3` | The drill path business users navigate |

### `gold_dim_currency`

| Column | Business meaning |
|---|---|
| `currency_sk`, `currency_code` | Keys (`GBP`) |
| `currency_name`, `currency_symbol` | Display |
| `numeric_code` | ISO 4217 numeric (`826`) |
| **`decimal_places`** | **2 for GBP/USD/EUR, 0 for JPY.** Rounding logic depends on it; getting it wrong creates false breaks of ¥1 |
| `is_cls_eligible` | Determines settlement mechanism and risk |
| `is_deliverable`, `is_restricted` | NDF currencies (BRL, KRW, TWD, INR) settle differently |
| `standard_settlement_lag_days` | T+2 for most, T+1 for USD/CAD |
| `settlement_calendar_code`, `primary_settlement_system` | `T2`, `FEDWIRE`, `CHAPS`, `BOJNET` |
| `payment_cutoff_time_utc` | **Why TRD10002's EUR leg failed** — TARGET2 cut-off |
| `region`, `is_g10`, `is_emerging` | Reporting groupings |

### `gold_dim_account`

| Column | Business meaning |
|---|---|
| `account_sk`, `account_id` | Keys |
| `account_number`, `iban`, `account_name` | Identification |
| `account_type` | `NOSTRO`, `VOSTRO`, `CLIENT`, `CLS`, `SUSPENSE` |
| `currency_code` | Denomination |
| `owner_legal_entity_sk` | Whose account it is |
| `agent_bank_name`, `agent_bic` | Where it is held |
| **`gl_account_code`** | **The join to the general ledger — without this Finance cannot post** |
| `cost_centre`, `is_reconcilable`, `is_active` | Finance and Ops attributes |

### `gold_dim_settlement_status`

| Column | Business meaning |
|---|---|
| `settlement_status_sk`, `status_code` | `SETTLED`, `PARTIALLY_SETTLED`, `FAILED`, `PENDING`, `CANCELLED`, `REVERSED` |
| `status_description` | Business-readable |
| `status_category` | `COMPLETE`, `IN_PROGRESS`, `EXCEPTION` |
| `is_terminal` | Whether further transitions are possible |
| `counts_as_settled` | **The single flag every MTD query filters on** — put the definition in the dimension, not in 40 SQL queries |
| `impacts_cash_position`, `requires_investigation` | Downstream behaviour flags |

### Additional conformed dimensions

| Dimension | Purpose |
|---|---|
| `gold_dim_book` | Desk / portfolio / business-line hierarchy for P&L |
| `gold_dim_trade_status` | `NEW`, `CONFIRMED`, `CANCELLED`, `MATURED`, `NOVATED` |
| `gold_dim_event_type` | Lifecycle event taxonomy |
| `gold_dim_settlement_method` | `CLS`, `RTGS_GROSS`, `NET`, `BOOK_TRANSFER` + risk classification (PVP vs non-PVP) |
| `gold_dim_fail_reason` | Fail taxonomy with root-cause category and owning team |
| `gold_dim_measure_type` | `NPV`, `PV01`, `DELTA` |

## 6.6 The semantic star

```
                        gold_dim_date
                              │
   gold_dim_legal_entity ─────┼───── gold_dim_counterparty
                              │
   gold_dim_currency ────┬────┴────┬──── gold_dim_product
                         │         │
                  ┌──────v─────────v──────┐
                  │  gold_fact_settled_cash│
                  │  ─────────────────────│
                  │  settled_amount        │
                  │  signed_settled_amount │
                  │  expected_amount       │
                  │  variance_amount       │
                  │  days_late             │
                  │  settlement_count      │
                  └──────┬─────────┬───────┘
                         │         │
   gold_dim_account ─────┘         └───── gold_dim_settlement_status
                                                    │
                                          gold_dim_settlement_method
                                          gold_dim_fail_reason

   The SAME dimensions also key:
     gold_fact_trade · gold_fact_cashflow · gold_fact_valuation · gold_fact_trade_event
   → this is what "conformed" means, and it is what makes drill-across possible.
```

## 6.7 Consumption patterns by audience

| Consumer | Primary facts | Typical question | Key dimensions |
|---|---|---|---|
| **Finance / Product Control** | `fact_settled_cash`, `fact_valuation` | "MTD net settled cash by currency and legal entity" | date, currency, legal entity, account |
| **Treasury / Liquidity** | `fact_cashflow`, `fact_settled_cash` | "Projected cash by currency for the next 5 business days" | date (value date), currency, account |
| **Market Risk** | `fact_trade`, `fact_valuation` | "FX delta by pair and tenor bucket" | product, currency, book, date |
| **Credit Risk** | `fact_trade`, `fact_valuation` | "Net exposure to the ABC Bank *group*" | counterparty (ultimate parent), date |
| **Settlement Ops** | `fact_settled_cash`, `fact_cashflow` | "Failed settlements by reason and currency, aged" | fail reason, currency, status, date |
| **Regulatory Reporting** | `fact_trade`, `fact_trade_event`, `fact_valuation` | "All EMIR-reportable new trades T+1" | date, counterparty, product, entity |
| **BI / Management** | all | "STP rate and confirmation lag trend by desk" | date, book, event type |
| **Data Science** | all + Silver | "Predict settlement fails from SSI, currency, counterparty, size" | full grain |

---
# PART 7 — THE SETTLED CASH MODEL

This is the part that separates people who have *built* a trade platform from people who have *read about* one. Settled cash is where the contractual world meets the banking world, and the two disagree constantly.

## 7.1 Five concepts that are routinely confused

| # | Concept | Definition | Epistemic status | Source of truth | Can it change? |
|---|---|---|---|---|---|
| 1 | **Trade** | A legally binding agreement to exchange value | **Contract** | Front office / FpML | Yes — amendment, novation, cancellation |
| 2 | **Expected Cashflow** | An amount of one currency due to move on a stated date because of the trade | **Expectation** | Derived from the trade | Yes — amendments and fixings change it |
| 3 | **Payment (instruction)** | An instruction to move cash on a date by a method | **Intent / action** | TPP payment engine | Yes — until released; after that, only by cancel + re-issue |
| 4 | **Settlement** | A confirmed, final, irrevocable transfer | **Fact** | CLS / camt.054 / camt.053 / nostro rec | **No** — only reversible by a new offsetting settlement |
| 5 | **Settled Cash** | The reportable, reconciled, accounting-grade record of cash that has actually moved, attributed back to trade, entity, counterparty and account | **Reconciled fact** | The data platform, from (4) ∩ nostro | No — restatements are new rows |

```
   ┌───────────┐   "we agreed"      contractual, versioned
   │   TRADE   │
   └─────┬─────┘
         │  generate  (1:N — 2 for FX spot/fwd, 4 for FX swap, ~40 for a 5y IRS)
         v
   ┌────────────────────┐   "money is due"   expectation, dated by VALUE DATE
   │ EXPECTED CASHFLOW  │
   └─────┬──────────────┘
         │  net / instruct  (N:M — netting collapses many into one)
         v
   ┌────────────────────┐   "we told the bank"   action, has a wire reference
   │ PAYMENT INSTRUCTION│
   └─────┬──────────────┘
         │  confirm  (1:N — partials, tranches, retries)
         v
   ┌────────────────────┐   "the money moved"   fact, dated by SETTLEMENT DATE
   │    SETTLEMENT      │
   └─────┬──────────────┘
         │  reconcile against nostro statement (camt.053)
         v
   ┌────────────────────┐   "we can report and book it"
   │   SETTLED CASH     │   reconciled fact, GL-postable
   └────────────────────┘
```

### The three questions that reveal whether you understand this

| Question | Wrong answer | Right answer |
|---|---|---|
| "When did the cash move?" | On the value date | On the **settlement date**, which equals the value date only when nothing went wrong |
| "How much settled?" | The trade amount | The **sum of `settled_amount` over settlements**, which may be zero (failed), partial, or in tranches |
| "Which trade did this nostro debit belong to?" | Look up the payment reference | **You cannot always** — the debit is a *net* figure covering many trades. You need the payment→cashflow allocation bridge |

## 7.2 Why `value_date` ≠ `settlement_date` matters so much

| | `value_date` | `settlement_date` |
|---|---|---|
| Meaning | The date cash is **contractually due** | The date cash **actually became final** |
| Set by | The trade | The payment system |
| Source | FpML `valueDate` | camt.054 / CLS report |
| Present on | Cashflow, payment, settlement | Settlement only |
| Used for | Liquidity forecasting, funding, exposure by tenor, "expected cash" | **MTD/LTD settled cash, GL posting date, cash position** |
| Nullable | Never | **Yes** — null while unsettled |

**A single table joining on the wrong one of these will misstate every Finance report.** TRD10002's EUR leg has `value_date = 2026-09-03` and `settlement_date = 2026-09-04`. Cash-basis reporting uses 04-Sep; liquidity forecasting used 03-Sep; interest compensation is owed for the one-day gap. All three are correct for their purpose, and only an explicit two-column model can serve all three.

---

## 7.3 The settled cash data model

### 7.3.1 `gold_fact_settled_cash`

**Grain statement (write this down before writing the DDL):**
> One row per confirmed cash movement, identified by `settlement_id`, attributable to exactly one cashflow of exactly one trade version, in exactly one currency, hitting exactly one account.

| Column | Type | Business meaning |
|---|---|---|
| **Identity** | | |
| `settled_cash_sk` | BIGINT | Surrogate PK |
| `settlement_id` | STRING | Business key, e.g. `STL-90002` |
| `payment_id` | STRING | The instruction this fulfils |
| `cashflow_id` | STRING | The expectation this satisfies |
| `trade_id` | STRING | The contract it arose from |
| `trade_version` | INT | Which version — an amendment can change what was due |
| **Dimension keys** | | |
| `settlement_date_sk` | INT | FK → dim_date. **The date cash actually moved — the MTD/LTD driver** |
| `value_date_sk` | INT | FK → dim_date. The date it was due |
| `trade_date_sk` | INT | FK → dim_date. For trade-date-basis reporting |
| `currency_sk` | BIGINT | FK → dim_currency |
| `account_sk` | BIGINT | FK → dim_account. The nostro hit |
| `legal_entity_sk` | BIGINT | FK → dim_legal_entity. **Our booking entity** |
| `counterparty_sk` | BIGINT | FK → dim_counterparty |
| `product_sk` | BIGINT | FK → dim_product |
| `book_sk` | BIGINT | FK → dim_book |
| `settlement_status_sk` | BIGINT | FK → dim_settlement_status |
| `settlement_method_sk` | BIGINT | FK → dim_settlement_method (`CLS`, `RTGS_GROSS`, `NET`) |
| `fail_reason_sk` | BIGINT | FK → dim_fail_reason (`-1` when no fail) |
| **Measures** | | |
| `payment_direction` | STRING | `PAY` / `RECEIVE` |
| `direction_sign` | SMALLINT | `-1` / `+1` |
| `expected_amount` | DECIMAL(28,2) | What was due (positive) |
| `settled_amount` | DECIMAL(28,2) | What moved (positive; `0` on a fail) |
| **`signed_settled_amount`** | DECIMAL(28,2) | `settled_amount × direction_sign`. **The measure every cash KPI is built on** |
| `unsettled_amount` | DECIMAL(28,2) | `expected − settled` for this movement |
| `variance_amount` | DECIMAL(28,2) | `settled − expected`. Non-zero means investigate |
| `settled_amount_base_ccy` | DECIMAL(28,2) | Reporting-currency (USD) equivalent |
| `signed_settled_amount_base_ccy` | DECIMAL(28,2) | Signed reporting-currency equivalent |
| `fx_rate_to_base` | DECIMAL(18,10) | The rate used |
| `fx_rate_date` / `fx_rate_source` | DATE / STRING | **Which rate on which date from where — required by audit** |
| `days_late` | INT | Business days `settlement_date − value_date` |
| `settlement_count` | INT | Always 1 |
| **Control** | | |
| `is_partial` | BOOLEAN | This movement is less than the full cashflow |
| `is_reversal` | BOOLEAN | This row reverses an earlier settlement |
| `reverses_settlement_id` | STRING | Which one |
| `is_netted` | BOOLEAN | Settled inside a net payment rather than gross |
| `reconciliation_status` | STRING | `MATCHED`, `UNMATCHED`, `BREAK`, `AGED_BREAK` |
| `bank_statement_id` / `statement_entry_ref` | STRING | The camt.053 entry this ties to |
| `confirmation_source` | STRING | `CLS_REPORT`, `CAMT054`, `CAMT053`, `MT950`, `MANUAL` |
| `settlement_timestamp` | TIMESTAMP | Time of finality |
| `gl_posting_date` | DATE | When Finance booked it (may differ from settlement date at period end) |
| `gl_journal_id` | STRING | Link to the GL |
| `source_system` | STRING | Provenance |
| `ingestion_timestamp` | TIMESTAMP | Late-arrival analysis |

### 7.3.2 Sample data — the full September ledger

| settlement_id | trade | cashflow | payment | ccy | dir | sign | expected | settled | signed_settled | value_date | settlement_date | days_late | status | method | fail_reason | account | entity | cpty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| STL-90001 | TRD10001 | CF-10001-01 | PMT-90001 | GBP | RECEIVE | +1 | 10,000,000.00 | 10,000,000.00 | **+10,000,000.00** | 2026-09-03 | 2026-09-03 | 0 | SETTLED | CLS | — | ACC-GBP-01 | GMB-LDN | CP-001 |
| STL-90002 | TRD10001 | CF-10001-02 | PMT-90002 | USD | PAY | −1 | 13,200,000.00 | 13,200,000.00 | **−13,200,000.00** | 2026-09-03 | 2026-09-03 | 0 | SETTLED | CLS | — | ACC-USD-01 | GMB-LDN | CP-001 |
| STL-90003 | TRD10002 | CF-10002-01 | PMT-90003 | USD | RECEIVE | +1 | 5,450,000.00 | 5,450,000.00 | **+5,450,000.00** | 2026-09-03 | 2026-09-03 | 0 | SETTLED | RTGS_GROSS | — | ACC-USD-01 | GMB-LDN | CP-002 |
| STL-90004 | TRD10002 | CF-10002-02 | PMT-90004 | EUR | PAY | −1 | 5,000,000.00 | **0.00** | **0.00** | 2026-09-03 | *(null)* | — | **FAILED** | RTGS_GROSS | INSUFFICIENT_FUNDS | ACC-EUR-01 | GMB-LDN | CP-002 |
| STL-90005 | TRD10002 | CF-10002-02 | PMT-90004-R1 | EUR | PAY | −1 | 5,000,000.00 | 5,000,000.00 | **−5,000,000.00** | 2026-09-03 | 2026-09-04 | **1** | SETTLED | RTGS_GROSS | — | ACC-EUR-01 | GMB-LDN | CP-002 |
| STL-90006 | TRD10003 | CF-10003-01 | PMT-90005 | USD | RECEIVE | +1 | 20,000,000.00 | 20,000,000.00 | **+20,000,000.00** | 2026-09-03 | 2026-09-03 | 0 | SETTLED | RTGS_GROSS | — | ACC-USD-01 | GMB-LDN | CP-001 |
| STL-90007 | TRD10003 | CF-10003-02 | PMT-90006 | JPY | PAY | −1 | 3,000,000,000 | 2,000,000,000 | **−2,000,000,000** | 2026-09-03 | 2026-09-03 | 0 | **PARTIAL** | RTGS_GROSS | — | ACC-JPY-01 | GMB-LDN | CP-001 |
| STL-90008 | TRD10003 | CF-10003-02 | PMT-90006-R1 | JPY | PAY | −1 | 1,000,000,000 | 1,000,000,000 | **−1,000,000,000** | 2026-09-03 | 2026-09-04 | **1** | SETTLED | RTGS_GROSS | — | ACC-JPY-01 | GMB-LDN | CP-001 |
| STL-90009 | TRD10002 | CF-10002-05 | PMT-90007 | EUR | PAY | −1 | 298.61 | 298.61 | **−298.61** | 2026-09-15 | 2026-09-15 | 0 | SETTLED | RTGS_GROSS | — | ACC-EUR-01 | GMB-LDN | CP-002 |

> **Note the modelling of the fail.** `STL-90004` is a row with `settled_amount = 0.00`, **not an absent row.** Keeping the failed attempt as a fact is what lets you report fail rates, fail values, ageing and root cause. If you only insert successes, your "failed settlement amount" report has no source data.

---

## 7.4 Handling the hard cases

### 7.4.1 Partial settlement

**Definition:** a cashflow is settled by more than one confirmed movement, whether in tranches on the same day, across days, or split across accounts.

**Model:** `Cashflow 1:N Settlement`. Never store settled amount only on the cashflow.

```
CF-10003-02   expected  JPY 3,000,000,000
   ├── STL-90007   settled JPY 2,000,000,000   03-Sep   status PARTIAL
   └── STL-90008   settled JPY 1,000,000,000   04-Sep   status SETTLED (residual)
                   ─────────────────────────
                   total   JPY 3,000,000,000   → cashflow status becomes SETTLED
```

**Rules:**

| Rule | Implementation |
|---|---|
| Cashflow status is **derived**, never asserted | `SETTLED` if `Σ settled = expected` (within currency tolerance); `PARTIALLY_SETTLED` if `0 < Σ settled < expected`; `EXPECTED` if `Σ = 0` and `value_date >= today`; `OVERDUE` if `Σ = 0` and `value_date < today` |
| Tolerance is currency-aware | ±0.01 for 2-dp currencies; ±1 for JPY; **use `dim_currency.decimal_places`** |
| The last tranche is not "the settlement" | Every tranche is a first-class fact row with its own settlement date |
| Over-settlement is an exception | `Σ settled > expected` must raise a break, never silently net |

### 7.4.2 Failed settlement

| Aspect | Treatment |
|---|---|
| Record | Insert a settlement row with `settlement_status = FAILED`, `settled_amount = 0`, `settlement_date = NULL`, `fail_reason_code` populated |
| Do **not** | Delete, update the original to zero, or omit the row |
| Retry | New payment `PMT-90004-R1` with `parent_payment_sk = PMT-90004`; new settlement row on success |
| Cashflow status | `FAILED` while unresolved, then `SETTLED` once the retry confirms |
| Ageing | `DATEDIFF(current_date, value_date)` while unresolved → fail-ageing buckets 1d / 2–5d / >5d |
| Compensation | The interest claim is a **new cashflow** (`cashflow_type = COMPENSATION_CLAIM`) linked to the same trade, not an adjustment to the original amount |
| Reporting | Fail rate by count and by value; both are needed — one large fail and a hundred tiny ones are different problems |

**State machine:**

```
   EXPECTED ──instruct──► INSTRUCTED ──release──► RELEASED
                                                     │
                          ┌──────────────────────────┼──────────────┐
                          v                          v              v
                  PARTIALLY_SETTLED             SETTLED          FAILED
                          │                        │                │
                          │ residual settles       │                │ retry
                          └────────►SETTLED        │                v
                                                   │           RELEASED (retry)
                                            reversal│                │
                                                   v                 v
                                              REVERSED           SETTLED
                                                                (days_late > 0)
```

### 7.4.3 Cancelled payments

| Scenario | Treatment |
|---|---|
| Cancelled **before release** | `payment_status = CANCELLED`; no settlement row; cashflow returns to `EXPECTED` and can be re-instructed |
| Cancelled **after release, before settlement** | Issue a cancellation to the payment system (`camt.056`). Payment `CANCELLED`; still no settlement row. **Log the attempt** |
| Cancelled **after settlement** | Not possible — settlement is final. The only remedy is a **return payment**: a new payment in the opposite direction, producing a new settlement row with `is_reversal = true` and `reverses_settlement_id` set |

> **Never mutate a settled row.** `SUM(signed_settled_amount)` must remain correct at every historical point in time. Corrections are additional rows, exactly as in double-entry accounting.

### 7.4.4 Amendments

Three cases, and the answer differs in each:

| Case | Treatment |
|---|---|
| **Amendment before any payment is instructed** (TRD10005) | Trade v1 → v2 (SCD2). v1 cashflows → `SUPERSEDED`. New v2 cashflows created as `EXPECTED`. No payment or settlement impact |
| **Amendment after payment instructed, before settlement** | Cancel the payment (`camt.056`), supersede the cashflow, create the new cashflow and a new payment. Two payment rows, one settlement |
| **Amendment after settlement** | The settled cash **stands**. The amendment creates a *differential* cashflow for the delta — e.g. a EUR 200,000 residual receivable — settling separately. **Never restate history** |

Cashflow supersession rule:

```sql
-- when trade version N+1 is created
UPDATE silver_cashflow
   SET cashflow_status = 'SUPERSEDED',
       valid_to        = :amendment_timestamp,
       is_current      = false
 WHERE trade_id      = :trade_id
   AND trade_version = :prior_version
   AND cashflow_status IN ('EXPECTED','INSTRUCTED');   -- settled/failed cashflows are NEVER superseded
```

That last predicate is the important one. **A settled cashflow is history and is immutable.**

### 7.4.5 Multiple cashflows per trade

| Product | Cashflows | Notes |
|---|---|---|
| FX Spot / Forward | 2 | One per currency, same value date |
| FX Forward with split value dates | 2 | Different value dates per leg — `dim_date` on the *cashflow*, not the trade |
| FX Swap | 4 | Near ×2, far ×2, two value dates |
| NDF | **1** | Net difference settles in one currency only |
| FX Option | 1 premium + 2 on exercise | Contingent cashflows — generate on the exercise event |
| 5y quarterly IRS | ~40 | Forecast, revised at each fixing |
| Cross-currency swap | ~40 + 4 principal exchanges | Initial and final principal exchange both ways |

**The rule that makes this work:** `Cashflow` is keyed by `(trade_sk, leg_number, cashflow_seq)`, never by trade alone, and every downstream cash object joins on `cashflow_id` rather than `trade_id`. Then the same settled-cash model serves an FX spot and a 30-year cross-currency swap without change.

### 7.4.6 Netting — the one that breaks naive models

On 03-Sep GMB has three USD movements with ABC Bank: pay 13,200,000 (TRD10001) and receive 20,000,000 (TRD10003). Under the netting agreement these settle as **one net receipt of USD 6,800,000**.

```
CF-10001-02   PAY     USD 13,200,000  ─┐
                                        ├─► PMT-90010  NET RECEIVE USD 6,800,000
CF-10003-01   RECEIVE USD 20,000,000  ─┘         │
                                                  v
                                          STL-90010  settled USD 6,800,000
                                                  │
                          payment_cashflow_allocation
                                ├─ CF-10001-02  −13,200,000
                                └─ CF-10003-01  +20,000,000
```

Two facts must both be true and reportable simultaneously:

| View | Value | Used by |
|---|---|---|
| **Gross settled cash** (trade attribution) | pay 13,200,000 and receive 20,000,000 | Finance trade P&L, regulatory gross-notional reporting, per-trade reconciliation |
| **Net settled cash** (nostro attribution) | net receipt 6,800,000 | Treasury liquidity, nostro reconciliation, actual cash position |

**Design answer:** `gold_fact_settled_cash` holds the **gross, trade-attributed** view (allocating the net settlement across constituent cashflows pro-rata via the bridge), and a separate `gold_fact_nostro_movement` holds the **actual net account movements** as they appear on the statement. The two are reconciled by `netting_group_id`.

> This is precisely the question "how would you reconcile settled cash against the source?" is testing. The gross view and the nostro will *never* tie line-for-line, and knowing why is the answer.

---

## 7.5 Deriving cashflow status from settlements

```sql
-- Cashflow settlement status: derived, never asserted
CREATE OR REPLACE VIEW gold_vw_cashflow_settlement_status AS
WITH settled AS (
  SELECT cashflow_id,
         SUM(CASE WHEN settlement_status IN ('SETTLED','PARTIALLY_SETTLED')
                  THEN settled_amount ELSE 0 END)                       AS total_settled,
         MAX(CASE WHEN settlement_status = 'FAILED' THEN 1 ELSE 0 END)  AS has_failed_attempt,
         MAX(settlement_date)                                           AS last_settlement_date,
         MAX(days_late)                                                 AS max_days_late
    FROM gold_fact_settled_cash
   WHERE is_reversal = false
   GROUP BY cashflow_id
)
SELECT cf.cashflow_id,
       cf.trade_id,
       cf.currency_code,
       cf.expected_amount,
       COALESCE(s.total_settled, 0)                          AS settled_amount,
       cf.expected_amount - COALESCE(s.total_settled, 0)     AS unsettled_amount,
       s.last_settlement_date,
       s.max_days_late,
       CASE
         WHEN cf.cashflow_status IN ('CANCELLED','SUPERSEDED')            THEN cf.cashflow_status
         WHEN ABS(cf.expected_amount - COALESCE(s.total_settled,0))
                <= dc.settlement_tolerance                                THEN 'SETTLED'
         WHEN COALESCE(s.total_settled,0) > dc.settlement_tolerance       THEN 'PARTIALLY_SETTLED'
         WHEN s.has_failed_attempt = 1                                    THEN 'FAILED'
         WHEN cf.value_date < current_date()                              THEN 'OVERDUE'
         ELSE 'EXPECTED'
       END                                                    AS derived_settlement_status
  FROM gold_fact_cashflow  cf
  JOIN gold_dim_currency   dc ON cf.currency_sk = dc.currency_sk
  LEFT JOIN settled        s  ON cf.cashflow_id = s.cashflow_id
 WHERE cf.is_current = true;
```

`dim_currency.settlement_tolerance` is `0.01` for GBP/USD/EUR and `1` for JPY. Currency-aware tolerance eliminates an entire class of false reconciliation breaks.

## 7.6 Reconciliation: the control that makes settled cash trustworthy

```
   Internal view                          External view
   ────────────                           ─────────────
   gold_fact_settled_cash                 camt.053 statement entries
   (what we think settled)                (what the agent bank says)
          │                                      │
          └──────────────┬───────────────────────┘
                         v
             match on (account, currency, settlement_date,
                       amount, payment_reference / UETR)
                         │
            ┌────────────┼─────────────┐
            v            v             v
        MATCHED      UNMATCHED       BREAK
                     (timing)     (amount / missing)
                                       │
                                  ageing buckets,
                                  ops workflow,
                                  root-cause dimension
```

| Break type | Meaning | Typical cause |
|---|---|---|
| **In our book, not on statement** | We think it settled; the bank doesn't | Optimistic status from an instruction rather than a confirmation. **The most dangerous kind** |
| **On statement, not in our book** | Cash arrived unexpectedly | Unmatched receipt, a counterparty paying early, an unbooked trade |
| **Amount difference** | Both agree it settled, different amounts | Fees deducted in the chain, FX conversion, partial |
| **Date difference** | Both agree the amount, different dates | Value-date vs booking-date convention at the agent |

**Ageing is mandatory:** a break aged over 5 business days is an operational-risk and regulatory-reporting matter, not a data issue. Model `break_age_days` and `break_owner` as attributes so ops workflow can drive off the warehouse.

## 7.7 What "settled cash" is used for

| Consumer | Use |
|---|---|
| **Finance / GL** | Cash-basis accounting entries: DR/CR nostro, realised FX P&L, intercompany |
| **Treasury** | Actual end-of-day cash position by currency and account; funding requirements |
| **Liquidity risk** | LCR/NSFR inputs; intraday liquidity (BCBS 248) reporting |
| **Settlement risk** | Herstatt exposure — value in flight where we have paid and not received |
| **Credit risk** | Settlement-risk limit usage by counterparty |
| **Ops** | Fail management, claims, nostro reconciliation, cut-off performance |
| **Regulatory** | EMIR/SFTR settlement fields, CSDR-style fail reporting analogues, intraday liquidity monitoring |
| **Client service** | "Did my money arrive?" — answerable with a timestamp and a reference |

---
# PART 8 — MTD AND LTD FINANCE EXAMPLE

## 8.1 What MTD and LTD actually mean here

| Measure | Definition | Date column driving it | Reset |
|---|---|---|---|
| **MTD** — Month To Date | Sum of settled cash where the **settlement date** falls between the fiscal month start and the as-at date | `settlement_date_sk` → `dim_date.fiscal_month` | Every month |
| **QTD / YTD** | Same, quarter / year boundaries | `dim_date.fiscal_quarter` / `fiscal_year` | Quarterly / annually |
| **LTD** — Life To Date | Cumulative settled cash from inception to the as-at date | `settlement_date_sk <= as_at` | Never |
| **Outstanding** | Expected cashflows not yet settled | `value_date` and `settlement_status` | Continuous |
| **Failed** | Cashflows due but not settled, with a failure event | `value_date < as_at` and status `FAILED` | Continuous |

**Three rules that keep Finance and the data platform agreeing:**

1. **Settled cash is dated by `settlement_date`, never by `value_date` or `trade_date`.** Cash-basis accounting recognises cash when it moves.
2. **LTD is a scope, not a separate table.** `LTD = SUM(...) WHERE settlement_date <= as_at`, on the same fact table. Building a "LTD summary table" that drifts from the detail is a classic anti-pattern.
3. **The fiscal calendar governs, not the Gregorian one.** `dim_date.fiscal_month_start_date` — a firm whose September closes on 28-Sep gets a different MTD than one that closes on 30-Sep, and only the date dimension knows which.

## 8.2 The base measure

Everything below is `SUM(signed_settled_amount)` filtered and grouped. Because direction is baked into the sign at load time, no consumer ever writes direction logic.

```sql
-- The one expression every cash KPI is built from
SUM(signed_settled_amount)                  -- native currency, additive
SUM(signed_settled_amount_base_ccy)         -- USD equivalent, additive across currencies
```

---

## 8.3 MTD settled amount — September 2026

```sql
-- MTD settled cash by currency, GMB London, September 2026
SELECT  dc.currency_code,
        SUM(CASE WHEN f.payment_direction = 'RECEIVE' THEN f.settled_amount ELSE 0 END) AS receipts,
        SUM(CASE WHEN f.payment_direction = 'PAY'     THEN f.settled_amount ELSE 0 END) AS payments,
        SUM(f.signed_settled_amount)                                                    AS net_settled,
        SUM(f.signed_settled_amount_base_ccy)                                           AS net_settled_usd,
        COUNT(*)                                                                        AS settlement_count
  FROM  gold_fact_settled_cash  f
  JOIN  gold_dim_date           dd ON f.settlement_date_sk    = dd.date_sk
  JOIN  gold_dim_currency       dc ON f.currency_sk           = dc.currency_sk
  JOIN  gold_dim_legal_entity   le ON f.legal_entity_sk       = le.legal_entity_sk
  JOIN  gold_dim_settlement_status ss ON f.settlement_status_sk = ss.settlement_status_sk
 WHERE  dd.fiscal_year          = 2026
   AND  dd.fiscal_month         = 9
   AND  dd.calendar_date       <= DATE '2026-09-30'
   AND  le.legal_entity_id      = 'GMB-LDN'
   AND  ss.counts_as_settled    = TRUE          -- excludes FAILED / CANCELLED
   AND  f.is_reversal           = FALSE
 GROUP BY dc.currency_code
 ORDER BY dc.currency_code;
```

**Result:**

| Currency | Receipts | Payments | **Net settled (native)** | FX to USD | **Net settled (USD)** | Count |
|---|---|---|---|---|---|---|
| EUR | 0.00 | 5,000,298.61 | **−5,000,298.61** | 1.0925 | **−5,462,826.23** | 2 |
| GBP | 10,000,000.00 | 0.00 | **+10,000,000.00** | 1.3310 | **+13,310,000.00** | 1 |
| JPY | 0 | 3,000,000,000 | **−3,000,000,000** | 0.0067024 | **−20,107,238.61** | 2 |
| USD | 25,450,000.00 | 13,200,000.00 | **+12,250,000.00** | 1.0000 | **+12,250,000.00** | 3 |
| **Total** | | | | | **−10,064.84** | **8** |

*(FX rates as at 30-Sep-2026: GBP/USD 1.3310, EUR/USD 1.0925, USD/JPY 149.20.)*

**Reading the result like a Finance business partner:**

- Eight settlement movements settled in September; the failed attempt `STL-90004` is excluded by `counts_as_settled = TRUE` but remains in the table for fail reporting.
- **The USD-equivalent total is −10,064.84 — essentially zero.** That is exactly what you expect from a matched FX book: every trade exchanges equal value in two currencies, so gross cash is large and net cash is small. The residual is trading margin, the fail claim of EUR 298.61, and revaluation drift between deal rates and month-end rates.
- **A large non-zero total here is an alarm**, not a result. It means either an unmatched leg, a mis-signed direction, or a missing settlement. Build this as a control, not just a report.

---

## 8.4 LTD settled amount

```sql
-- Life-to-date settled cash, as at any date
SELECT  dc.currency_code,
        SUM(f.signed_settled_amount)          AS ltd_net_settled,
        SUM(f.signed_settled_amount_base_ccy) AS ltd_net_settled_usd,
        MIN(dd.calendar_date)                 AS first_settlement,
        MAX(dd.calendar_date)                 AS last_settlement,
        COUNT(*)                              AS ltd_settlement_count
  FROM  gold_fact_settled_cash     f
  JOIN  gold_dim_date              dd ON f.settlement_date_sk    = dd.date_sk
  JOIN  gold_dim_currency          dc ON f.currency_sk           = dc.currency_sk
  JOIN  gold_dim_legal_entity      le ON f.legal_entity_sk       = le.legal_entity_sk
  JOIN  gold_dim_settlement_status ss ON f.settlement_status_sk  = ss.settlement_status_sk
 WHERE  dd.calendar_date        <= DATE '2026-12-31'     -- ← the ONLY difference from MTD
   AND  le.legal_entity_id       = 'GMB-LDN'
   AND  ss.counts_as_settled     = TRUE
   AND  f.is_reversal            = FALSE
 GROUP BY dc.currency_code;
```

Assuming the December far legs settle on time (TRD10003 far leg and TRD10005 v2 on 03-Dec-2026):

| Currency | Sep MTD | Dec MTD | **LTD @ 31-Dec-2026** | FX @ 31-Dec | **LTD (USD)** |
|---|---|---|---|---|---|
| GBP | +10,000,000.00 | +6,000,000.00 | **+16,000,000.00** | 1.3400 | **+21,440,000.00** |
| USD | +12,250,000.00 | −27,950,000.00 | **−15,700,000.00** | 1.0000 | **−15,700,000.00** |
| EUR | −5,000,298.61 | 0.00 | **−5,000,298.61** | 1.1000 | **−5,500,328.47** |
| JPY | −3,000,000,000 | +2,974,000,000 | **−26,000,000** | 0.00675676 | **−175,675.68** |
| **Total** | | | | | **+63,995.85** |

**What the JPY line teaches:** LTD JPY of −26,000,000 is not an error. It is the **cost of the FX swap** — GMB borrowed USD against JPY for 91 days and paid JPY 26,000,000 (≈ USD 174,849 at the far-leg rate of 148.70, ≈ USD 175,676 revalued at 31-Dec — either way ≈ 3.5% annualised on USD 20m) for the privilege. **The swap's entire economics show up as an LTD cash residual in the JPY leg while the USD legs net to zero.** Being able to explain a cash number in terms of the trade economics that produced it is what Finance wants from a data architect.

> **A warning worth voicing in interview:** the LTD USD total changes with the revaluation rate, because you are converting balances in four currencies at a point-in-time rate. **Settled cash is not P&L.** If Finance wants P&L, convert each settlement at its own settlement-date rate and hold the revaluation separately. Store `fx_rate_to_base` and `fx_rate_date` on the fact so both treatments are available.

---

## 8.5 Outstanding settlement amount

```sql
-- Expected cashflows not yet fully settled, as at a date
SELECT  dc.currency_code,
        SUM(CASE WHEN cf.value_date <  DATE '2026-09-30'
                 THEN cf.unsettled_amount ELSE 0 END)   AS overdue_amount,
        SUM(CASE WHEN cf.value_date >= DATE '2026-09-30'
                 THEN cf.unsettled_amount ELSE 0 END)   AS not_yet_due_amount,
        SUM(cf.unsettled_amount)                        AS total_outstanding,
        SUM(cf.unsettled_amount
            * CASE cf.payment_direction WHEN 'PAY' THEN -1 ELSE 1 END
            * cf.fx_rate_to_base)                       AS net_outstanding_usd
  FROM  gold_vw_cashflow_settlement_status  cf
  JOIN  gold_dim_currency                   dc ON cf.currency_sk = dc.currency_sk
 WHERE  cf.derived_settlement_status IN ('EXPECTED','PARTIALLY_SETTLED','OVERDUE','FAILED')
   AND  cf.is_current = TRUE
 GROUP BY dc.currency_code;
```

**Result as at 30-Sep-2026:**

| Currency | Trade / cashflow | Direction | Outstanding | Value date | Due status | USD equiv (signed) |
|---|---|---|---|---|---|---|
| USD | TRD10003 CF-10003-03 | PAY | 20,000,000.00 | 2026-12-03 | Not yet due | −20,000,000.00 |
| USD | TRD10005 CF-10005-04 | PAY | 7,950,000.00 | 2026-12-03 | Not yet due | −7,950,000.00 |
| GBP | TRD10005 CF-10005-03 | RECEIVE | 6,000,000.00 | 2026-12-03 | Not yet due | +7,986,000.00 |
| JPY | TRD10003 CF-10003-04 | RECEIVE | 2,974,000,000 | 2026-12-03 | Not yet due | +19,932,975.87 |
| | | | | | **Net** | **−31,024.13** |

**Critical distinction to make explicit in the model:** all four of these are **not yet due**, not overdue. `derived_settlement_status = 'EXPECTED'`. An "outstanding settlements" report that lumps a December forward in with a payment that failed yesterday is useless to Operations. Always split by `value_date` versus the as-at date, and expose `overdue_amount` and `not_yet_due_amount` as separate measures.

---

## 8.6 Failed settlement amount

```sql
-- Failed settlements with ageing and root cause
SELECT  dc.currency_code,
        fr.fail_reason_code,
        fr.root_cause_category,
        COUNT(*)                                                              AS fail_count,
        SUM(f.expected_amount)                                                AS failed_amount,
        SUM(f.expected_amount * f.fx_rate_to_base)                            AS failed_amount_usd,
        AVG(DATEDIFF(DATE '2026-09-30', dv.calendar_date))                    AS avg_age_days,
        MAX(DATEDIFF(DATE '2026-09-30', dv.calendar_date))                    AS max_age_days
  FROM  gold_fact_settled_cash   f
  JOIN  gold_dim_date            dv ON f.value_date_sk  = dv.date_sk
  JOIN  gold_dim_currency        dc ON f.currency_sk    = dc.currency_sk
  JOIN  gold_dim_fail_reason     fr ON f.fail_reason_sk = fr.fail_reason_sk
  JOIN  gold_dim_settlement_status ss ON f.settlement_status_sk = ss.settlement_status_sk
 WHERE  ss.status_code = 'FAILED'
   AND  dv.calendar_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
 GROUP BY dc.currency_code, fr.fail_reason_code, fr.root_cause_category;
```

**Result — September 2026:**

| Currency | Fail reason | Root cause | Count | Failed amount | USD equiv | Age at 30-Sep | Resolution |
|---|---|---|---|---|---|---|---|
| EUR | `INSUFFICIENT_FUNDS` | `TREASURY_FUNDING` | 1 | 5,000,000.00 | 5,462,500.00 | Resolved (1 day) | Settled 04-Sep as `STL-90005`; claim EUR 298.61 paid 15-Sep |

**Related settlement-quality KPIs from the same fact table:**

| KPI | Definition | September value |
|---|---|---|
| Fail rate (count) | Failed ÷ total attempted movements | 1 ÷ 9 = **11.1%** |
| Fail rate (value, USD) | Failed value ÷ total attempted value | 5,462,500 ÷ 77,530,065 = **7.0%** |
| Late settlement rate | Movements with `days_late > 0` ÷ settled movements | 2 ÷ 8 = **25.0%** (STL-90005, STL-90008) |
| Partial settlement rate | Cashflows settled by >1 movement ÷ cashflows settled | 1 ÷ 7 = **14.3%** (CF-10003-02) |
| Average days late (when late) | AVG(`days_late`) where > 0 | **1.0 day** |
| Cost of fails | SUM of `COMPENSATION_CLAIM` cashflows | **EUR 298.61** |

*(Total attempted value is the USD equivalent of all nine movements at 30-Sep rates: 10,000,000 GBP@1.3310 + 13,200,000 + 5,450,000 + 5,000,000 EUR@1.0925 + 20,000,000 + 3,000,000,000 JPY@0.0067024 + 298.61 EUR@1.0925 = 77,530,064.84.)*

> **Why the failed row must exist.** `STL-90004` has `settled_amount = 0.00` and therefore contributes nothing to any settled-cash total — but it is the *only* source of the fail count, the fail value, the fail reason and the ageing. Insert-only fact modelling of failures is what makes settlement-quality reporting possible at all.

---

## 8.7 Cash position by currency

```sql
-- Daily cash position by currency and account (running balance)
SELECT  dd.calendar_date,
        dc.currency_code,
        da.account_id,
        SUM(f.signed_settled_amount)                                        AS daily_movement,
        SUM(SUM(f.signed_settled_amount)) OVER (
              PARTITION BY dc.currency_code, da.account_id
              ORDER BY dd.calendar_date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)             AS running_balance
  FROM  gold_fact_settled_cash     f
  JOIN  gold_dim_date              dd ON f.settlement_date_sk   = dd.date_sk
  JOIN  gold_dim_currency          dc ON f.currency_sk          = dc.currency_sk
  JOIN  gold_dim_account           da ON f.account_sk           = da.account_sk
  JOIN  gold_dim_settlement_status ss ON f.settlement_status_sk = ss.settlement_status_sk
 WHERE  ss.counts_as_settled = TRUE
 GROUP BY dd.calendar_date, dc.currency_code, da.account_id
 ORDER BY dc.currency_code, dd.calendar_date;
```

**Result — daily movements and running nostro balances (contribution from this portfolio only):**

| Date | Account | Ccy | Movement | Running balance |
|---|---|---|---|---|
| 2026-09-03 | ACC-GBP-01 | GBP | +10,000,000.00 | +10,000,000.00 |
| 2026-09-03 | ACC-USD-01 | USD | +12,250,000.00 | +12,250,000.00 |
| 2026-09-03 | ACC-JPY-01 | JPY | −2,000,000,000 | −2,000,000,000 |
| 2026-09-04 | ACC-EUR-01 | EUR | −5,000,000.00 | −5,000,000.00 |
| 2026-09-04 | ACC-JPY-01 | JPY | −1,000,000,000 | −3,000,000,000 |
| 2026-09-15 | ACC-EUR-01 | EUR | −298.61 | −5,000,298.61 |

> **Position is a semi-additive measure.** `running_balance` sums across accounts and currencies but **not across dates** — the 30-Sep balance is the *latest* value, not the sum of daily balances. This distinction belongs in the semantic layer's metric definitions so BI tools cannot get it wrong. In a Databricks Metric View or a Snowflake semantic view, declare `running_balance` with `aggregate: LAST` over the date dimension.

## 8.8 Cash position by legal entity

```sql
SELECT  le.legal_entity_id,
        le.entity_name,
        le.booking_location,
        le.functional_currency,
        dc.currency_code,
        SUM(f.signed_settled_amount)          AS net_settled_native,
        SUM(f.signed_settled_amount_base_ccy) AS net_settled_usd
  FROM  gold_fact_settled_cash     f
  JOIN  gold_dim_legal_entity      le ON f.legal_entity_sk      = le.legal_entity_sk AND le.is_current
  JOIN  gold_dim_currency          dc ON f.currency_sk          = dc.currency_sk
  JOIN  gold_dim_date              dd ON f.settlement_date_sk   = dd.date_sk
  JOIN  gold_dim_settlement_status ss ON f.settlement_status_sk = ss.settlement_status_sk
 WHERE  dd.fiscal_year = 2026 AND dd.fiscal_month = 9
   AND  ss.counts_as_settled = TRUE
 GROUP BY ROLLUP (le.legal_entity_id, le.entity_name, le.booking_location,
                  le.functional_currency, dc.currency_code);
```

| Legal entity | Location | Functional ccy | Currency | Net settled (native) | Net settled (USD) |
|---|---|---|---|---|---|
| GMB-LDN | London | GBP | EUR | −5,000,298.61 | −5,462,826.23 |
| GMB-LDN | London | GBP | GBP | +10,000,000.00 | +13,310,000.00 |
| GMB-LDN | London | GBP | JPY | −3,000,000,000 | −20,107,238.61 |
| GMB-LDN | London | GBP | USD | +12,250,000.00 | +12,250,000.00 |
| **GMB-LDN total** | | | | | **−10,064.84** |

**Why legal entity matters more than it looks:** each entity has its own general ledger, its own functional currency, its own regulator and its own statutory accounts. A cash number that is not attributable to a legal entity cannot be posted, cannot be audited and cannot be reported. This is why `legal_entity_sk` is mandatory on the settled-cash fact and why `dim_legal_entity` carries `gl_company_code` and `functional_currency`.

## 8.9 Cash position by counterparty

```sql
SELECT  cp.counterparty_id,
        cp.counterparty_name,
        cp.ultimate_parent_name,
        cp.emir_classification,
        dc.currency_code,
        SUM(f.signed_settled_amount)                                          AS mtd_net_settled,
        SUM(f.signed_settled_amount_base_ccy)                                 AS mtd_net_settled_usd,
        SUM(CASE WHEN ss.status_code = 'FAILED' THEN f.expected_amount ELSE 0 END) AS failed_amount,
        COUNT(*)                                                              AS movement_count
  FROM  gold_fact_settled_cash     f
  JOIN  gold_dim_counterparty      cp ON f.counterparty_sk      = cp.counterparty_sk AND cp.is_current
  JOIN  gold_dim_currency          dc ON f.currency_sk          = dc.currency_sk
  JOIN  gold_dim_date              dd ON f.settlement_date_sk   = dd.date_sk
  JOIN  gold_dim_settlement_status ss ON f.settlement_status_sk = ss.settlement_status_sk
 WHERE  dd.fiscal_year = 2026 AND dd.fiscal_month = 9
 GROUP BY cp.counterparty_id, cp.counterparty_name, cp.ultimate_parent_name,
          cp.emir_classification, dc.currency_code;
```

| Counterparty | EMIR class | Currency | MTD net settled | USD equiv | Failed | Movements |
|---|---|---|---|---|---|---|
| ABC Bank Limited (CP-001) | FC | GBP | +10,000,000.00 | +13,310,000.00 | — | 1 |
| ABC Bank Limited (CP-001) | FC | JPY | −3,000,000,000 | −20,107,238.61 | — | 2 |
| ABC Bank Limited (CP-001) | FC | USD | +6,800,000.00 | +6,800,000.00 | — | 2 |
| **ABC Bank total** | | | | **+2,761.39** | | **5** |
| XYZ Asset Management (CP-002) | NFC+ | EUR | −5,000,298.61 | −5,462,826.23 | 5,000,000.00 | 3 |
| XYZ Asset Management (CP-002) | NFC+ | USD | +5,450,000.00 | +5,450,000.00 | — | 1 |
| **XYZ AM total** | | | | **−12,826.23** | | **4** |
| **Grand total** | | | | **−10,064.84** | | **9** |

*(ABC Bank USD: +20,000,000 received on TRD10003 less 13,200,000 paid on TRD10001 = +6,800,000 — the net figure that actually moved under the netting agreement, reconciling to the gross attribution in §7.4.6.)*

**Counterparty cash position drives four different conversations:**

| Consumer | Reads this as |
|---|---|
| Credit Risk | Settlement-risk exposure and limit usage per counterparty |
| Operations | Which counterparties cause fails — CP-002 is the whole September fail population |
| Front office | Client profitability and relationship value |
| Regulatory | Counterparty classification drives EMIR clearing and margin obligations |

## 8.10 Putting it into a semantic metric layer

Rather than letting each consumer write these joins, publish the definitions once — as a Databricks **Metric View**, a Snowflake **semantic view**, or a dbt metrics layer:

```yaml
metrics:
  - name: mtd_settled_cash_usd
    label: "MTD Settled Cash (USD)"
    source: gold_fact_settled_cash
    measure: SUM(signed_settled_amount_base_ccy)
    filters:
      - settlement_status.counts_as_settled = true
      - is_reversal = false
    time_dimension: settlement_date
    grain: fiscal_month_to_date
    dimensions: [legal_entity, counterparty, currency, product, account, book]
    additivity: fully_additive

  - name: outstanding_settlement_amount
    label: "Outstanding Settlement Amount"
    source: gold_vw_cashflow_settlement_status
    measure: SUM(unsettled_amount * direction_sign * fx_rate_to_base)
    filters:
      - derived_settlement_status IN ('EXPECTED','PARTIALLY_SETTLED','OVERDUE','FAILED')
      - is_current = true
    time_dimension: value_date
    additivity: fully_additive

  - name: cash_position_by_currency
    label: "Cash Position by Currency"
    source: gold_fact_settled_cash
    measure: SUM(signed_settled_amount)
    time_dimension: settlement_date
    additivity: semi_additive          # LAST over time, SUM over other dimensions
```

**Why this matters:** the moment "MTD settled cash" exists as a governed metric rather than as SQL pasted into eleven dashboards, Finance, Treasury and Risk stop producing different numbers for the same question. That outcome — not the star schema — is what a Solution Director is actually being hired to deliver.

---
# PART 9 — PHYSICAL DATA MODEL

## 9.1 Where each logical model lives

```
   ┌─────────────────────────────────────────────────────────────────────┐
   │ BRONZE  —  raw, immutable, source-shaped                            │
   │ The FpML message exactly as received, plus envelope metadata.       │
   │ No parsing beyond what is needed to partition. Never deleted.       │
   │ ► NO logical model applies here. Bronze is "the tape".              │
   └───────────────────────────┬─────────────────────────────────────────┘
                               v
   ┌─────────────────────────────────────────────────────────────────────┐
   │ SILVER  —  parsed, validated, conformed, deduplicated               │
   │ ► THE CANONICAL DATA MODEL LIVES HERE.                              │
   │ Normalised, bitemporal, source-agnostic, business-key resolved.     │
   │ One version of the truth. Not optimised for BI queries.             │
   └───────────────────────────┬─────────────────────────────────────────┘
                               v
   ┌─────────────────────────────────────────────────────────────────────┐
   │ GOLD  —  dimensional, aggregated, consumer-shaped                   │
   │ ► THE SEMANTIC DATA MODEL LIVES HERE.                               │
   │ Star schemas, conformed dimensions, metric views, data products.    │
   │ Optimised for query. Rebuildable from Silver at any time.           │
   └─────────────────────────────────────────────────────────────────────┘
```

| Layer | Logical model | Grain | Mutability | Retention | Owner |
|---|---|---|---|---|---|
| Bronze | none (source shape) | one message | append-only, immutable | 7–10 years (regulatory) | Platform engineering |
| Silver | **Canonical** | one entity version | SCD2 / append-only | 7–10 years | Enterprise Data Architecture |
| Gold | **Semantic** | one business fact | rebuildable | as required by consumers | Data product teams |

**The two sentences to say in an interview:** *"The canonical model is Silver — it is where source volatility stops. The semantic model is Gold — it is where consumer volatility stops. Bronze is the immutable evidence that lets me rebuild both."*

---

## 9.2 Databricks + Delta Lake implementation

### 9.2.1 Bronze — `bronze_fpml_message`

```sql
CREATE TABLE IF NOT EXISTS gmb_lakehouse.bronze.bronze_fpml_message (
    -- Ingestion envelope
    ingestion_id            STRING    NOT NULL COMMENT 'UUID assigned at ingestion',
    kafka_topic             STRING,
    kafka_partition         INT,
    kafka_offset            BIGINT,
    ingestion_timestamp     TIMESTAMP NOT NULL,
    ingestion_date          DATE      NOT NULL COMMENT 'Partition column',
    -- Lightly parsed routing keys (extracted from the envelope only)
    message_id              STRING    COMMENT 'FpML header/messageId - idempotency key',
    correlation_id          STRING,
    sequence_number         INT,
    sent_by                 STRING,
    sent_to                 STRING,
    message_creation_ts     TIMESTAMP,
    fpml_version            STRING    COMMENT 'FpML/@fpmlVersion, e.g. 5-13',
    fpml_view               STRING    COMMENT 'confirmation | reporting | recordkeeping',
    message_type            STRING    COMMENT 'executionNotification, tradeAmended, ...',
    source_system           STRING,
    -- The payload, untouched
    raw_payload             STRING    NOT NULL COMMENT 'Complete FpML XML, verbatim',
    payload_hash            STRING    NOT NULL COMMENT 'SHA-256 for exact-duplicate detection',
    payload_size_bytes      BIGINT,
    -- Validation outcome (recorded, not enforced)
    schema_valid            BOOLEAN,
    schema_validation_errors STRING,
    -- Lineage
    _rescued_data           STRING    COMMENT 'Anything the reader could not place'
)
USING DELTA
PARTITIONED BY (ingestion_date)
CLUSTER BY (source_system, message_type)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'      = 'true',
    'delta.autoOptimize.optimizeWrite'= 'true',
    'delta.autoOptimize.autoCompact'  = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'delta.appendOnly'                = 'true',
    'quality'                         = 'bronze'
);
```

**Bronze design rules:**

| Rule | Reason |
|---|---|
| Store the payload **verbatim** as a string | Regulatory evidence; enables full replay after a mapping bug |
| **Never** transform, cleanse or reject in Bronze | A rejected message you did not store is a message you cannot investigate |
| Extract only envelope fields needed for routing and partitioning | Keeps ingestion fast and version-independent |
| `payload_hash` for exact duplicates; `message_id` for logical duplicates | Two different dedup problems, two different keys |
| `delta.appendOnly = true` | Makes immutability a table property, not a convention |
| Partition by `ingestion_date`, cluster by source and type | Ingestion-date partitioning matches the write pattern; liquid clustering handles read patterns |

**Sample Bronze row — TRD10001:**

| ingestion_id | message_id | fpml_version | message_type | source_system | payload_hash | schema_valid | ingestion_date |
|---|---|---|---|---|---|---|---|
| `a3f1…` | `MSG-20260901-000451` | `5-13` | `executionNotification` | `MUREX_FX` | `9c2e…` | true | 2026-09-01 |

### 9.2.2 Silver — the canonical tables

```sql
CREATE TABLE gmb_lakehouse.silver.silver_trade (
    trade_sk                BIGINT   GENERATED ALWAYS AS IDENTITY,
    trade_id                STRING   NOT NULL COMMENT 'Enterprise trade id, stable across versions',
    source_trade_id         STRING   NOT NULL COMMENT 'FpML tradeHeader/partyTradeIdentifier/tradeId (our party)',
    counterparty_trade_id   STRING            COMMENT 'Their reference',
    uti                     STRING            COMMENT 'Unique Trade Identifier (EMIR/CFTC)',
    trade_version           INT      NOT NULL,
    trade_date              DATE     NOT NULL COMMENT 'FpML tradeHeader/tradeDate',
    execution_timestamp     TIMESTAMP         COMMENT 'FpML partyTradeInformation/executionDateTime',
    effective_date          DATE,
    settlement_date         DATE              COMMENT 'FpML fxSingleLeg/valueDate',
    maturity_date           DATE,
    product_sk              BIGINT   NOT NULL,
    asset_class             STRING   NOT NULL,
    product_type            STRING   NOT NULL,
    source_product_type     STRING            COMMENT 'As labelled by the source, before derivation',
    booking_entity_sk       BIGINT   NOT NULL COMMENT 'Our side - derived from partyTradeInformation',
    booking_branch_id       STRING,
    counterparty_sk         BIGINT   NOT NULL COMMENT 'Their side - set complement',
    trader_id               STRING,
    book_id                 STRING,
    trade_status            STRING   NOT NULL COMMENT 'Derived state machine over trade events',
    confirmation_status     STRING,
    clearing_status         STRING,
    execution_venue         STRING,
    master_agreement_type   STRING,
    reporting_regimes       ARRAY<STRING>,
    is_intragroup           BOOLEAN,
    parent_trade_sk         BIGINT            COMMENT 'Novation / allocation lineage',
    -- provenance
    source_system           STRING   NOT NULL,
    fpml_version            STRING,
    source_message_id       STRING   NOT NULL,
    bronze_ingestion_id     STRING   NOT NULL COMMENT 'Row-level lineage to Bronze',
    -- bitemporality
    business_effective_from TIMESTAMP NOT NULL,
    business_effective_to   TIMESTAMP,
    valid_from              TIMESTAMP NOT NULL,
    valid_to                TIMESTAMP,
    is_current              BOOLEAN   NOT NULL,
    created_timestamp       TIMESTAMP NOT NULL,
    updated_timestamp       TIMESTAMP NOT NULL,
    CONSTRAINT pk_silver_trade PRIMARY KEY (trade_sk),
    CONSTRAINT chk_version    CHECK (trade_version >= 1),
    CONSTRAINT chk_dates      CHECK (settlement_date IS NULL OR settlement_date >= trade_date)
)
USING DELTA
PARTITIONED BY (trade_date)
CLUSTER BY (trade_id, counterparty_sk)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'silver');
```

```sql
CREATE TABLE gmb_lakehouse.silver.silver_cashflow (
    cashflow_sk             BIGINT   GENERATED ALWAYS AS IDENTITY,
    cashflow_id             STRING   NOT NULL,
    trade_sk                BIGINT   NOT NULL,
    trade_id                STRING   NOT NULL,
    trade_version           INT      NOT NULL,
    trade_leg_sk            BIGINT,
    leg_number              INT      NOT NULL,
    cashflow_seq            INT      NOT NULL,
    cashflow_type           STRING   NOT NULL COMMENT 'PRINCIPAL_EXCHANGE | INTEREST | PREMIUM | FEE | COMPENSATION_CLAIM',
    cashflow_basis          STRING   NOT NULL COMMENT 'CONTRACTUAL | FORECAST | RESET_FIXED | ACTUAL',
    currency_code           STRING   NOT NULL,
    expected_amount         DECIMAL(28,10) NOT NULL COMMENT 'Always positive',
    payment_direction       STRING   NOT NULL COMMENT 'PAY | RECEIVE, relative to booking entity',
    direction_sign          SMALLINT NOT NULL,
    signed_amount           DECIMAL(28,10) NOT NULL,
    value_date              DATE     NOT NULL COMMENT 'CONTRACTUAL date cash is due',
    unadjusted_payment_date DATE,
    payer_party_sk          BIGINT   NOT NULL,
    receiver_party_sk       BIGINT   NOT NULL,
    booking_entity_sk       BIGINT   NOT NULL,
    counterparty_sk         BIGINT   NOT NULL,
    account_sk              BIGINT,
    settlement_instruction_sk BIGINT,
    cashflow_status         STRING   NOT NULL COMMENT 'EXPECTED|INSTRUCTED|PARTIALLY_SETTLED|SETTLED|FAILED|CANCELLED|SUPERSEDED',
    netting_group_id        STRING,
    is_netted               BOOLEAN,
    discount_factor         DECIMAL(18,12),
    source_system           STRING   NOT NULL,
    source_message_id       STRING,
    valid_from              TIMESTAMP NOT NULL,
    valid_to                TIMESTAMP,
    is_current              BOOLEAN   NOT NULL,
    CONSTRAINT pk_silver_cashflow PRIMARY KEY (cashflow_sk),
    CONSTRAINT chk_amount_positive CHECK (expected_amount >= 0),
    CONSTRAINT chk_direction CHECK (payment_direction IN ('PAY','RECEIVE')),
    CONSTRAINT chk_sign CHECK ((payment_direction='PAY' AND direction_sign=-1)
                            OR (payment_direction='RECEIVE' AND direction_sign=1))
)
USING DELTA
PARTITIONED BY (value_date)
CLUSTER BY (trade_id, currency_code);
```

```sql
CREATE TABLE gmb_lakehouse.silver.silver_settlement (
    settlement_sk           BIGINT   GENERATED ALWAYS AS IDENTITY,
    settlement_id           STRING   NOT NULL,
    payment_sk              BIGINT   NOT NULL,
    payment_id              STRING   NOT NULL,
    cashflow_sk             BIGINT   NOT NULL,
    cashflow_id             STRING   NOT NULL,
    trade_sk                BIGINT   NOT NULL,
    trade_id                STRING   NOT NULL,
    account_sk              BIGINT   NOT NULL,
    currency_code           STRING   NOT NULL,
    settled_amount          DECIMAL(28,10) NOT NULL,
    settlement_direction    STRING   NOT NULL,
    direction_sign          SMALLINT NOT NULL,
    signed_settled_amount   DECIMAL(28,10) NOT NULL,
    value_date              DATE     NOT NULL COMMENT 'Contractual',
    settlement_date         DATE              COMMENT 'ACTUAL - null while unsettled/failed',
    settlement_timestamp    TIMESTAMP,
    settlement_status       STRING   NOT NULL,
    settlement_method       STRING,
    fail_reason_code        STRING,
    days_late               INT,
    confirmation_source     STRING   NOT NULL COMMENT 'CLS_REPORT|CAMT054|CAMT053|MT950|MANUAL',
    confirmation_reference  STRING,
    bank_statement_id       STRING,
    reversal_of_settlement_sk BIGINT,
    is_reversal             BOOLEAN  NOT NULL DEFAULT false,
    reconciliation_status   STRING,
    source_system           STRING   NOT NULL,
    created_timestamp       TIMESTAMP NOT NULL,
    CONSTRAINT pk_silver_settlement PRIMARY KEY (settlement_sk),
    CONSTRAINT chk_settled_nonneg CHECK (settled_amount >= 0),
    CONSTRAINT chk_settled_date CHECK (settlement_date IS NULL OR settlement_date >= value_date - INTERVAL 5 DAYS)
)
USING DELTA
PARTITIONED BY (settlement_date)
CLUSTER BY (currency_code, account_sk)
TBLPROPERTIES ('delta.appendOnly' = 'true', 'quality' = 'silver');
```

> Note `delta.appendOnly = true` on settlement. **Settlement facts are never updated.** A reversal is a new row.

Remaining Silver tables follow the same pattern: `silver_trade_leg`, `silver_trade_party_role`, `silver_payment`, `silver_payment_cashflow_allocation`, `silver_settlement_instruction`, `silver_trade_event`, `silver_valuation`, `silver_party`, `silver_account`, `silver_product`.

### 9.2.3 Gold — the semantic tables

```sql
CREATE TABLE gmb_lakehouse.gold.gold_fact_settled_cash (
    settled_cash_sk         BIGINT GENERATED ALWAYS AS IDENTITY,
    -- degenerate dimensions (drill-through keys)
    settlement_id           STRING NOT NULL,
    payment_id              STRING NOT NULL,
    cashflow_id             STRING NOT NULL,
    trade_id                STRING NOT NULL,
    trade_version           INT    NOT NULL,
    -- conformed dimension foreign keys
    settlement_date_sk      INT,                -- null while unsettled
    value_date_sk           INT    NOT NULL,
    trade_date_sk           INT    NOT NULL,
    currency_sk             BIGINT NOT NULL,
    account_sk              BIGINT NOT NULL,
    legal_entity_sk         BIGINT NOT NULL,
    counterparty_sk         BIGINT NOT NULL,
    product_sk              BIGINT NOT NULL,
    book_sk                 BIGINT NOT NULL,
    settlement_status_sk    BIGINT NOT NULL,
    settlement_method_sk    BIGINT NOT NULL,
    fail_reason_sk          BIGINT NOT NULL DEFAULT -1,   -- -1 = 'Not Applicable'
    -- measures
    payment_direction       STRING NOT NULL,
    direction_sign          SMALLINT NOT NULL,
    expected_amount         DECIMAL(28,2) NOT NULL,
    settled_amount          DECIMAL(28,2) NOT NULL,
    signed_settled_amount   DECIMAL(28,2) NOT NULL,
    unsettled_amount        DECIMAL(28,2) NOT NULL,
    variance_amount         DECIMAL(28,2) NOT NULL,
    settled_amount_base_ccy DECIMAL(28,2),
    signed_settled_amount_base_ccy DECIMAL(28,2),
    fx_rate_to_base         DECIMAL(18,10),
    fx_rate_date            DATE,
    fx_rate_source          STRING,
    days_late               INT,
    settlement_count        INT NOT NULL DEFAULT 1,
    -- control
    is_partial              BOOLEAN NOT NULL DEFAULT false,
    is_reversal             BOOLEAN NOT NULL DEFAULT false,
    reverses_settlement_id  STRING,
    is_netted               BOOLEAN NOT NULL DEFAULT false,
    netting_group_id        STRING,
    reconciliation_status   STRING,
    bank_statement_id       STRING,
    confirmation_source     STRING,
    gl_posting_date         DATE,
    gl_journal_id           STRING,
    source_system           STRING NOT NULL,
    ingestion_timestamp     TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (settlement_date_sk)
CLUSTER BY (currency_sk, legal_entity_sk, counterparty_sk)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.dataSkippingNumIndexedCols' = '32',
    'quality'                          = 'gold'
);
```

```sql
CREATE TABLE gmb_lakehouse.gold.gold_dim_counterparty (
    counterparty_sk         BIGINT GENERATED ALWAYS AS IDENTITY,
    counterparty_id         STRING NOT NULL,
    lei                     STRING,
    counterparty_name       STRING NOT NULL,
    short_name              STRING,
    counterparty_type       STRING,
    emir_classification     STRING,
    sector                  STRING,
    country_of_incorporation STRING,
    region                  STRING,
    ultimate_parent_lei     STRING,
    ultimate_parent_name    STRING,
    credit_rating           STRING,
    internal_credit_grade   STRING,
    is_cls_member           BOOLEAN,
    netting_agreement_id    STRING,
    has_csa                 BOOLEAN,
    is_internal             BOOLEAN,
    -- SCD Type 2
    valid_from              TIMESTAMP NOT NULL,
    valid_to                TIMESTAMP,
    is_current              BOOLEAN   NOT NULL,
    scd_hash                STRING    COMMENT 'Hash of Type-2 tracked columns',
    CONSTRAINT pk_dim_cpty PRIMARY KEY (counterparty_sk)
)
USING DELTA
CLUSTER BY (counterparty_id, is_current);

-- Mandatory: the Unknown member, so facts never carry a null FK
INSERT INTO gmb_lakehouse.gold.gold_dim_counterparty
VALUES (-1,'UNKNOWN',NULL,'Unknown Counterparty','UNK','UNKNOWN','UNKNOWN','UNKNOWN',
        'UNKNOWN','UNKNOWN',NULL,'Unknown',NULL,NULL,false,NULL,false,false,
        TIMESTAMP'1900-01-01 00:00:00',NULL,true,'-');
```

### 9.2.4 Physical design decisions that matter

| Decision | Choice | Rationale |
|---|---|---|
| **Partitioning** | Bronze by `ingestion_date`; Silver cashflow by `value_date`; settlement and Gold by `settlement_date_sk` | Partition on the column the dominant query filters. MTD queries filter settlement date, so that is the partition |
| **Liquid clustering vs Z-order** | `CLUSTER BY` (liquid) on high-cardinality query keys | Adapts to changing query patterns without a rewrite; no partition-skew problem |
| **File sizing** | Auto-optimize + auto-compact on | FX volumes create many small files; small-file proliferation is the #1 lakehouse performance killer |
| **Decimal precision** | `DECIMAL(28,10)` in Silver, `DECIMAL(28,2)` in Gold | Silver preserves full precision from source; Gold rounds to reporting precision. **Never `DOUBLE` for money** |
| **CDF (Change Data Feed)** | Enabled on Bronze and Silver | Drives incremental Gold builds and downstream CDC without full rescans |
| **Deletion vectors** | Enabled on Silver SCD2 tables | Makes MERGE-heavy SCD2 updates far cheaper |
| **Append-only** | Enforced on Bronze and `silver_settlement` | Immutability as a table property, not a code convention |
| **Constraints** | `CHECK` constraints on sign, positivity, date ordering | Fail the write, not the report. Delta enforces these |
| **Time travel** | `VERSION AS OF` / `TIMESTAMP AS OF` retained 30+ days | "What did the settled cash report say on 30-Sep before the late arrival?" is answerable without a snapshot table |

### 9.2.5 The pipeline in Databricks

```python
# ---- BRONZE: streaming ingest, no transformation ----
(spark.readStream
      .format("kafka")
      .option("subscribe", "fx.trade.events")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr(
          "uuid() AS ingestion_id",
          "topic AS kafka_topic", "partition AS kafka_partition", "offset AS kafka_offset",
          "current_timestamp() AS ingestion_timestamp",
          "current_date() AS ingestion_date",
          "CAST(value AS STRING) AS raw_payload",
          "sha2(CAST(value AS STRING), 256) AS payload_hash")
      .transform(extract_fpml_envelope)          # messageId, correlationId, version only
      .writeStream
      .option("checkpointLocation", "/mnt/chk/bronze_fpml_message")
      .trigger(processingTime="30 seconds")
      .toTable("gmb_lakehouse.bronze.bronze_fpml_message"))


# ---- SILVER: parse, validate, resolve, version ----
def bronze_to_silver_trade(micro_batch_df, batch_id):
    parsed = (micro_batch_df
        .transform(deduplicate_by_message_id)        # idempotency
        .transform(validate_against_xsd)             # quarantine failures, do not drop
        .transform(parse_fpml_to_canonical)          # version-aware XPath mapping
        .transform(resolve_party_references)         # @href -> LEI -> party_sk
        .transform(derive_booking_entity_and_cpty)   # the perspective transformation
        .transform(derive_product_classification)
        .transform(apply_reference_data_enrichment))

    (DeltaTable.forName(spark, "gmb_lakehouse.silver.silver_trade").alias("tgt")
       .merge(parsed.alias("src"),
              "tgt.trade_id = src.trade_id AND tgt.is_current = true")
       # close the prior version
       .whenMatchedUpdate(
            condition = "src.trade_version > tgt.trade_version",
            set = {"is_current": "false",
                   "valid_to":   "src.business_effective_from",
                   "business_effective_to": "src.business_effective_from",
                   "updated_timestamp": "current_timestamp()"})
       .whenNotMatchedInsertAll()
       .execute())
    # then insert the new current version
    append_new_current_versions(parsed)


# ---- GOLD: incremental dimensional build off the Change Data Feed ----
changes = (spark.read.format("delta")
             .option("readChangeFeed", "true")
             .option("startingVersion", last_processed_version)
             .table("gmb_lakehouse.silver.silver_settlement"))

(changes.filter("_change_type IN ('insert','update_postimage')")
        .transform(join_conformed_dimensions)     # surrogate key lookup, -1 on miss
        .transform(compute_base_currency_amounts) # with fx_rate_date and source
        .transform(compute_days_late)
        .write.format("delta").mode("append")
        .saveAsTable("gmb_lakehouse.gold.gold_fact_settled_cash"))
```

---

## 9.3 Snowflake implementation

The logical design is identical; the physical mechanics differ.

```sql
-- ===== BRONZE =====
CREATE OR REPLACE TABLE GMB.BRONZE.BRONZE_FPML_MESSAGE (
    INGESTION_ID          VARCHAR(36)   NOT NULL,
    INGESTION_TIMESTAMP   TIMESTAMP_NTZ NOT NULL,
    INGESTION_DATE        DATE          NOT NULL,
    MESSAGE_ID            VARCHAR(100),
    CORRELATION_ID        VARCHAR(100),
    SEQUENCE_NUMBER       NUMBER(10,0),
    FPML_VERSION          VARCHAR(10),
    MESSAGE_TYPE          VARCHAR(100),
    SOURCE_SYSTEM         VARCHAR(50),
    RAW_PAYLOAD           VARIANT       NOT NULL COMMENT 'PARSE_XML of the FpML document',
    PAYLOAD_TEXT          VARCHAR       NOT NULL COMMENT 'Verbatim XML for evidence',
    PAYLOAD_HASH          VARCHAR(64)   NOT NULL,
    SCHEMA_VALID          BOOLEAN
)
CLUSTER BY (INGESTION_DATE, SOURCE_SYSTEM);

-- Snowflake can shred the XML natively:
SELECT
    XMLGET(RAW_PAYLOAD, 'trade'):"$"                                          AS trade_node,
    GET(XMLGET(XMLGET(RAW_PAYLOAD,'trade'),'tradeHeader'),'tradeDate')::DATE   AS trade_date
FROM GMB.BRONZE.BRONZE_FPML_MESSAGE;

-- ===== SILVER (canonical) =====
CREATE OR REPLACE TABLE GMB.SILVER.SILVER_SETTLEMENT (
    SETTLEMENT_SK          NUMBER      IDENTITY,
    SETTLEMENT_ID          VARCHAR(50) NOT NULL,
    PAYMENT_ID             VARCHAR(50) NOT NULL,
    CASHFLOW_ID            VARCHAR(50) NOT NULL,
    TRADE_ID               VARCHAR(50) NOT NULL,
    ACCOUNT_SK             NUMBER      NOT NULL,
    CURRENCY_CODE          VARCHAR(3)  NOT NULL,
    SETTLED_AMOUNT         NUMBER(28,10) NOT NULL,
    DIRECTION_SIGN         NUMBER(1,0)   NOT NULL,
    SIGNED_SETTLED_AMOUNT  NUMBER(28,10) NOT NULL,
    VALUE_DATE             DATE        NOT NULL,
    SETTLEMENT_DATE        DATE,
    SETTLEMENT_STATUS      VARCHAR(30) NOT NULL,
    FAIL_REASON_CODE       VARCHAR(50),
    DAYS_LATE              NUMBER(5,0),
    CONFIRMATION_SOURCE    VARCHAR(30) NOT NULL,
    IS_REVERSAL            BOOLEAN     DEFAULT FALSE,
    CREATED_TIMESTAMP      TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_SILVER_SETTLEMENT PRIMARY KEY (SETTLEMENT_SK)
)
CLUSTER BY (SETTLEMENT_DATE, CURRENCY_CODE);

-- ===== GOLD (semantic) =====
CREATE OR REPLACE TABLE GMB.GOLD.GOLD_FACT_SETTLED_CASH (
    SETTLED_CASH_SK        NUMBER IDENTITY,
    SETTLEMENT_ID          VARCHAR(50)  NOT NULL,
    TRADE_ID               VARCHAR(50)  NOT NULL,
    CASHFLOW_ID            VARCHAR(50)  NOT NULL,
    SETTLEMENT_DATE_SK     NUMBER(8,0),
    VALUE_DATE_SK          NUMBER(8,0)  NOT NULL,
    CURRENCY_SK            NUMBER       NOT NULL,
    LEGAL_ENTITY_SK        NUMBER       NOT NULL,
    COUNTERPARTY_SK        NUMBER       NOT NULL,
    PRODUCT_SK             NUMBER       NOT NULL,
    ACCOUNT_SK             NUMBER       NOT NULL,
    SETTLEMENT_STATUS_SK   NUMBER       NOT NULL,
    FAIL_REASON_SK         NUMBER       NOT NULL DEFAULT -1,
    EXPECTED_AMOUNT        NUMBER(28,2) NOT NULL,
    SETTLED_AMOUNT         NUMBER(28,2) NOT NULL,
    SIGNED_SETTLED_AMOUNT  NUMBER(28,2) NOT NULL,
    SETTLED_AMOUNT_BASE_CCY NUMBER(28,2),
    FX_RATE_TO_BASE        NUMBER(18,10),
    DAYS_LATE              NUMBER(5,0),
    SETTLEMENT_COUNT       NUMBER(1,0)  DEFAULT 1,
    FOREIGN KEY (COUNTERPARTY_SK) REFERENCES GMB.GOLD.GOLD_DIM_COUNTERPARTY(COUNTERPARTY_SK)
)
CLUSTER BY (SETTLEMENT_DATE_SK, CURRENCY_SK);

-- Incremental Silver->Gold with Streams and Tasks
CREATE OR REPLACE STREAM GMB.SILVER.STRM_SETTLEMENT ON TABLE GMB.SILVER.SILVER_SETTLEMENT;

CREATE OR REPLACE TASK GMB.GOLD.TSK_BUILD_SETTLED_CASH
  WAREHOUSE = WH_ETL
  SCHEDULE  = 'USING CRON 0,15,30,45 * * * * UTC'
  WHEN SYSTEM$STREAM_HAS_DATA('GMB.SILVER.STRM_SETTLEMENT')
AS
  INSERT INTO GMB.GOLD.GOLD_FACT_SETTLED_CASH (...)
  SELECT ... FROM GMB.SILVER.STRM_SETTLEMENT s
    JOIN GMB.GOLD.GOLD_DIM_CURRENCY      c  ON s.CURRENCY_CODE = c.CURRENCY_CODE
    JOIN GMB.GOLD.GOLD_DIM_COUNTERPARTY  cp ON s.COUNTERPARTY_ID = cp.COUNTERPARTY_ID
                                           AND cp.IS_CURRENT = TRUE
   WHERE s.METADATA$ACTION = 'INSERT';
```

### Databricks vs Snowflake — the honest comparison

| Dimension | Databricks + Delta | Snowflake |
|---|---|---|
| **XML/FpML parsing** | `spark-xml`, custom Python/Scala parsers, full control over recursive structures | `PARSE_XML` → `VARIANT` with `XMLGET`; convenient for shallow paths, awkward for deeply recursive FpML |
| **Streaming ingest** | Structured Streaming, native and mature | Snowpipe Streaming; good, slightly higher latency floor |
| **Change capture** | Change Data Feed | Streams |
| **Orchestration** | Workflows / DLT | Tasks (with task graphs) |
| **SCD2 mechanics** | MERGE + deletion vectors, or DLT `APPLY CHANGES INTO` | MERGE; Dynamic Tables for declarative incremental |
| **Governance** | Unity Catalog (tables, files, ML models, lineage) | Horizon / Object Tagging + Access Policies |
| **Semantic layer** | Metric Views | Semantic Views |
| **Cost model** | DBU by cluster and workload type; needs tuning discipline | Credits by warehouse; simpler to predict, easy to over-provision |
| **Best fit** | Heavy XML shredding, ML, data science, complex transformation | SQL-centric analytics, simpler operations, strong BI concurrency |

**A defensible recommendation:** for FpML specifically, **Bronze and Silver on Databricks** (XML shredding, recursive product structures, version-aware parsing, and the ML use cases like fail prediction all favour Spark), with **Gold optionally served from Snowflake** if the BI estate is already there — sharing via Delta Sharing or Iceberg tables. Do not fight the tool: FpML shredding in pure SQL is a false economy.

---
# PART 10 — END-TO-END DATA ARCHITECTURE

## 10.1 The reference architecture

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  1. SOURCE SYSTEMS                                                           ║
║  ┌────────────┐ ┌──────────┐ ┌────────────┐ ┌──────────┐ ┌────────────────┐ ║
║  │ Murex MX.3 │ │360T/FXall│ │  OSTTRA /  │ │   Risk   │ │ Nostro agents  │ ║
║  │ front off. │ │ e-venues │ │   DTCC     │ │  engine  │ │ CLS / SWIFT    │ ║
║  └─────┬──────┘ └────┬─────┘ └─────┬──────┘ └────┬─────┘ └───────┬────────┘ ║
║   FpML 5.13       FIX 4.4      FpML confirm   FpML report   ISO 20022        ║
║   confirmation                     status      valuation    camt.053/054     ║
╚════════┼──────────────┼─────────────┼──────────────┼───────────────┼─────────╝
         └──────────────┴──────┬──────┴──────────────┴───────────────┘
                               v
╔══════════════════════════════════════════════════════════════════════════════╗
║  2. INGESTION LAYER                                                          ║
║  ┌──────────────────────────────────────────────────────────────────────┐   ║
║  │  APACHE KAFKA                                                        │   ║
║  │  Topics: fx.trade.events · fx.confirmation.status · fx.valuation     │   ║
║  │          cash.payment.events · cash.settlement.confirm · refdata.*   │   ║
║  │  Partition key: trade_id (ordering per trade) · Retention: 7 days    │   ║
║  │  Compacted topics for reference data · DLQ per topic                 │   ║
║  ├──────────────────────────────────────────────────────────────────────┤   ║
║  │  SCHEMA REGISTRY (Confluent / Unity Catalog)                         │   ║
║  │  Avro/Protobuf envelope + FpML XSD version registry                  │   ║
║  │  Compatibility: BACKWARD_TRANSITIVE · producers fail-fast on breach  │   ║
║  └──────────────────────────────────────────────────────────────────────┘   ║
╚═══════════════════════════════════┬══════════════════════════════════════════╝
                                    v
╔══════════════════════════════════════════════════════════════════════════════╗
║  3. RAW / BRONZE            (Delta Lake · immutable · append-only)           ║
║     bronze_fpml_message · bronze_iso20022_message · bronze_cls_report        ║
║     bronze_refdata_snapshot                                                 ║
║     ► verbatim payload + envelope + hash + XSD validation result            ║
╚═══════════════════════════════════┬══════════════════════════════════════════╝
                                    v
╔══════════════════════════════════════════════════════════════════════════════╗
║  4. FpML PARSING & VALIDATION                                                ║
║   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌───────────────────┐  ║
║   │ XSD schema   │ │ version-aware│ │  @href       │ │ business-rule     │  ║
║   │ validation   │ │ XPath mapping│ │  resolution  │ │ validation        │  ║
║   │ (5.11-5.14)  │ │              │ │ party/account│ │ (rate=spot+points)│  ║
║   └──────────────┘ └──────────────┘ └──────────────┘ └───────────────────┘  ║
║   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌───────────────────┐  ║
║   │ dedup by     │ │ ordering by  │ │ perspective  │ │  QUARANTINE       │  ║
║   │ message_id   │ │ seq / event  │ │ derivation   │ │  (never drop)     │  ║
║   │ + hash       │ │ time         │ │ (us vs them) │ │                   │  ║
║   └──────────────┘ └──────────────┘ └──────────────┘ └───────────────────┘  ║
╚═══════════════════════════════════┬══════════════════════════════════════════╝
                                    v
╔══════════════════════════════════════════════════════════════════════════════╗
║  5. CANONICAL / SILVER           ◄──── REFERENCE & MASTER DATA ────┐        ║
║     silver_trade · silver_trade_leg · silver_trade_party_role      │        ║
║     silver_cashflow · silver_payment · silver_settlement           │        ║
║     silver_settlement_instruction · silver_trade_event             │        ║
║     silver_valuation · silver_party · silver_account · silver_product       ║
║     ► THE CANONICAL DATA MODEL · bitemporal · source-agnostic              ║
╚═══════════════════════════════════┬══════════════════════════════════════════╝
                                    v
╔══════════════════════════════════════════════════════════════════════════════╗
║  6. BUSINESS RULES ENGINE                                                    ║
║   cashflow generation · netting · direction & sign derivation                ║
║   settlement status state machine · partial/fail resolution                  ║
║   FX conversion to reporting currency · P&L attribution                      ║
║   regulatory eligibility · SCD2 versioning · nostro reconciliation           ║
╚═══════════════════════════════════┬══════════════════════════════════════════╝
                                    v
╔══════════════════════════════════════════════════════════════════════════════╗
║  7. SEMANTIC / GOLD                                                          ║
║   FACTS: fact_trade · fact_cashflow · fact_settled_cash                      ║
║          fact_valuation · fact_trade_event · fact_nostro_movement            ║
║   DIMS : dim_date · dim_counterparty · dim_legal_entity · dim_product        ║
║          dim_currency · dim_account · dim_settlement_status · dim_book       ║
║   METRIC VIEWS: mtd_settled_cash · outstanding_settlement · fail_rate        ║
╚═══┬═══════════┬═══════════┬═══════════════┬═══════════════┬════════════════════╝
    v           v           v               v               v
┌─────────┐┌─────────┐┌──────────────┐┌────────────┐┌──────────────┐
│ FINANCE ││  RISK   ││  REGULATORY  ││     BI     ││     APIs     │
│ GL, MTD ││ VaR,MTM ││ EMIR, MiFIR, ││ Power BI,  ││ REST/GraphQL │
│ LTD,P&L ││ PV01,   ││ CFTC, SFTR   ││ Tableau,   ││ Delta Sharing│
│ cash pos││ limits  ││              ││ Databricks ││ event feeds  │
└─────────┘└─────────┘└──────────────┘└────────────┘└──────────────┘

  ═══ CROSS-CUTTING (every layer) ═══════════════════════════════════════
  Unity Catalog · Data Quality · Lineage · Metadata · Governance
  Observability · Security & Entitlements · Cost management
```

---

## 10.2 Component-by-component design

### Kafka

| Design point | Decision | Rationale |
|---|---|---|
| **Partition key** | `trade_id` | Guarantees **per-trade ordering** — NEW before AMEND before CANCEL. Partitioning by anything else breaks event sequencing |
| **Topic per domain, not per source** | `fx.trade.events` carries all FX trade events regardless of source system | Consumers subscribe to business events, not to system topics. Adding a source system does not add a topic |
| **Retention** | 7 days on event topics; **compacted, infinite** on `refdata.*` | Events replay from Bronze, not Kafka. Reference data must always have its latest value available |
| **Headers** | `fpml_version`, `message_type`, `source_system`, `trace_id` | Routing and observability without deserialising the payload |
| **Dead letter queue** | One DLQ per topic with the failure reason | A message that cannot be parsed must be visible, not lost |
| **Exactly-once** | Idempotent producers + transactional writes | Combined with `message_id` dedup at Bronze, gives effective exactly-once |
| **Ordering caveat** | Kafka guarantees order **within a partition**, and only if `max.in.flight.requests=1` or idempotence is on | Say this — it is a favourite follow-up question |

### Schema Registry

Two schemas govern each message, and conflating them is a common design error:

| Schema | What it governs | Where it lives | Compatibility |
|---|---|---|---|
| **Envelope schema** (Avro/Protobuf) | The Kafka message wrapper: ids, timestamps, version, source | Confluent Schema Registry | `BACKWARD_TRANSITIVE` |
| **Payload schema** (FpML XSD) | The FpML document itself | XSD registry keyed by `fpmlVersion` + view | Governed by ISDA; we register each version we accept |

```
Producer                    Schema Registry               Consumer
   │                              │                          │
   │── register envelope v3 ─────►│                          │
   │◄── compatible? YES ──────────│                          │
   │                              │                          │
   │── publish (schema_id=3, ─────┼─────────────────────────►│
   │    fpml_version=5-13)        │   ── fetch schema 3 ────►│
   │                              │   ── fetch XSD 5-13 ────►│
```

**Rules:** compatibility is enforced at *publish* time, not discovered at consume time. New optional fields are additive and fine; a field type change or a removal is rejected. An FpML version we have not registered a parser for goes to quarantine with a specific alert, never to silent failure.

### Databricks / Spark

| Workload | Pattern |
|---|---|
| Bronze ingest | Structured Streaming from Kafka, 30-second trigger, checkpointed |
| FpML parsing | `spark-xml` for shallow structures; a purpose-built recursive parser for product nodes. Parser dispatched by `fpml_version` |
| Silver merge | `MERGE INTO` with SCD2 logic, or DLT `APPLY CHANGES INTO` for declarative CDC |
| Gold build | Incremental from the Change Data Feed, not full rebuild |
| Backfill / replay | Batch read of Bronze filtered by `ingestion_date`, same code path as streaming |
| ML | Settlement-fail prediction, anomaly detection on cash movements, SSI-mismatch detection |

**Compute separation:** ingestion (small, continuous, job clusters), transformation (autoscaling job clusters), and serving (SQL warehouses) are separate so a heavy month-end Finance query cannot delay trade ingestion.

### Delta Lake

| Feature | Use in this architecture |
|---|---|
| ACID transactions | A settlement and its cashflow status update are one atomic commit |
| Time travel | "What did the report say before the late arrival?" — no snapshot tables needed |
| Change Data Feed | Incremental Gold builds; downstream CDC to Snowflake/Kafka |
| Schema evolution | `mergeSchema` for additive changes; explicit migration for breaking ones |
| Liquid clustering | Adapts to changing query patterns without repartitioning |
| Deletion vectors | Cheap MERGE on large SCD2 tables |
| Constraints | `CHECK` on sign, positivity, date ordering — fail the write |
| `VACUUM` policy | Retain 30+ days; **never** vacuum Bronze below the regulatory retention period |

### Data Quality

Quality gates at every boundary, with the check level matched to the layer:

| Layer | Check type | Examples | On failure |
|---|---|---|---|
| **Ingest** | Structural | Well-formed XML; XSD-valid; known `fpmlVersion` | Quarantine + alert; **never drop** |
| **Bronze→Silver** | Referential | Every `@href` resolves; LEI exists in party master; currency in ISO 4217 | Quarantine; enrich with `Unknown` member if non-blocking |
| **Bronze→Silver** | Business | `spot_rate + forward_points = rate` (±tolerance); `value_date >= trade_date`; `value_date` is a business day in both currencies | Warn and flag; block only if economically impossible |
| **Silver** | Completeness | Cashflow count matches `dim_product.default_cashflow_count`; both legs present | Alert to ops |
| **Silver** | Cross-source | Trade count vs source system control totals; notional sums | Daily reconciliation report |
| **Silver→Gold** | Aggregate | `SUM(gold) = SUM(silver)` by day and currency; row-count parity | Block publish |
| **Gold** | Business assertion | Net USD-equivalent settled cash near zero for a matched FX book; no negative `settled_amount`; no orphan FKs | Alert Finance and Data Ops |

Implementation: Delta Live Tables expectations, Great Expectations, or dbt tests — the tool matters less than the principle that **checks are versioned code with owners and thresholds, not ad-hoc notebooks.**

```python
@dlt.expect_or_drop("valid_currency",     "currency_code RLIKE '^[A-Z]{3}$'")
@dlt.expect_or_fail("positive_amount",    "expected_amount >= 0")
@dlt.expect("rate_consistency",           "ABS(spot_rate + forward_points - exchange_rate) < 0.00001")
@dlt.expect("value_date_after_trade_date","value_date >= trade_date")
@dlt.expect_or_drop("settled_le_expected","settled_amount <= expected_amount * 1.0001")
def silver_trade(): ...
```

### Data Lineage

| Level | Mechanism | Question it answers |
|---|---|---|
| **Column-level** | Unity Catalog automatic lineage | "Where does `gold_fact_settled_cash.settled_amount` come from?" → `silver_settlement.settled_amount` → camt.054 |
| **Row-level** | `bronze_ingestion_id` carried on every Silver row | "Show me the exact FpML message behind this trade version" |
| **Business-level** | Mapping spec published to the catalogue | "Which FpML XPath feeds this Finance report field?" |
| **Cross-system** | OpenLineage events from every job | End-to-end from Murex to the Power BI dashboard |

**Row-level lineage is the one people skip and then regret.** Carrying `bronze_ingestion_id` through Silver costs one column and turns every production incident from a forensic exercise into a single query.

### Metadata & Data Governance

| Layer | What it holds |
|---|---|
| **Technical metadata** | Schemas, partitions, file sizes, freshness, row counts, cost |
| **Business metadata** | Glossary terms, definitions, ownership, criticality tier |
| **Operational metadata** | Job runs, SLA attainment, DQ scores, incident history |
| **Governance metadata** | Classification (PII, MNPI, confidential), retention, entitlements |

Governance operating model:

| Role | Responsibility |
|---|---|
| **Data Owner** (business, e.g. Head of FX Ops) | Accountable for the meaning and quality of trade and settlement data |
| **Data Steward** | Maintains definitions, resolves DQ issues, approves mapping changes |
| **Data Custodian** (platform) | Runs the pipelines, availability, security |
| **Data Architect** | Owns the canonical model and the change process into it |
| **Data Governance Council** | Approves canonical model changes, arbitrates definitional disputes |

**The canonical model change process is the governance artefact that matters most.** Adding an attribute is additive and lightweight. Changing the meaning of an existing attribute requires impact analysis across every consumer — and having that process written down is what separates a governed platform from a data swamp.

### Reference Data and Master Data

| Domain | Type | Source | Distribution | SCD |
|---|---|---|---|---|
| Currencies | Reference | ISO 4217 + internal conventions | Compacted Kafka topic → Delta | Type 2 |
| Holiday calendars | Reference | Vendor (Copp Clark / Refinitiv) | Daily batch | Type 2 |
| Countries, MICs | Reference | ISO 3166, ISO 10383 | Quarterly | Type 2 |
| Product taxonomy | Reference | ISDA taxonomy, ANNA DSB UPI | On ISDA release | Type 2 |
| **Legal entities / LEI** | **Master** | GLEIF + internal client master | Daily + event-driven | **Type 2 — mandatory** |
| **Counterparties** | **Master** | Client onboarding / KYC | Event-driven | **Type 2 — mandatory** |
| **Accounts / nostros** | **Master** | Treasury account master | Event-driven | Type 2 |
| **SSIs** | **Master** | DTCC ALERT / SWIFT SSI | Event-driven, **dual-control** | Type 2 |
| Books / hierarchy | Master | Risk org structure | Monthly + event | Type 2 |
| FX rates | Market data | Refinitiv / Bloomberg | Intraday + official EOD snapshot | Point-in-time |
| Curves | Market data | Risk engine | Daily snapshot | Point-in-time |

**Three rules:**
1. Reference data is a **product with an SLA**, not a lookup table someone maintains in a spreadsheet.
2. **Late-arriving dimensions are inevitable.** A trade with a brand-new counterparty will arrive before the counterparty is onboarded. Use inferred members: insert a placeholder dimension row keyed on the natural key, flag it `is_inferred = true`, and update it when the master data lands. Never drop the fact.
3. Market data used in a calculation must be **snapshotted and identified** (`market_data_snapshot_id`), or your valuations are not reproducible and will fail model validation.

### Unity Catalog

```
gmb_lakehouse (metastore)
├── bronze  (catalog)      ── platform engineering only
│   ├── bronze_fpml_message
│   ├── bronze_iso20022_message
│   └── bronze_cls_report
├── silver  (catalog)      ── data engineering + stewards
│   ├── silver_trade, silver_cashflow, silver_settlement, ...
│   └── silver_quarantine
├── gold    (catalog)      ── read for all consumers
│   ├── gold_fact_*, gold_dim_*, metric views
├── refdata (catalog)      ── shared, read-only
└── sandbox (catalog)      ── analyst scratch, no production dependency
```

| Capability | Use |
|---|---|
| Three-level namespace | Clean layer separation and per-layer entitlement |
| Fine-grained access control | Row filters by legal entity (a New York analyst sees GMB-NY rows); column masks on client identifiers |
| Automatic lineage | Table and column level, no instrumentation |
| Tags | `pii`, `mnpi`, `regulatory`, `criticality:tier1` — drive policy from tags, not from table names |
| Delta Sharing | Publish Gold to Snowflake, to a group entity, or to a regulator without copying |
| System tables | Query history, cost attribution, audit logs — the raw material for observability |
| Volumes | Governed storage for raw XML archives |

### Data Observability

| Signal | Metric | Alert threshold (illustrative) |
|---|---|---|
| **Freshness** | Minutes since last successful Gold write | > 30 min during trading hours |
| **Volume** | Trades ingested vs 20-day moving average | ±3σ |
| **Schema** | Unexpected element in the FpML payload | Any occurrence |
| **Distribution** | Currency mix, product mix, notional distribution | Significant drift |
| **Quality** | DQ pass rate by check | < 99.5% |
| **Reconciliation** | Silver vs source control totals | Any break |
| **Reconciliation** | Settled cash vs nostro statement | Any break aged > 1 day |
| **Lag** | Event time to ingestion time (p50/p95/p99) | p99 > 5 min |
| **Failure** | Quarantine queue depth | > 0 sustained 15 min |
| **Cost** | DBU per million messages | +20% week on week |

**Two dashboards, two audiences:** a technical one (pipeline health, lag, cost) for Data Ops, and a **business control dashboard** (trade counts vs source, unmatched settlements, DQ exceptions by owner) for Front Office Support and Product Control. The second is what earns the platform credibility with the business.

---

## 10.3 Non-functional design

| Concern | Target and approach |
|---|---|
| **Latency** | Trade event → Gold in < 5 minutes (streaming). Settled cash → Gold in < 15 minutes of camt.054 |
| **Throughput** | 500k trade events/day peak, 2M cashflow rows/day; sized for 5× headroom |
| **Availability** | 99.9% during trading hours; multi-AZ; Kafka replication factor 3 |
| **Recovery** | RPO ≈ 0 (Kafka + Bronze immutability); RTO < 2h via full replay from Bronze |
| **Retention** | Bronze 10 years (MiFIR/EMIR record-keeping); Silver 10 years; Gold per consumer need |
| **Security** | Encryption at rest and in transit; entitlements by legal entity and desk; no production PII in sandbox |
| **Auditability** | Every Gold number traceable to a Bronze `ingestion_id`; time travel proves what was reported when |
| **Disaster recovery** | Cross-region Delta replication; Kafka MirrorMaker; quarterly failover test |
| **Cost** | Tag every job to a cost centre; alert on 20% WoW drift; auto-terminate idle clusters |

---
# PART 11 — DATA ARCHITECT INTERVIEW PERSPECTIVE

## 11.1 How to present this in the room

### The 90-second opening

> "FpML is an excellent *messaging* standard and a poor *analytical* model, and understanding why is the whole job. It's symmetric where the business is asymmetric, hierarchical where analytics needs relational, and it stops at the point where the money is contractually due — it has nothing to say about whether the money actually moved. So my design has three models with three jobs: FpML stays at the boundary as the source contract; a canonical model in Silver absorbs source volatility and joins the FpML world to the ISO 20022 cash world; and a semantic model in Gold absorbs consumer volatility. I'll trace one FX forward — a ten million sterling against dollars — from XML to a Finance month-to-date number, and I'll show you the two places where naive designs break: the perspective transformation, and the cashflow-to-payment relationship."

### The whiteboard sequence (draw in this order, one arrow at a time)

```
1.  FpML XML  ──►  Bronze  ──►  Silver (canonical)  ──►  Gold (semantic)  ──►  Consumers

2.  Trade ──► Cashflow ──► Payment ──► Settlement ──► Settled Cash
    (contract) (expectation) (action)   (fact)       (reconciled fact)

3.  FpML gives you 1 and 2.
    Your TPP gives you 3.
    ISO 20022 / CLS gives you 4.
    The platform gives you 5.
    ── that is the argument for the canonical model, in one picture.

4.  Then the cardinalities:  Trade 1:N Cashflow  N:M Payment  1:N Settlement
    and say why netting and partials make each one necessary.
```

### Signals that mark you as senior

| Do | Don't |
|---|---|
| Name real messages: `camt.053`, `camt.054`, `MT202`, CLS session, `executionNotification` | Say "the settlement feed" |
| State the grain before the columns | Present a table and let them guess the grain |
| Say "FpML has no counterparty element — perspective is derived" | Draw a `counterparty` box as if FpML gave you one |
| Distinguish `value_date` from `settlement_date` unprompted | Use them interchangeably |
| Mention semi-additivity of MTM and positions | Sum NPV across dates |
| Give a *cost* for every design choice | Present a design with no trade-offs |
| Say what you'd do in phase 1 vs phase 3 | Present a two-year big-bang |
| Admit the limits: "FX first, because rates cashflow generation is a different problem" | Claim one model covers all asset classes on day one |

### A delivery roadmap they will ask for

| Phase | Duration | Scope | Proves |
|---|---|---|---|
| **1 — Thin slice** | 3 months | FX Spot/Forward only, one source system, Bronze→Silver→Gold, `fact_trade` + `fact_cashflow` | The pattern works end to end |
| **2 — Settled cash** | 3 months | ISO 20022 ingestion, payment/settlement model, nostro reconciliation, MTD/LTD for Finance | The hardest and highest-value piece |
| **3 — Breadth** | 6 months | FX Swap, NDF, FX Options; second source system; Risk and Regulatory consumers | The canonical model scales across products and sources |
| **4 — Cross-asset** | 6 months+ | Rates and Credit; schedule-driven cashflow generation | The supertype/subtype design was right |

**Never propose building the full canonical model before delivering anything.** The correct answer to "how long?" is "three months to a working thin slice that Finance can use, then we extend."

---

## 11.2 The questions, with architect-level answers

---

### Q1. "Why do we need a canonical model if FpML already provides a standard schema?"

**Answer.**

Because FpML and a canonical model solve different problems, and FpML is only about 60% of what an enterprise needs.

**First, FpML is a message standard, not a data model.** It describes what one party tells another at a point in time. It has no concept of current state, of history, of "as we knew it on 15 September", or of an entity's lifecycle. Every one of those is essential for Finance, Risk and audit.

**Second, FpML is symmetric; the enterprise is asymmetric.** There is no `<counterparty>` element — both sides are just `<party>`. Every internal question is asked from our point of view: our exposure, our booking entity, our payment direction. Converting a perspective-free message into a house-view record is business logic that has to live somewhere, and it must live in exactly one place or four departments will implement it four ways.

**Third, FpML stops where the money starts.** It gives you the trade, the cashflows and the settlement *instruction*. It has no payment identifier, no settlement status, no settled amount, no actual settlement date, no fail reason. Those come from ISO 20022 and CLS — a completely different standard, transport and vocabulary. **The canonical model is the only place where the trade world and the cash world are joined.** That join is the reason it exists.

**Fourth, integration economics.** Murex, the e-FX platform, the bond desk and a future acquisition all speak different dialects. Finance, Risk, Regulatory, Treasury and BI all want different shapes. Without a canonical layer that's N×M point-to-point integrations; with one it's N+M.

**Fifth, insulation.** ISDA publishes a new FpML version roughly annually — 5.14 is in Last Call Working Draft right now. Without a canonical layer, every downstream consumer is coupled to ISDA's release cycle. With it, one parser changes and nothing downstream moves.

**And finally, FpML is polymorphic by design.** `<fxSingleLeg>`, `<swap>` and `<creditDefaultSwap>` are structurally unrelated. That is right for a confirmation and wrong for a query. You cannot write "total notional by counterparty" over a substitution group.

> **The one-liner:** *"FpML tells me what my counterparty said. The canonical model tells me what my firm knows. Those aren't the same thing, and only one of them can be reported on."*

---

### Q2. "Why shouldn't downstream consumers just read FpML directly?"

**Answer.** Nine reasons, and I'd give the first four out loud:

1. **Coupling.** Every consumer becomes dependent on the ISDA release cycle and on each source system's dialect. One FpML version change becomes twenty change projects.
2. **Duplicated business logic.** Deriving payment direction from payer/receiver party references, resolving `@href`, picking the right `partyTradeIdentifier`, deriving trade status from an event stream — every consumer would reimplement all of it, differently. Finance and Risk then report different numbers and spend the quarter reconciling.
3. **It has no answers to their questions.** Finance needs settled cash. FpML has no settlement status. Reading FpML directly, Finance simply cannot answer its own question.
4. **Query performance.** Shredding XML per query is orders of magnitude slower than scanning a columnar Delta table. A month-end aggregate over a year of XML is not viable.
5. **No history.** A message is a point in time. Point-in-time reporting needs a bitemporal store.
6. **No reference data.** FpML carries an LEI, not a sector, a rating, an ultimate parent or an EMIR classification.
7. **No cross-source conformity.** Two source systems' FpML will disagree on product codes and identifier schemes.
8. **Governance.** A definition of "notional" cannot be governed if it is re-derived in twenty places.
9. **Optionality.** Almost every FpML element is optional. Consumers would each need their own null-handling and defaulting rules.

**The nuance that shows judgment:** there *are* legitimate direct consumers of FpML — the confirmation platform, the trade repository submission engine, and the counterparty. Those are *messaging* consumers operating at message grain. **Analytical consumers should never read FpML directly.** Drawing that line correctly is the point.

---

### Q3. "How do you model trade amendments?"

**Answer.** As a new *version*, never as an in-place update, using SCD Type 2 driven off an append-only event stream.

**Mechanically:**

```
FpML  <requestAmendmentConfirmation>
        <amendment>
          <originalTrade>   ...GBP 8,000,000 / USD 10,600,000
          <amendedTrade>    ...GBP 6,000,000 / USD  7,950,000

silver_trade_event   INSERT  event_type=AMEND, event_qualifier=NOTIONAL_DECREASE,
                             previous_version=1, resulting_version=2
silver_trade         v1: is_current=false, valid_to  =2026-09-02T10:14Z
                     v2: is_current=true,  valid_from=2026-09-02T10:14Z
silver_cashflow      CF-10005-01/02 → SUPERSEDED
                     CF-10005-03/04 → EXPECTED (new)
```

**Four things I'd insist on:**

1. **Both business time and system time.** The amendment was *agreed* at 10:14 on 02-Sep (business effective) but we might have *received* it at 11:30, or the next morning after a queue backlog. Finance needs "what was true on 2 September"; audit needs "what did we know on 2 September". Only bitemporality answers both.

2. **Classify the amendment.** "Amended" is not enough. Compute a structural diff between `originalTrade` and `amendedTrade` and record `event_qualifier` — `NOTIONAL_DECREASE`, `RATE_CORRECTION`, `DATE_ROLL`, `SSI_CHANGE`. Risk cares about notional changes; Ops cares about SSI changes; Regulatory needs the correct EMIR action type (`MODI` vs `CORR`).

3. **Cashflow handling depends on settlement state.** Cashflows still `EXPECTED` are superseded. Cashflows already `SETTLED` are **never** touched — a post-settlement amendment creates a *differential* cashflow for the delta, which settles separately. Restating settled cash would break the general ledger.

4. **Downstream impact is state-dependent.** If a payment has been released, you cannot simply change it: you issue a cancellation (`camt.056`) and a new payment. The model must be able to represent one cashflow with a cancelled payment and a replacement payment.

**Distinguish an amendment from a correction.** An amendment is a genuine bilateral change to the economics — a new version, effective from the amendment date. A correction (`isCorrection = true`) fixes a booking error — the previous assertion was *wrong*, so it is superseded as at its original effective date. Regulatory reporting treats them differently: `MODI` versus `CORR`.

---

### Q4. "How do you maintain trade versions?"

**Answer.** Event sourcing into an append-only event table, with SCD Type 2 as a *derived* projection.

```
silver_trade_event   (append-only, immutable — the source of truth)
        │
        │ project / fold
        v
silver_trade         (SCD2 — one row per version, is_current flag)
        │
        v
gold_fact_trade      (dimensional projection)
```

| Aspect | Design |
|---|---|
| **Version key** | `(trade_id, trade_version)`; `trade_version` monotonic per trade |
| **Current view** | `is_current = true` — one row per `trade_id` |
| **Version derivation** | `previous_version + 1` on the event, **not** trusting a source-supplied number. Sources reuse and reset version numbers |
| **Bitemporal columns** | `business_effective_from/to` (when it was economically true) and `valid_from/to` (when we knew it) |
| **Point-in-time query** | `WHERE business_effective_from <= :asof AND (business_effective_to > :asof OR NULL)` |
| **"What did we know then"** | The same predicate on `valid_from/to` |
| **Rebuild** | Truncate `silver_trade` and re-fold the event table. **If you cannot do this, you do not have event sourcing** |
| **Late/out-of-order** | Order by `(correlation_id, sequence_number)` then `event_datetime`; a lower sequence arriving after a higher one is applied to history, not to the current version |
| **Conflict** | Two events with the same version: highest `sequence_number` wins; if equal, latest `message_timestamp`; if still tied, quarantine and alert. **Never silently pick one** |

**The key insight to voice:** SCD2 is a *cache* of the fold over the event log. Keeping the event log as the system of record means every bug in your versioning logic is fixable by replay rather than by a data-fixing script. That property is worth the extra table.

---

### Q5. "How do you model multiple cashflows?"

**Answer.** Cashflow is a first-class entity at 1:N from trade, keyed by `(trade_sk, leg_number, cashflow_seq)`, never derived on the fly from trade columns.

| Product | Cashflows | Why the generic model handles it |
|---|---|---|
| FX Spot / Forward | 2 | One per exchanged currency |
| FX Forward, split value dates | 2, different `value_date` | `value_date` lives on the cashflow, not the trade |
| FX Swap | 4 | Two legs × two currencies; `leg_number` distinguishes near from far |
| NDF | **1** | Only the net difference moves — driven by `settlement_type = NON_DELIVERABLE` |
| FX Option | 1 premium; +2 on exercise | Contingent cashflows generated by the `EXERCISE` event |
| 5y quarterly IRS | ~40 | Schedule-driven, `cashflow_basis = FORECAST`, revised at each fixing |
| Cross-currency swap | ~40 + 4 principal exchanges | Same model, more rows |

**Design points that make it work:**

- **`cashflow_basis`** separates `CONTRACTUAL` (FX — fixed at trade time), `FORECAST` (projected from a curve), `RESET_FIXED` (fixed at the reset), and `ACTUAL`. A forecast cashflow changing is normal; a contractual one changing is an amendment.
- **`cashflow_type`** separates principal exchange, interest, premium, fee and compensation claim. Finance treats each differently.
- **Cashflow generation is a business-rules service, not a mapping.** For FX it reads two elements. For an IRS it walks a calculation schedule against a holiday calendar and a curve. Isolating it means adding an asset class does not change the canonical model.
- **Regeneration is versioned, not destructive.** When a trade is amended, the old cashflows become `SUPERSEDED` — they are not deleted. Cashflows that already settled are never superseded.
- **`default_cashflow_count` on `dim_product` is a DQ control.** An FX Forward that produced three cashflows is a defect, caught automatically.

---

### Q6. "How do you handle late-arriving settlement events?"

**Answer.** Late arrival is normal, not exceptional. Nostro statements arrive overnight; CLS confirmations arrive in the morning session; a fail investigation can resolve days later. The architecture has to assume it.

| Mechanism | Design |
|---|---|
| **Separate event time from processing time** | `settlement_timestamp` (when it settled) vs `ingestion_timestamp` (when we learned). Facts are dated by event time; SLA monitoring uses the difference |
| **Append, never mutate** | A late settlement is a new row in `fact_settled_cash`. Nothing existing is updated |
| **Derived cashflow status** | Cashflow status is a *view* over settlements, so it corrects itself the moment a late settlement lands. No back-fixing pass |
| **Partition on settlement date** | A settlement arriving on 5-Sep for value date 3-Sep writes to the 3-Sep partition. Delta handles the out-of-order write; only that partition is rewritten |
| **Watermark and grace** | Streaming watermark of 24h for ordering; a **7-day grace window** on Gold partitions, which are recomputed on change via CDF |
| **Restatement policy** | Beyond the grace window, do not silently restate a closed accounting period. Post to the **current** period with `original_settlement_date` retained, and flag `is_prior_period_adjustment = true`. **Finance decides the policy; the model implements it** |
| **Time travel proves it** | `SELECT ... TIMESTAMP AS OF '2026-09-30'` reproduces exactly what the month-end report said before the late arrival. This is the answer to "why has last month's number changed?" |
| **Monitoring** | Track p50/p95/p99 arrival lag by confirmation source. A rising p99 on camt.054 from one agent bank is an operational issue you can spot before Finance does |

**The concrete example:** TRD10003's residual JPY 1,000,000,000 settled on 04-Sep against a 03-Sep value date. It lands as `STL-90008` with `settlement_date = 2026-09-04` and `days_late = 1`. The 03-Sep cash position is *not* restated — the cash genuinely did not move on the 3rd. The cashflow's derived status flips from `PARTIALLY_SETTLED` to `SETTLED` automatically. **No update, no restatement, and both dates remain reportable.**

---

### Q7. "How would you ensure idempotency?"

**Answer.** Idempotency at four layers, because failure can happen at any of them.

| Layer | Key | Mechanism |
|---|---|---|
| **Kafka producer** | Producer id + sequence | `enable.idempotence=true`, transactional writes |
| **Bronze** | `(sent_by, message_id)` + `payload_hash` | `MERGE ... WHEN NOT MATCHED THEN INSERT`. Redelivery is a no-op. Two keys because two problems: an exact byte-for-byte replay (hash) and a logical resend with a different envelope (message_id) |
| **Silver** | `(trade_id, trade_version)` | `MERGE` on the business key; a lower or equal version than current is ignored |
| **Gold** | `settlement_id`, `trade_sk` | Deterministic surrogate keys; `MERGE` not blind `INSERT`; CDF-driven incremental so reprocessing is naturally convergent |

**Beyond deduplication — the properties that actually matter:**

1. **Deterministic transformations.** Given the same Bronze input, the pipeline must produce byte-identical Silver output. That means no `current_timestamp()` inside business logic, no random surrogate key generation, no dependence on processing order. Surrogate keys are either identity columns assigned once on first insert, or a deterministic hash of the business key.

2. **Checkpointing.** Structured Streaming checkpoints give exactly-once *processing* semantics into Delta; combined with MERGE, exactly-once *effects*.

3. **Replayability as a first-class capability.** A backfill from Bronze must use the *same code path* as the streaming job, and re-running it over an already-processed range must be a no-op. I'd make this a tested property, not an assumption: an automated test that runs the same batch twice and asserts identical row counts and checksums.

4. **Out-of-order tolerance.** Idempotency is not just "don't duplicate" — it is also "don't corrupt". Applying AMEND before NEW must not create a phantom trade. `sequence_number` and version checks in the MERGE predicate handle this.

```sql
MERGE INTO silver_trade tgt
USING staged src
   ON tgt.trade_id = src.trade_id AND tgt.is_current = true
 WHEN MATCHED AND src.trade_version > tgt.trade_version
      THEN UPDATE SET is_current = false, valid_to = src.business_effective_from
 WHEN NOT MATCHED
      THEN INSERT *;
-- src.trade_version <= tgt.trade_version : NO ACTION. That silence is the idempotency.
```

---

### Q8. "How would you reconcile settled cash against the source?"

**Answer.** Three reconciliations, each answering a different question. Doing only one is the common mistake.

**Reconciliation 1 — Completeness: platform vs source system**
*Question: did we receive and process everything?*
Daily control totals from Murex/TPP (trade count, cashflow count, notional by currency) versus Silver. Breaks mean ingestion loss, quarantine, or filter error. This is a **pipeline** control.

**Reconciliation 2 — Accuracy: settled cash vs nostro statement (camt.053)**
*Question: does what we think settled match what the bank says?*
Match on `(account, currency, settlement_date, amount, payment_reference / UETR)`.

| Break type | Meaning | Typical cause |
|---|---|---|
| In our book, not on statement | We recorded a settlement the bank has no record of | Status derived from an *instruction* rather than a *confirmation*. **The most dangerous kind** — it overstates cash |
| On statement, not in our book | Unexpected cash | Unmatched receipt, counterparty paid early, unbooked trade |
| Amount difference | Both agree it settled, amounts differ | Fees deducted in the chain, FX conversion, partial settlement |
| Date difference | Amounts agree, dates differ | Value-date vs booking-date convention at the agent |

**Reconciliation 3 — Attribution: gross settled cash vs net nostro movement**
*Question: can we explain a net nostro movement in terms of individual trades?*

This is the one that catches people out. Under netting, GMB's 03-Sep USD movement with ABC Bank is a **single net receipt of 6,800,000**, while our trade-attributed view shows a payment of 13,200,000 and a receipt of 20,000,000. **These will never tie line for line, and that is correct.**

The reconciliation is: `SUM(gross signed settled cash) GROUP BY netting_group_id` **must equal** the net nostro movement for that netting group. The `payment_cashflow_allocation` bridge is what makes this provable. If you modelled `Cashflow → Payment` as 1:1, you cannot perform this reconciliation at all.

**Operating model:** automated matching with currency-aware tolerance (`dim_currency.settlement_tolerance` — 0.01 for GBP, 1 for JPY); unmatched items age into buckets with an owning team; breaks over 5 business days escalate as operational risk; and root causes feed `dim_fail_reason` so the trend is visible, not just the incident.

> **The line that lands:** *"A settled-cash figure that has never been reconciled to a nostro statement is a forecast wearing a fact's clothing."*

---

### Q9. "Where would SCD Type 2 be appropriate?"

**Answer.** Type 2 wherever a historical report must reproduce the classification that was in force at the time — and *not* everywhere, because Type 2 has real costs.

| Entity | Type | Reasoning |
|---|---|---|
| **`dim_counterparty`** | **Type 2** | Credit rating, EMIR classification, ultimate parent and sector all change. A September exposure report must show the September rating. Rewriting history would misstate historical capital and limit usage |
| **`dim_legal_entity`** | **Type 2** | Reorganisations, branch changes, functional currency changes. Statutory reporting must reproduce the historical structure |
| **`dim_product`** | **Type 2** | Taxonomy and regulatory classification evolve (UPI adoption, ISDA taxonomy versions) |
| **`dim_account`** | **Type 2** | Agent bank changes, GL mapping changes. "Which GL account did this post to in September?" |
| **`silver_settlement_instruction`** | **Type 2 (mandatory)** | After an SSI-related fail or fraud, you must know which SSI version was applied on the day. Non-negotiable |
| **`silver_trade`** | **Type 2 (version-driven)** | The trade version *is* the Type 2 mechanism |
| `dim_currency` | **Type 1** mostly | ISO codes are stable. But `is_cls_eligible` and `standard_settlement_lag` should be Type 2 — the USD move to T+1 is exactly this |
| `dim_date` | **Type 0** | Immutable by nature |
| `dim_settlement_status`, `dim_event_type` | **Type 1** | Small controlled vocabularies; a description fix should apply everywhere |
| `fact_*` | **Not applicable** | Facts are insert-only; corrections are new rows |

**The decision test:** *"If this attribute changes, must a report run today for a past period show the old value or the new one?"* Old → Type 2. New → Type 1.

**The costs I'd name unprompted:**

- Storage and row multiplication on a large dimension.
- Every fact load must resolve the *effective* surrogate key at the fact's event date, not `is_current = true`. **Getting this wrong is the single most common star-schema defect**: joining a September fact to today's counterparty row silently rewrites history.
- Queries become subtler — `is_current` for "as now", the effective-dated join for "as then".
- **Type 2 is not free and not always right.** Fixing a misspelled counterparty name should be Type 1; there was never a time when the misspelling was correct.

A practical middle path: **Type 2 on the attributes that need it, Type 1 on the rest of the same row** (sometimes called Type 6), with an explicit `scd_hash` over the Type 2 columns so only genuine changes trigger a new version.

---

### Q10. "How would you handle schema evolution when the FpML version changes?"

**Answer.** With version-aware parsing, a stable canonical contract, and a rehearsal process — because the version *will* change: 5.13 is the current Recommendation, 5.14 is in Last Call Working Draft right now.

**Layer by layer:**

| Layer | Strategy |
|---|---|
| **Bronze** | Untouched. Raw XML is version-agnostic; `fpml_version` is captured as a column. A new version ingests successfully on day one, whatever we do downstream |
| **Parser** | **Version-aware dispatch:** a parser registry keyed by `(fpml_version, view)`. New version → new parser implementation, old ones retained forever for replay of historical data |
| **Canonical (Silver)** | **The contract that does not change.** New FpML elements map into existing canonical attributes where they mean the same thing; genuinely new business concepts are *additive* new nullable columns or new entities. Never a breaking change |
| **Semantic (Gold)** | Unaffected unless a consumer wants the new concept. This is the insulation paying for itself |
| **Schema Registry** | Register the new XSD; set the envelope schema to `BACKWARD_TRANSITIVE`; producers declare the version they emit |
| **Unmapped elements** | Land in a `raw_unmapped MAP<STRING,STRING>` column with an alert on first appearance. **Nothing is silently dropped** |

**The process:**

```
1. ISDA publishes 5.14 Working Draft
   └─► Impact analysis: diff 5.13 vs 5.14 schemas; classify each change as
       ADDITIVE / MODIFIED / REMOVED / RESTRUCTURED

2. Regression corpus
   └─► Replay 12 months of Bronze through the new parser.
       Assert canonical output is byte-identical for unchanged constructs.
       This is the single most valuable test you can own.

3. Parallel run
   └─► Both parsers live; route by fpml_version. Compare outputs on trades
       received in both formats during counterparty migration.

4. Canonical change (only if a genuinely new concept appears)
   └─► Additive columns; nullable; defaulted. Governance council approval.
       Consumers opt in when ready.

5. Cut over per source system, not big bang
   └─► Counterparties migrate at different times. Both versions must be
       supported simultaneously for months. Design for that from the start.
```

**Three specific hazards worth naming:**

1. **Silent semantic change.** An element that keeps its name but changes meaning or cardinality is far more dangerous than one that is removed — a removal fails loudly, a semantic change corrupts quietly. Only the regression corpus catches it.
2. **Coding scheme changes.** New `settlementMethod` or `productType` scheme values will appear. Validate against the registered scheme and *quarantine unknown values with an alert*; do not default them to `OTHER`, which hides the problem.
3. **View divergence.** The Confirmation and Reporting views may not move in lockstep. Track version per view, not per message.

**And the wider point:** the same discipline applies to source-system upgrades, to ISO 20022 CBPR+ releases, and to ISDA CDM adoption. **A canonical model whose stability depends on nobody upstream changing anything is not a canonical model.**

---

### Q11. "How do you model a novation?"

**Answer.** Not as an update. A novation is the legal replacement of one contract with another, so it produces **two trade records and three party roles**.

```
Original: GMB ←→ ABC Bank (transferor)
Novated:  GMB ←→ DEF Bank (transferee)
GMB is the remaining party.

silver_trade  TRD10001 v2   trade_status = NOVATED, business_effective_to = novation date
silver_trade  TRD10001-N1   new trade, parent_trade_sk = TRD10001,
                            counterparty_sk = DEF Bank, novation_date set
silver_trade_party_role     TRANSFEROR = ABC Bank
                            TRANSFEREE = DEF Bank
                            REMAINING  = GMB
```

Consequences the model must support: credit exposure moves entirely from ABC to DEF on the novation date; unsettled cashflows are superseded and regenerated against the new counterparty; **settled cashflows stay attributed to ABC Bank forever** — history is not rewritten; a novation fee may be payable, which is a new cashflow; and regulatory reporting requires a termination for the old and a new trade for the new. `parent_trade_sk` is what lets you follow the chain, which matters for compression and for exposure lineage.

---

### Q12. "How do you handle netting in the model without losing trade attribution?"

**Answer.** Two facts, one bridge. This is covered in §7.4.6, and the short version is:

- `gold_fact_settled_cash` holds the **gross, trade-attributed** view — every cashflow keeps its own row, with the net settlement allocated across constituent cashflows via `payment_cashflow_allocation`.
- `gold_fact_nostro_movement` holds the **actual net account movements** as they appear on the statement.
- They reconcile by `netting_group_id`: `SUM(gross signed) per group = net movement`.

Finance needs gross for trade P&L and regulatory notional; Treasury needs net for liquidity. Both are correct, and a model that only stores one cannot serve the other. If a single `Cashflow → Payment` 1:1 relationship were used, the gross view would be unrecoverable.

---

### Q13. "What is your data quality strategy, in one minute?"

**Answer.** Checks at every boundary, matched to what that boundary can actually assert, with owners and thresholds.

- **Structural at ingest:** well-formed, XSD-valid, known version. Quarantine — never drop.
- **Referential at Silver:** every `@href` resolves, LEI exists, currency is ISO. Missing dimension members become *inferred* rows, not dropped facts.
- **Business at Silver:** `spot + points = rate`, `value_date >= trade_date`, `value_date` is a good business day in both currencies, cashflow count matches the product's expected count.
- **Aggregate at Gold:** row-count and sum parity with Silver by day and currency; block publish on failure.
- **Business assertion at Gold:** net USD-equivalent settled cash should be near zero for a matched FX book — a large value is an alarm, not a result.
- **Reconciliation as the ultimate control:** Silver vs source control totals, settled cash vs nostro.

Every check is versioned code with a named owner and a threshold, surfaced on a business-facing control dashboard, not just a technical one.

---

### Q14. "How would you build this incrementally without a two-year big bang?"

**Answer.** The phased roadmap in §11.1: a thin vertical slice through all layers for FX Spot and Forward from one source in three months, then settled cash (the hardest and most valuable), then product and source breadth, then cross-asset.

The principle: **prove the pattern end-to-end on the narrowest possible scope before widening it.** A canonical model designed in the abstract for all asset classes and delivered in year two is the classic failure mode — it is unfalsifiable until it is too late to change. Design the model to be *extensible* (supertype/subtype, associative party roles, event sourcing), but *build* it for FX first.

---

### Q15. "How does ISDA CDM change this picture?"

**Answer.** CDM is a genuine evolution and I would design with it in mind without betting the programme on it.

FpML describes *state* in XML. **CDM describes state and the functions that transition state**, in a machine-executable, format-agnostic model — with defined lifecycle event primitives (`newTrade`, `quantityChange`, `partyChange`, `termsChange`) and reference implementations in Java, Scala, Python and DAML.

Where it helps: the trade *event* model and the cashflow generation logic — precisely the parts I described as business rules sitting between Silver and Gold. CDM offers a standard vocabulary for them rather than a bespoke one.

Where it does not replace anything: **FpML remains the dominant production wire format.** Counterparties, confirmation platforms and trade repositories send FpML today. And CDM says nothing more than FpML does about actual settlement — the ISO 20022 join is still yours to build.

**My position:** ingest FpML, model canonically, and align the canonical event taxonomy and lifecycle primitives to CDM concepts so that adopting CDM later is a mapping exercise rather than a rebuild. That is a low-cost hedge and the answer that shows you are current without being credulous.

---

### Q16. "What is the hardest part of this, in your experience?"

**Answer.** Not the technology. Three things, in order:

1. **Agreeing definitions.** "Notional" means something different to Risk (exposure measure), Finance (accounting base) and Regulatory (reportable field). The canonical model forces those conversations, and they are political before they are technical. The mitigation is a data governance council with real decision rights and a business Data Owner who will make the call.

2. **The cash side.** Everyone budgets for trade ingestion; almost nobody budgets for the ISO 20022 join, netting attribution and nostro reconciliation. **Settled cash is 60% of the effort and 80% of the business value**, and it is where the FpML-only designs fail.

3. **Reference data readiness.** A beautiful canonical model on top of an unmastered party hierarchy produces exposure numbers nobody trusts. LEI and counterparty mastering has to be a parallel workstream from day one, not a dependency discovered in month four.

---

## 11.3 One-page cheat sheet

```
THE CHAIN
  FpML → Bronze → Canonical (Silver) → Semantic (Gold) → Finance/Risk/Reg/BI

THE FIVE CASH CONCEPTS
  Trade (contract) → Expected Cashflow (expectation) → Payment (action)
  → Settlement (fact) → Settled Cash (reconciled fact)

THE CARDINALITIES THAT MATTER
  Trade 1:N Cashflow    N:M Payment    1:N Settlement
       (2/4/40)        (netting)      (partials, fails)

WHAT FpML GIVES YOU        WHAT IT DOESN'T
  Trade economics            Counterparty (perspective is derived)
  Product terms              Trade status (events, not states)
  Contractual cashflows      Payment id / status
  Settlement instructions    Settled amount / date / fail reason
  Party LEIs                 Reference data, GL codes, book hierarchy
  Business events            History, bitemporality, "current"

THE THREE MODELS
  Source  = fidelity     → owned by ISDA   → changes on their cycle
  Canonical = correctness → owned by EDA    → the stable contract
  Semantic  = comprehension → owned by consumers → evolves freely

THE NUMBERS (September 2026, GMB London)
  MTD net settled: GBP +10,000,000 · USD +12,250,000
                   EUR −5,000,298.61 · JPY −3,000,000,000
                   ≈ USD −10,065 net  (near zero = matched FX book ✓)
  Failed: EUR 5,000,000 (INSUFFICIENT_FUNDS, resolved T+1, claim EUR 298.61)
  Outstanding: 4 cashflows, value date 2026-12-03, ≈ USD −31,024 net

THE THREE THINGS TO SAY UNPROMPTED
  1. value_date ≠ settlement_date, and both are needed
  2. FpML has no counterparty — perspective is a canonical derivation
  3. Settled cash comes from ISO 20022, not from FpML
```

---

## Sources

- [FpML — Most Recent Versions](https://www.fpml.org/the_standard/current/) — FpML 5.14 Last Call Working Draft, 5.13 Recommendation 2, 5.12 Recommendation, Architecture Specification 3.1, and the six schema views
- [FpML 5.14 Working Draft 1](https://www.fpml.org/spec/fpml-5-14-1-wd-1/) — published 30 January 2026; view definitions
- [ISDA publishes the Recommendation for FpML version 5.13](https://www.fpml.org/latest_news/isda-publishes-the-recommendation-for-fpml-version-5-13/)
- [ISDA has republished the Recommendation for FpML version 5.13 (build 8)](https://www.fpml.org/latest_news/isda-has-republished-the-recommendation-for-fpml-version-5-13-build-8/)
- [FpML — The Standard](https://www.fpml.org/the_standard/)

*Trade data, entity names, LEIs, account identifiers and market rates in this document are illustrative and constructed for teaching purposes. FpML fragments are simplified for readability and are structurally representative rather than schema-complete — validate against the published 5.13 XSDs before use in any implementation.*
