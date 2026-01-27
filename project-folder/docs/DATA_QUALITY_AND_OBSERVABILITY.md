# Data Quality & Observability Strategy

> **Document Purpose**: This document describes the data quality framework and observability strategy for the Brewery Data Pipeline, fulfilling the AB InBev technical challenge requirements for data quality checks and monitoring/alerting mechanisms.

---

## Table of Contents

1. [Data Quality Framework](#1-data-quality-framework)
   - [Data Quality Dimensions](#11-data-quality-dimensions)
   - [Quality Checks by Layer](#12-quality-checks-by-layer)
   - [Quality Scoring System](#13-quality-scoring-system)
   - [Thresholds & Rationale](#14-thresholds--rationale)
2. [Observability Strategy](#2-observability-strategy)
   - [Monitoring Architecture](#21-monitoring-architecture)
   - [Key Metrics & KPIs](#22-key-metrics--kpis)
   - [Alerting Mechanisms](#23-alerting-mechanisms)
   - [Incident Response](#24-incident-response)
3. [Implementation Details](#3-implementation-details)

---

## 1. Data Quality Framework

### 1.1 Data Quality Dimensions

Our data quality framework is built around six core dimensions, each addressed by specific validation checks:

| Dimension | Definition | Why It Matters | Implementation |
|-----------|------------|----------------|----------------|
| **Completeness** | All required data is present | Missing brewery IDs or names break downstream analytics | Required field null checks, record count thresholds |
| **Accuracy** | Data correctly represents reality | Invalid coordinates lead to wrong geo-analytics | Coordinate range validation, brewery type validation |
| **Consistency** | Data follows defined standards | "US" vs "USA" vs "United States" fragments aggregations | Country/state standardization, type normalization |
| **Uniqueness** | No duplicate records | Duplicate IDs inflate counts and skew metrics | Deduplication with `keep_latest` strategy |
| **Timeliness** | Data is available when needed | Stale data affects business decisions | Daily pipeline schedule, ingestion timestamps |
| **Validity** | Data conforms to business rules | Invalid brewery types break category reports | Schema enforcement, allowed values validation |

### 1.2 Quality Checks by Layer

#### Bronze Layer (Raw Data) - `BronzeValidator`

**Philosophy**: Minimal validation - preserve raw data integrity, catch only catastrophic issues.

| Check | Dimension | Rule | Failure Action |
|-------|-----------|------|----------------|
| Directory exists | Completeness | Bronze path must exist | ❌ Fail pipeline |
| JSON files exist | Completeness | At least 1 JSON file present | ❌ Fail pipeline |
| Valid JSON format | Validity | Files must parse as JSON arrays | ❌ Fail pipeline |
| Minimum records | Completeness | `record_count >= 100` | ❌ Fail pipeline |
| No empty records | Completeness | Records contain data | ⚠️ Warning |

**Code Reference**: [src/quality/validators.py](../src/quality/validators.py#L137-L268) - `BronzeValidator`

```python
# Example Bronze validation
validator = BronzeValidator(config=config, spark=spark)
result = validator.validate("/data/bronze/breweries/", execution_date="2026-01-25")
# Returns: {layer: 'bronze', passed: True/False, errors: [], warnings: [], metrics: {...}}
```

#### Silver Layer (Cleaned Data) - `SilverValidator`

**Philosophy**: Strict validation - ensure data quality before business consumption.

| Check | Dimension | Rule | Failure Action |
|-------|-----------|------|----------------|
| No duplicate IDs | Uniqueness | `COUNT(DISTINCT id) == COUNT(*)` | ❌ Fail pipeline |
| Required fields non-null | Completeness | `id`, `name` NOT NULL | ❌ Fail pipeline |
| Valid coordinates | Accuracy | `-90 ≤ lat ≤ 90`, `-180 ≤ lon ≤ 180` | ⚠️ Warning |
| Quality score threshold | Overall | `AVG(data_quality_score) >= 80` | ⚠️ Warning |

**Code Reference**: [src/quality/validators.py](../src/quality/validators.py#L271-L386) - `SilverValidator`

**Why partition filtering matters**:
```python
# Silver stores daily snapshots - same ID in different dates is EXPECTED
# Duplicate check applies WITHIN a single partition only
validator.validate(silver_path, processing_date="2026-01-25")
```

#### Gold Layer (Aggregations) - `GoldValidator`

**Philosophy**: Business rule validation - ensure aggregations are analytically correct.

| Check | Dimension | Rule | Failure Action |
|-------|-----------|------|----------------|
| All aggregations exist | Completeness | 4 expected tables present | ❌ Fail pipeline |
| Non-empty aggregations | Completeness | Each table has `COUNT(*) > 0` | ⚠️ Warning |
| No negative counts | Validity | `record_count >= 0` | ❌ Fail pipeline |
| Valid percentages | Validity | `0 <= percentage <= 100` | ⚠️ Warning |

**Code Reference**: [src/quality/validators.py](../src/quality/validators.py#L389-L481) - `GoldValidator`

**Expected Gold aggregations**:
1. `breweries_by_type` - Market composition analysis
2. `breweries_by_location` - Geographic distribution
3. `breweries_by_type_location` - Competitive density
4. `brewery_data_quality_metrics` - Data health summary

### 1.3 Quality Scoring System

Each brewery record receives a **data quality score (0-100)** calculated in the Silver layer:

```python
# src/layers/silver_layer.py - _add_quality_flags()
quality_score = (
    (id IS NOT NULL)           * 25 +  # Identity completeness
    (name IS NOT NULL)         * 25 +  # Business completeness  
    (is_valid_location)        * 25 +  # Geographic accuracy
    (has_complete_address)     * 25    # Contact completeness
)
```

| Score Range | Quality Level | Action |
|-------------|---------------|--------|
| 90-100 | Excellent | ✅ No action needed |
| 75-89 | Good | ✅ Monitor trends |
| 50-74 | Fair | ⚠️ Investigate data source |
| 0-49 | Poor | 🚨 Alert and remediate |

**Quality flags added to Silver data**:
- `is_valid_location`: Boolean - coordinates within valid ranges
- `has_complete_address`: Boolean - city, state, country all present
- `data_quality_score`: Integer (0-100) - composite score

## 2. Observability Strategy

### 2.1 Monitoring Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        OBSERVABILITY LAYERS                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                  │
│  │   METRICS   │    │    LOGS     │    │   ALERTS    │                  │
│  │             │    │             │    │             │                  │
│  │ • Counts    │    │ • Airflow   │    │ • Slack     │                  │
│  │ • Duration  │    │ • Spark     │    │ • Email     │                  │
│  │ • Quality   │    │ • App       │    │             │                  │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                  │
│         │                  │                  │                         │
│         └──────────────────┼──────────────────┘                         │
│                            │                                            │
│                   ┌────────▼────────┐                                   │
│                   │                 │                                   │
│                   │   (metrics.py)  │                                   │
│                   └────────┬────────┘                                   │
│                            │                                            │
│         ┌──────────────────┼──────────────────┐                         │
│         │                  │                  │                         │
│  ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐                  │
│  │  Bronze     │    │   Silver    │    │    Gold     │                  │
│  │  Validator  │    │  Validator  │    │  Validator  │                  │
│  └─────────────┘    └─────────────┘    └─────────────┘                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Key Metrics & KPIs

#### Pipeline Health Metrics

| Metric | Description | Target | Alert Threshold |
|--------|-------------|--------|-----------------|
| `pipeline_duration_seconds` | End-to-end execution time | <30 min | >45 min |
| `task_success_rate` | % of tasks completing successfully | 100% | <100% |
| `dag_run_state` | Pipeline execution status | success | failed |

#### Data Volume Metrics

| Metric | Description | Expected Range | Alert Threshold |
|--------|-------------|----------------|-----------------|
| `bronze_record_count` | Raw records ingested | 8,000-10,000 | <100 or >15,000 |
| `silver_record_count` | Cleaned records | ~same as Bronze | Δ >10% from Bronze |
| `gold_aggregation_count` | Aggregated rows | 200-500 per table | <50 |

#### Data Quality Metrics

| Metric | Description | Target | Alert Threshold |
|--------|-------------|--------|-----------------|
| `avg_quality_score` | Mean quality score across records | >80 | <70 |
| `duplicate_count` | Number of duplicate IDs detected | 0 | >0 |
| `null_required_field_count` | Nulls in id/name fields | 0 | >0 |
| `invalid_coordinate_count` | Out-of-range lat/lon | <1% | >5% |

**Code Reference**: [src/monitoring/metrics.py](../src/monitoring/metrics.py) - `MetricsCollector`

```python
# Metrics are collected and published at each pipeline stage
collector = MetricsCollector()
collector.publish({
    'dag_id': 'brewery_pipeline',
    'layer': 'silver',
    'record_count': 8432,
    'avg_quality_score': 87.5,
    'duplicate_count': 0,
    'duration_seconds': 45
})
```

### 2.3 Alerting Mechanisms

#### Alert Channels

| Channel | Use Case | Severity Levels | Configuration |
|---------|----------|-----------------|---------------|
| **Slack** | Real-time team notifications | CRITICAL, HIGH | `#data-engineering` channel |
| **Email** | Formal incident records | CRITICAL, HIGH, MEDIUM | `data-engineering@company.com` |
| **PagerDuty** | On-call escalation (production) | CRITICAL | Integrated via webhook |

#### Alert Types

| Alert | Trigger Condition | Severity | Recipients |
|-------|-------------------|----------|------------|
| Pipeline Failure | Any task fails after retries | 🔴 CRITICAL | Slack + Email + PagerDuty |
| Data Quality Failure | Validation check fails | 🟠 HIGH | Slack + Email |
| Performance Degradation | Duration >45 min | 🟡 MEDIUM | Email |
| Low Quality Score | avg_quality_score <70 | 🟡 MEDIUM | Email |
| Success Notification | Pipeline completes successfully | 🟢 INFO | Slack (daily summary) |

**Code Reference**: [src/monitoring/alerts.py](../src/monitoring/alerts.py)

## 3. Implementation Details

### 3.1 Validator Architecture (SOLID Principles)

```
┌─────────────────────────────────────────────────────────────────┐
│                     ValidatorProtocol                           │
│                     (Interface - ISP)                           │
│                                                                 │
│  validate(path: str) -> Dict                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ implements
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BaseValidator (ABC)                        │
│                                                                 │
│  • Dependency injection (config, spark)                         │
│  • _create_result() - standard result structure                 │
│  • _log_result() - logging helper                               │
└─────────────────────────────────────────────────────────────────┘
                              │
           ┌──────────────────┼──────────────────┐
           │                  │                  │
           ▼                  ▼                  ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ BronzeValidator │  │ SilverValidator │  │  GoldValidator  │
│                 │  │                 │  │                 │
│ • JSON parsing  │  │ • Duplicates    │  │ • Aggregation   │
│ • Record count  │  │ • Null checks   │  │   existence     │
│ • File exists   │  │ • Coordinates   │  │ • Value ranges  │
│                 │  │ • Quality score │  │ • Percentages   │
└─────────────────┘  └─────────────────┘  └─────────────────┘
           │                  │                  │
           └──────────────────┼──────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DataQualityValidator                          │
│                   (Facade Pattern)                              │
│                                                                 │
│  • validate_bronze() → BronzeValidator.validate()               │
│  • validate_silver() → SilverValidator.validate()               │
│  • validate_gold()   → GoldValidator.validate()                 │
│  • run_all_validations() → all three                            │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Configuration Reference

**Data Quality Configuration** ([brewery_config.yaml](../dags/brewery/brewery_config.yaml)):

```yaml
data_quality:
  thresholds:
    bronze_min_records: 100
    silver_quality_score: 0.80
    gold_completeness: 0.95
    
  validations:
    bronze:
      - check: "schema_valid"
      - check: "no_empty_records"
      - check: "record_count_above_threshold"
    silver:
      - check: "no_duplicates"
      - check: "required_fields_non_null"
      - check: "valid_coordinates"
      - check: "valid_brewery_types"
    gold:
      - check: "aggregation_sum_matches_source"
      - check: "no_negative_values"
      - check: "all_dimensions_present"
```

### 3.3 Usage Examples

#### Running Validations Programmatically

```python
from src.quality.validators import DataQualityValidator

# Initialize with default config
validator = DataQualityValidator()

# Validate individual layers
bronze_result = validator.validate_bronze("/data/bronze/breweries/")
silver_result = validator.validate_silver("/data/silver/breweries/", processing_date="2026-01-25")
gold_result = validator.validate_gold(
    "/data/gold/", 
    expected_aggregations=[
        'breweries_by_type',
        'breweries_by_location',
        'breweries_by_type_location',
        'brewery_data_quality_metrics'
    ]
)

# Or validate all at once
all_results = validator.run_all_validations(
    bronze_path="/data/bronze/breweries/",
    silver_path="/data/silver/breweries/",
    gold_path="/data/gold/"
)
```
