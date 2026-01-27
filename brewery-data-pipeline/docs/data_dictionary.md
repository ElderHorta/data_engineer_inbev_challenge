# Data Dictionary

## Bronze Layer

### breweries (Raw JSON Files)

**Format**: Raw JSON files (append-only, immutable)  
**Naming**: `brewery_bronze_{source}_{execution_date}_{timestamp}.json`  
**Example**: `brewery_bronze_api_2026-01-25_20260126_193015.json`

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| id | string | Unique brewery identifier | API |
| name | string | Brewery name | API |
| brewery_type | string | Type of brewery | API |
| address_1 | string | Street address line 1 | API |
| city | string | City name | API |
| state_province | string | State or province | API |
| postal_code | string | Postal/ZIP code | API |
| country | string | Country name | API |
| longitude | string | Longitude coordinate | API |
| latitude | string | Latitude coordinate | API |
| phone | string | Phone number | API |
| website_url | string | Website URL | API |
| state | string | State name | API |
| street | string | Full street address | API |
| _ingestion_timestamp | timestamp | When record was ingested | Pipeline |
| _ingestion_date | date | Date of ingestion | Pipeline |
| _source | string | Data source identifier | Pipeline |

---

## Silver Layer

### breweries (Cleaned & Standardized)

All Bronze fields plus:

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| latitude | float | Latitude coordinate | Cast from string |
| longitude | float | Longitude coordinate | Cast from string |
| country | string | Standardized country name | Standardization |
| state | string | Standardized state name | Standardization |
| is_valid_location | boolean | Has valid coordinates | Validation flag |
| has_complete_address | boolean | Has all address fields | Validation flag |
| data_quality_score | int | Quality score 0-100 | Calculated |
| _processing_timestamp | timestamp | When record was processed | Pipeline |
| _layer | string | Layer identifier ("silver") | Pipeline |

**Partitioning:** `country`, `state`

**Deduplication:** By `id`, keeping latest by `_ingestion_timestamp`

---

## Gold Layer

**Format**: Delta Lake (incremental daily aggregations)  
**Naming**: `{aggregation_name}_gold_{execution_date}_{timestamp}`  
**Example**: `breweries_by_location_gold_2026-01-26_20260127_054115`

Where:
- `aggregation_name`: Base aggregation name (e.g., `breweries_by_type`)
- `execution_date`: Airflow logical date (YYYY-MM-DD format)
- `timestamp`: Processing timestamp (YYYYMMDD_HHMMSS format)

**Folder Behavior**:
- Each daily run creates a new timestamped folder
- Re-runs on the same day create new folders (idempotent)
- Latest folder for each execution_date contains the most recent data

### breweries_by_type

Business Metric: Market composition analysis - distribution of brewery types.

| Column | Type | Description |
|--------|------|-------------|
| brewery_type | string | Type of brewery (micro, nano, brewpub, etc.) |
| record_count | long | Number of breweries of this type |
| percentage_of_total | decimal(5,2) | Percentage of total breweries |
| _created_at | timestamp | When aggregation was created |

---

### breweries_by_location

Business Metric: Geographic distribution for market expansion planning.

| Column | Type | Description |
|--------|------|-------------|
| country | string | Country name |
| state | string | State/province name |
| record_count | long | Total breweries in this location |
| percentage_of_total | decimal(5,2) | Percentage of total breweries |
| city_count | long | Number of distinct cities with breweries |
| brewery_types | array<string> | List of brewery types present in location |
| _created_at | timestamp | When aggregation was created |

---

### breweries_by_type_location

Business Metric: Competitive density analysis - type distribution by location.

| Column | Type | Description |
|--------|------|-------------|
| country | string | Country name |
| state | string | State/province name |
| city | string | City name |
| brewery_type | string | Type of brewery |
| record_count | long | Number of breweries |
| percentage_of_total | decimal(5,2) | Percentage of total breweries |
| _created_at | timestamp | When aggregation was created |

---

### brewery_data_quality_metrics

Operational Metric: Data pipeline health monitoring.

| Column | Type | Description |
|--------|------|-------------|
| total_records | long | Total number of brewery records |
| country_count | long | Number of distinct countries |
| state_count | long | Number of distinct states |
| city_count | long | Number of distinct cities |
| brewery_type_count | long | Number of distinct brewery types |
| avg_quality_score | decimal(5,2) | Average data quality score |
| min_quality_score | int | Minimum data quality score |
| max_quality_score | int | Maximum data quality score |
| valid_location_count | long | Records with valid coordinates |
| complete_address_count | long | Records with complete addresses |
| records_with_phone | long | Records with phone number |
| records_with_website | long | Records with website URL |
| _created_at | timestamp | When metrics were calculated |

---

## Data Quality Scores

### Calculation Method

Each record receives a score from 0-100 based on 4 components (25 points each):
- ID present (not null): +25 points
- Name present (not null): +25 points
- Valid location (valid coordinates): +25 points
- Complete address (city, state, country): +25 points

**Code Reference**: `src/layers/silver_layer.py` - `_add_quality_flags()` method

### Score Ranges

| Range | Quality Level | Action |
|-------|---------------|--------|
| 80-100 | Excellent | No action needed |
| 60-79 | Good | Minor improvements possible |
| 40-59 | Fair | Review for completeness |
| 0-39 | Poor | Investigate data source |

---

## Brewery Types Reference

| Type | Description | Typical Annual Production |
|------|-------------|---------------------------|
| micro | Small craft brewery | < 15,000 barrels |
| nano | Extremely small brewery | ≤ 3 barrels per batch |
| regional | Regional brewery location | Varies |
| brewpub | Restaurant with on-premise brewery | Varies |
| large | Large commercial brewery | > 6 million barrels |
| planning | Brewery in planning | N/A |
| bar | Multi-brewery bar | N/A |
| contract | Contract brewing | Varies |
| proprietor | Mostly off-site production | < 25% on-site |
| closed | No longer operating | N/A |
