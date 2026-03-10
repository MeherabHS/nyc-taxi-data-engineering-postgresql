# NYC Taxi Data Engineering and SQL Analytics using PostgreSQL

**Author:** Meherab Hossain Shafin  
**Department:** Software Engineering  
**Institution:** Daffodil International University  
**Project Type:** Large-Scale Data Engineering & SQL Analytics  
**Dataset Size:** 40,435,831 records (~3.9 GB)

---

# Project Overview

This project implements a **scalable data engineering pipeline** to process and analyze the **New York City Yellow Taxi dataset (2024)**.

The system ingests raw transportation data, transforms it into a relational database format, and performs optimized analytical queries using PostgreSQL.

The objective is to demonstrate three professional competencies:

| Area | Demonstration |
|-----|-----|
| Data Engineering | Large-scale dataset ingestion pipeline |
| Database Engineering | Schema design and indexing optimization |
| Data Analytics | SQL-based insight extraction from millions of records |

The final system processes **over 40 million taxi trip records** and executes analytical queries efficiently using indexed relational structures.

---

# Dataset Source

Dataset: **NYC Taxi & Limousine Commission (TLC) Trip Record Data**

Official Source  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

The dataset contains detailed trip-level taxi data including pickup time, dropoff time, passenger counts, travel distance, pickup locations, and fare information.

### Dataset Attributes

| Column | Description |
|------|------|
| tpep_pickup_datetime | Trip start timestamp |
| tpep_dropoff_datetime | Trip end timestamp |
| passenger_count | Number of passengers |
| trip_distance | Distance traveled |
| pulocationid | Pickup taxi zone |
| dolocationid | Dropoff taxi zone |
| fare_amount | Base fare |
| total_amount | Total fare including fees |

---

# Dataset Size

| Stage | Description | Size |
|-----|-----|-----|
| Raw Parquet files | Monthly TLC datasets | ~60 MB each |
| Combined dataset | All 2024 taxi trips | ~40 million rows |
| Cleaned CSV dataset | Consolidated dataset for ETL | ~3.9 GB |

Total rows loaded into PostgreSQL:

**40,435,831 taxi trips**

---

# Python Dependencies

The project requires the following Python libraries.

| Library | Purpose |
|------|------|
| pandas | Data cleaning and transformation |
| pyarrow | Reading Parquet files |
| psycopg2 | PostgreSQL database connectivity |

Install dependencies using:

```bash
pip install -r requirements.txt
# System Architecture

The project follows a structured **data engineering pipeline**.

```
Parquet Dataset
│
Python Processing Scripts
│
Clean CSV Dataset (~3.9GB)
│
PostgreSQL ETL Pipeline
│
Analytics Table
│
SQL Analytical Queries
```

Each stage performs a specific transformation that ensures efficient storage and query performance.

---

# Repository Structure

```
nyc-taxi-data-engineering-postgresql
│
├── scripts
│   ├── phase1_parquet_to_clean_csv.py
│   └── phase2_postgres_etl.py
│
├── reports
│   └── phase1_processing_summary.json
│
├── docs
│   └── taxi_data_engineering_report.pdf
│
├── sql
│   └── analytical_queries.sql
│
└── README.md
```

---

# Software Stack

| Tool | Purpose |
|----|----|
| Python | Data processing pipeline |
| Pandas | Data transformation |
| PyArrow | Parquet file reading |
| PostgreSQL | Relational data storage |
| pgAdmin | Database management |
| SQL | Analytical query execution |

---

# Data Processing Pipeline

The project consists of two main processing phases.

---

## Phase 1 — Parquet to CSV Processing

Script:

`phase1_parquet_to_clean_csv.py`

Purpose:

- Load monthly Parquet files  
- Validate schema consistency  
- Clean invalid rows  
- Normalize timestamp fields  
- Merge all months into a single dataset  
- Export a consolidated CSV dataset  

Output file:

`yellow_taxi_2024_cleaned.csv`

Size:

~3.9 GB

The processing script uses **chunk-based data handling** to prevent memory overflow when processing multi-gigabyte datasets.

---

## Phase 2 — PostgreSQL ETL Pipeline

Script:

`phase2_postgres_etl.py`

The ETL workflow performs:

1. Database connection initialization  
2. Creation of staging tables  
3. Bulk loading using PostgreSQL `COPY`  
4. Data type conversion  
5. Transfer into analytics schema  

This staged ingestion strategy improves reliability and ensures data integrity.

---

# Database Architecture

The PostgreSQL database is organized into two schema layers.

```
staging.yellow_taxi_trips_raw
│
│ transformation
▼
analytics.yellow_taxi_trips_2024
```

### Staging Layer

Purpose:

- Raw data ingestion  
- Fast bulk loading  
- Minimal transformation  

### Analytics Layer

Purpose:

- Cleaned dataset  
- Typed relational structure  
- Optimized analytical queries  

---

# Query Optimization

To improve performance on large analytical queries, several indexes were created.

### Time Index

```sql
CREATE INDEX idx_pickup_datetime
ON analytics.yellow_taxi_trips_2024 (tpep_pickup_datetime);
```

Purpose: accelerate time-range queries.

### Location Index

```sql
CREATE INDEX idx_pulocationid
ON analytics.yellow_taxi_trips_2024 (pulocationid);
```

Purpose: optimize spatial grouping queries.

### Composite Index

```sql
CREATE INDEX idx_pickup_location
ON analytics.yellow_taxi_trips_2024 (tpep_pickup_datetime, pulocationid);
```

Purpose: accelerate queries filtering by both location and time.

---

# Analytical Queries

The dataset was analyzed using SQL queries executed directly on the PostgreSQL database.

These queries reveal patterns in taxi demand, passenger behavior, and revenue distribution.

Key analytical topics include:

- Taxi demand by pickup zone  
- Monthly trip volume trends  
- Average trip distance patterns  
- Passenger count distribution  
- Revenue concentration across zones  

---

# Optimization over Raw Output

A simple revenue calculation could be performed using spreadsheets or basic scripts.  
However, this project focuses on demonstrating scalable analytical engineering practices.

### Scalability

The system processes over **40 million records efficiently** within PostgreSQL.

### Indexing

Instead of scanning the entire dataset sequentially, PostgreSQL uses **B-Tree indexes** to retrieve relevant records quickly.

### Performance Improvement

| Query Type | Before Index | After Index |
|------------|-------------|-------------|
| Time filtered query | ~1.2 seconds | ~0.05 seconds |

This demonstrates how proper indexing dramatically improves query performance.

---

# Analytical Insights

Key findings from the analysis include:

- Taxi demand is concentrated in airport and commercial zones  
- **JFK Airport (Zone 132)** is the most active pickup zone  
- Taxi demand increases during spring and autumn months  
- Single-passenger trips dominate taxi usage  
- Airport zones generate a large share of total taxi revenue  

These insights illustrate how transportation datasets can support urban mobility analysis.

---

# Engineering Competency Demonstrated

| Tier | Output | What it Demonstrates |
|----|----|----|
| Database Engineering | SQL schema and indexing | Ability to design optimized relational databases |
| Data Analytics | Temporal and spatial trends | Ability to extract insights from large datasets |
| Communication | Performance benchmarking | Ability to document technical systems clearly |

---

# Lessons Learned

This project provided several important engineering insights:

- Large datasets require careful memory-efficient ingestion  
- Indexing strategy significantly impacts query performance  
- Real-world datasets often contain data inconsistencies  
- PostgreSQL can efficiently support large analytical workloads  
- Materialized summaries improve repeated analytical queries  

---

# Conclusion

This project demonstrates a scalable data engineering workflow for processing and analyzing large transportation datasets.

By combining **Python data processing, PostgreSQL relational storage, and optimized SQL queries**, the system efficiently processes more than **40 million taxi trip records**.

The resulting architecture supports **high-performance analytical queries** and highlights the importance of **schema design, indexing strategies, and structured ETL pipelines** in real-world data engineering systems.
