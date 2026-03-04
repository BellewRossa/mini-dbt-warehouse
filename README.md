# Mini Data Warehouse (DuckDB + dbt)

This project implements a small end-to-end analytics warehouse using DuckDB and dbt. Raw e-commerce data is ingested from CSV files, transformed through a staged cleaning layer, modelled into a dimensional schema, and exposed through analytics marts.

The project demonstrates a typical analytics engineering workflow and modern data transformation practices.

## Architecture

The warehouse follows a layered transformation architecture:

Raw Data (CSV)
      ↓
Bronze Layer (Raw Tables)
      ↓
Silver Layer (Staging Models)
      ↓
Gold Layer (Dimensional Model)
      ↓
Analytics Marts

Each layer progressively improves data quality and usability.

## Model Lineage (dbt DAG)

dbt automatically builds a dependency graph of all models.
The DAG below shows how raw data flows through staging models, dimensional tables, and analytics marts.

Pipeline flow:

raw tables
   ↓
stg_customers
stg_products
stg_orders
stg_order_items
   ↓
dim_customers
dim_products
fact_order_items
   ↓
mart_daily_revenue
mart_customer_ltv
mart_top_products
Technologies

DuckDB (local analytical database)

dbt-core

dbt-duckdb adapter

Python

SQL

VS Code

## Project Structure
mini-warehousev1
│
├─ data/
│ ├─ customers.csv
│ ├─ products.csv
│ ├─ orders.csv
│ └─ order_items.csv
│
├─ scripts/
│ └─ load_raw.py
│
├─ docs/
│ └─ warehousedag.png
│
└─ dbt/
└─ mini_warehouse/
└─ models/
├─ silver/
│ ├─ stg_customers.sql
│ ├─ stg_products.sql
│ ├─ stg_orders.sql
│ └─ stg_order_items.sql
│
├─ gold/
│ ├─ dim_customers.sql
│ ├─ dim_products.sql
│ └─ fact_order_items.sql
│
└─ marts/
├─ mart_daily_revenue.sql
├─ mart_customer_ltv.sql
└─ mart_top_products.sql
Data Model

The warehouse uses a star schema.

    Dimensions

    dim_customers

    Customer attributes.

Columns:

    customer_id

    first_seen_date

    country

    email

    dim_products

    Product metadata.

    Columns:

    product_id

    product_name

    category

    unit_cost

    Fact Table

    fact_order_items

Grain:

    1 row per order_id + product_id

Columns:

    order_id

    order_ts

    customer_id

    product_id

    quantity

    unit_price

    revenue

    total_cost

    gross_margin

    order_status

Metrics are derived as:

revenue = quantity * unit_price
total_cost = quantity * unit_cost
gross_margin = revenue - total_cost

Analytics Marts
    mart_daily_revenue

    Daily sales metrics.

Columns:

    date

    total_revenue

    total_orders

    aov (Average Order Value)

    rolling_7d_revenue

    AOV calculation:

    total_revenue / total_orders
    mart_customer_ltv

    Customer lifetime metrics.

Columns:

    customer_id

    first_order_date

    last_order_date

    lifetime_revenue

    order_count

    mart_top_products

    Product revenue ranking.

Columns:

    product_id

    product_name

    category

    revenue

    margin

    rank_by_revenue

Ranking uses:

    DENSE_RANK() OVER (ORDER BY revenue DESC)
    Running the Pipeline

From the project root:

    python scripts/load_raw.py

Then run dbt:

    cd dbt/mini_warehouse
    dbt run
    dbt test
Preview Analytics Outputs

Example:

dbt show --select mart_daily_revenue --limit 10

Example output:

date	total_revenue	total_orders	aov
2026-03-01	110	2	55
2026-03-02	60	1	60
2026-03-03	65	1	65
Concepts Demonstrated

This project demonstrates:

    analytics engineering workflows

    dbt model dependency management

    dimensional modelling

    SQL transformations

    window functions

    layered warehouse architecture

    data quality testing