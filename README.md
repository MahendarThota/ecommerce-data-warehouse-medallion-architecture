 Ecommerce Data Warehouse — SQL Server Medallion Architecture

 Production-Grade End-to-End Data Engineering Pipeline built using Microsoft SQL Server following the Medallion Architecture (Source → Bronze → Silver → Gold) and delivering business-ready insights through Power BI dashboards.

 Project Overview

This project implements a modern Data Warehouse pipeline that processes raw ecommerce CSV data into clean, structured, and analytics-ready datasets.

The pipeline simulates a real-world production data engineering workflow, including:

Raw data ingestion
Incremental loading
Data cleaning
Data validation
Star schema modeling
BI reporting

The final output is delivered through interactive Power BI dashboards.

 Architecture Overview
                Raw CSV Files
                       │
                       ▼
               ┌──────────────┐
               │    SOURCE     │
               │ Raw Ingestion │
               └──────────────┘
                       │
                       ▼
               ┌──────────────┐
               │    BRONZE     │
               │ Raw + Metadata│
               └──────────────┘
                       │
                       ▼
               ┌──────────────┐
               │    SILVER     │
               │ Clean + Typed │
               └──────────────┘
                       │
                       ▼
               ┌──────────────┐
               │     GOLD      │
               │ Star Schema   │
               └──────────────┘
                       │
                       ▼
                   Power BI

This architecture ensures:

✔ Data quality
✔ Scalability
✔ Incremental processing
✔ Analytics readiness

🧰 Technology Stack
Component	Technology
Database	Microsoft SQL Server 2019+
Language	T-SQL
ETL	Stored Procedures
Architecture	Medallion Architecture
Reporting	Power BI
Data Source	CSV Files
📂 Dataset Overview

The project uses ecommerce transactional datasets.

Dataset	Description
Customers	Customer demographics
Orders	Order lifecycle
Order Items	Product-level transactions
Products	Product catalog
Payments	Payment details

Total records processed exceed 400K+ rows, enabling realistic analytics scenarios.

 Data Pipeline Flow
CSV Files
    │
    ▼
BULK INSERT
    │
    ▼
source.*
    │
    ▼
bronze.*
    │
    ▼
silver.*
    │
    ▼
gold.*
    │
    ▼
Power BI Dashboards
 Layer Design
 Source Layer

Purpose:
Ingest raw CSV files into SQL Server.

Key Features:

Uses BULK INSERT
Stores raw data
No transformation applied
Snapshot-based loading
🪵 Bronze Layer

Purpose:
Store raw data along with ingestion metadata.

Key Features:

Adds ingestion_timestamp
Preserves raw history
Supports incremental loading

Load Strategy:

NOT EXISTS logic
Prevents duplicate records
 Silver Layer

Purpose:
Clean, standardize, and validate data.

This is the core transformation layer.

Transformations
Data type conversion
NULL handling
String cleaning
Deduplication
Data validation
Data Quality Handling

Issues are flagged instead of deleting records.

Examples:

Missing values
Invalid dates
Delivery inconsistencies
⭐ Gold Layer — Star Schema

Purpose:
Deliver analytics-ready datasets.

Implements Star Schema modeling.

Dimension Tables
dim_customers
dim_products
dim_date
Fact Tables
fact_sales
fact_orders
Design Decisions

Foreign keys are not enforced to:

Improve performance
Support bulk operations
Enable scalable warehouse design
⚙️ Pipeline Execution

The pipeline is executed using modular stored procedures.

Run Incremental Pipeline
EXEC dbo.run_pipeline;
Run Snapshot Pipeline
EXEC dbo.run_pipeline @load_type='snapshot';
🧠 Control Layer

Tracks pipeline execution and incremental loads.

pipeline_watermark

Stores:

Last load time
Supports incremental loading
pipeline_log

Stores:

Pipeline status
Row counts
Duration
Error messages

Used for monitoring and debugging.

📊 Power BI Integration

The final Gold layer feeds into Power BI dashboards.

Dashboard Pages
📈 Sales Overview
Monthly revenue trend
Order counts
Payment distribution
📦 Product Performance
Top products
Category revenue
Units sold
👥 Customer Insights
Customer lifetime value
Repeat customers
Revenue by state
🚚 Delivery Quality
Late deliveries
Delivery issues
Cancellation analysis
📂 Project Folder Structure
Ecommerce-Data-Warehouse/
│
├── datasets/
│   ├── olist_customers_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_products_dataset.csv
│   └── olist_order_payments_dataset.csv
│
├── sql_scripts/
│   ├── 01_setup_schemas_control.sql
│   ├── 02_source_layer.sql
│   ├── 03_bronze_layer.sql
│   ├── 04_silver_layer.sql
│   ├── 05_gold_layer.sql
│   └── 06_main_pipeline.sql
│
├── powerbi/
│   ├── ecommerce_dashboard.pbix
│   ├── powerbi_data_model.md
│   ├── dax_measures.md
│   ├── powerbi_notes.md
│   └── dashboard_screenshots/
│
├── documentation/
│   └── pipeline_design_documentation.pdf
│
└── README.md
▶️ How to Run the Project

Follow these steps:

01_setup_schemas_control.sql
02_source_layer.sql
03_bronze_layer.sql
04_silver_layer.sql
05_gold_layer.sql
06_main_pipeline.sql

This will:

Create schemas
Create tables
Load data
Build warehouse
Execute pipeline
🧪 Monitoring Queries

Check pipeline runs:

SELECT TOP 10 *
FROM control.pipeline_log
ORDER BY log_id DESC;

Check watermark:

SELECT *
FROM control.pipeline_watermark;
⭐ Key Features

✔ Medallion Architecture
✔ Incremental Loading
✔ Snapshot Reload Support
✔ Modular Stored Procedures
✔ Data Quality Validation
✔ Star Schema Modeling
✔ Logging & Monitoring
✔ Power BI Reporting

🎯 Skills Demonstrated

This project demonstrates strong:

Data Warehouse Design
ETL Pipeline Engineering
SQL Development
Incremental Loading
Data Quality Engineering
BI Reporting
Star Schema Modeling
📌 Business Value

This pipeline transforms raw data into meaningful business insights, enabling organizations to:

Monitor sales performance
Identify top-performing products
Understand customer behavior
Improve delivery operations
📷 Dashboard Screenshots

(Add your screenshots here)

![Sales Overview](powerbi/dashboard_screenshots/sales_overview.png)

![Product Performance](powerbi/dashboard_screenshots/product_performance.png)

![Customer Insights](powerbi/dashboard_screenshots/customer_insights.png)

![Delivery Quality](powerbi/dashboard_screenshots/delivery_quality.png)
📚 Documentation

Detailed technical documentation is available in:

documentation/pipeline_design_documentation.pdf

This includes:

Architecture details
Transformation logic
Design decisions
Pipeline execution flow