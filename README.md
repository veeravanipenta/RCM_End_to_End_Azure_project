# Healthcare Revenue Cycle Management (RCM) Data Engineering Project

## Project Overview

This project focuses on **Healthcare Revenue Cycle Management (RCM)**, which is the financial process used by hospitals to manage the flow of revenue from patient appointments to final payments received.  

The goal of this project is to **build a robust data engineering pipeline** that consolidates data from multiple sources, cleanses it, and prepares it for reporting, KPI calculation, and analytics.  


## Business Problem

Hospitals face challenges in managing their financial workflows effectively:  

- Ensuring timely billing and claim processing  
- Tracking payments from insurance companies and patients  
- Reducing claim denials and delayed payments  
- Improving cash flow through efficient Accounts Receivable (AR) management  

## Accounts Receivable (AR) and KPIs

**Accounts Receivable** (AR) tracks payments due to the hospital.  

### Scenarios where patients pay:  
- Low insurance coverage  
- Private clinics and dental treatments  
- Deductibles  

### Key AR Metrics:  

1. **AR > 90 Days**
2. **Days in AR**

Objective: maximize cash collection and minimize collection period.


## Data Engineering Objectives

- Ingest data from multiple sources  
- Clean, enrich, and transform data  
- Build **fact tables** and **dimension tables** for reporting and analytics  
- Support KPI dashboards for AR and financial monitoring  


## Datasets

| Source | Format | Description | Location |
|--------|--------|-------------|----------|
| EMR Data | Azure SQL DB | Patients, Providers, Departments, Transactions, Encounters | SQL DB |
| Claims Data | Flat Files | Insurance claims data | Data Lake - Landing |
| NPI Data | Public API | National Provider Identifier | API |
| ICD Data | Public API | Standardized diagnosis codes | API |


## Solution Architecture

**Medallion Architecture Layers:**  

- **Landing** → **Bronze** → **Silver** → **Gold**  

**Flow:**  

- EMR (SQL DB) → Bronze → Silver → Gold  
- Claims (Flat files/landing) → Bronze → Silver → Gold  
- Codes (API) → Bronze → Silver → Gold  

**Layer Purpose:**  

- **Bronze:** Raw source data in Parquet format (source of truth)  
- **Silver:** Cleaned, enriched data, conforming to **Common Data Model (CDM)**, handling SCD2, Unity catalog, Change Data Capture 
- **Gold:** Aggregated fact & dimension tables for business users



## Project Implementation & Achievements

We successfully **implemented the end-to-end RCM data pipeline**, transforming raw EMR, claims, and code datasets into **cleaned, enriched, and structured fact and dimension tables** ready for reporting and analytics.  

### Key Achievements

- **Fact Tables:** Captured numeric, transactional data including claims submitted, payments received (insurance and patient), and outstanding balances.  
- **Dimension Tables:** Modeled descriptive attributes such as patient information, provider and department details, and insurance/claim metadata with SCD2 support.  
- **Data Processing:** Utilized Azure Data Factory for ingestion, Azure Databricks for transformation, and Delta Lake for versioned, high-quality data storage.  
- **Medallion Architecture:** Implemented landing → bronze → silver → gold layers to ensure data quality, lineage, and reusability.  
- **KPI Enablement:** Enabled accurate reporting for Accounts Receivable (AR) and Accounts Payable (AP), including monitoring payments, tracking claim statuses, and calculating AR metrics.  
- **Business Impact:** Empowered business users and analysts to run queries, generate dashboards, and make data-driven decisions without handling raw or unstructured data.  

### Outcome

- Fully operational **data warehouse for RCM** supporting multiple hospitals.  
- Reduced manual effort in data preparation for reporting.  
- Reliable and consistent metrics for financial health monitoring and decision-making.

