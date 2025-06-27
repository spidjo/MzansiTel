# MzansiTel Telecom Billing ETL System

This repository contains a robust PL/SQL-based Telecom Billing System designed to handle the complete data pipeline from ingestion to monthly billing and payment recording. The solution simulates a South African telecom provider, **MzansiTel Communications**, and focuses on scalable, modular, and auditable ETL and billing operations.

> 🚧 *Fraud detection module is planned for a future release. Current focus: ETL + Billing + Upcoming Reporting.*

---

## 📦 Project Overview

The system is built to process and transform telecom call records, validate subscriber data, calculate charges based on dynamic tariffs, generate monthly invoices, and record payments—all within Oracle PL/SQL.

### 🔁 ETL Pipeline: From CSV to Production
The ETL process supports bulk loading of external data files (CSV) for:

- **Subscribers**
- **Tariff Plans**
- **Subscriber Plans**
- **Call Detail Records (CDRs)**

Each file is loaded into **staging tables** using external tables and PL/SQL utilities that:
- Validate data integrity (type checks, required fields, reference lookups)
- Log and handle errors in a centralized logging table
- Track statistics like load counts and errors
- Move and compress processed CSV files to an archive folder

The core packages:
- `pkg_csv_loader` — reads and prepares raw files
- `pkg_load_staging` — validates and loads data into staging
- `pkg_file_utils` — handles file archiving
- `pkg_load_core` — merges validated records into production tables using optimized `MERGE` statements

## 🧩 Entity Relationship Diagram (ERD)

The following ERD illustrates the core data model used in the MzansiTel Billing System, covering subscriber lifecycle, plan assignments, usage tracking, invoicing, payments, and notifications.

![MzansiTel ERD](./MzansiTel_ERD.png)

> The ERD shows key relationships:
> - One-to-many between **subscriber** and **invoices**, **notifications**, **plans**
> - Usage is tracked via **call_detail_record** tied to `msisdn`
> - Payments are linked to invoices
> - Plans are assigned to subscribers and reference a **tariff_plan**


### 💰 Billing Engine
The billing engine is responsible for:

- Calculating usage-based charges per subscriber
- Fetching appropriate tariff rates from plans
- Generating monthly invoices
- Recording payments and updating invoice statuses
- Sending notifications (email/SMS) via the `notifications_prc` utility

Key modules:
- `pkg_billing` — billing core logic (calculate, generate, compute, record)
- `tariff_plan`, `invoice`, and `payment` — production tables used in billing

### 📊 Upcoming Reporting Phase
The next milestone will introduce a **Reporting Layer** that will offer:

- Invoice summaries per billing period
- Usage trends per subscriber or plan
- Outstanding payments and debt age analysis
- Error and exception reporting across ETL

Reports will be generated using:
- Materialized views / analytical queries
- PL/SQL summary procedures
- Possibly integration with Oracle BI or external reporting tools

---

## 💡 Skills & Technologies Demonstrated

- Advanced **PL/SQL** development
- External table management and bulk file loading
- Data validation and error handling
- Modular and reusable package-based architecture
- Transaction control and exception safety
- Performance optimization (indexes, batching, `MERGE`)
- File system interaction via Oracle `UTL_FILE`
- Notification integration for email/SMS alerts
- Clean separation of **staging**, **core**, and **archive** logic

---

## 📂 Directory Structure (Coming Soon)

```bash
├── ddl/                    # Table, index, and constraint definitions
├── pkg/                    # All PL/SQL packages (CSV, staging, billing, file utils)
├── data/                   # Sample CSVs for testing
├── logs/                   # Load and error logs
├── archive/                # Processed files
├── reports/                # To be added in next phase
├── README.md
└── LICENSE
