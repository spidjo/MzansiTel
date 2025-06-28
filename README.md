# 📡 MzansiTel Telecom Billing System (Oracle PL/SQL)

This repository contains a robust PL/SQL-based Telecom Billing System designed to handle the complete data pipeline from ingestion to monthly billing and payment recording. The solution simulates a South African telecom provider, **MzansiTel Communications**, and focuses on scalable, modular, and auditable ETL and billing operations.

> 🚧 *Fraud detection module is planned for a future release. Current focus: ETL + Billing + Upcoming Reporting.*

---

## 🚀 Overview

**MzansiTel** is a fictional South African telecom operator. This project replicates their data flow and business processes using PL/SQL — providing a hands-on showcase of telcom operations, including:

- External file ingestion via Oracle External Tables
- Staging data validation with error handling
- Subscriber billing & usage charge computation
- Payment tracking
- Monthly invoicing
- Future-ready reporting framework

The system is built to process and transform telecom call records, validate subscriber data, calculate charges based on dynamic tariffs, generate monthly invoices, and record payments—all within Oracle PL/SQL.

---

## 📦 Modules

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

- **External Table Creation Script**  
  Dynamically creates external tables using date-named CSVs (e.g., `cdr_data_20250628.csv`).  
  Supports:
  - `Call Detail Records (CDR)`
  - `Subscribers`
  - `Tariff Plans`
  - `Subscriber Plans`  
  → Located in: `etl/create_external_tables.sql`

- **`pkg_load_staging`**  
  Reads from the external tables and performs:
  - Data type validation
  - Referential checks
  - Error logging
  - Staging table population
 
- **`pkg_load_core`**  
  Reads from the staging tables and performs:
  - Merges validated records into production tables using optimized `MERGE` statements
  - Error logging

- **`pkg_load_core`**  
  Handles file archiving

---

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
- Possibly integration with Oracle BI or external reporting tools---

## 🗃️ Entity Relationship Diagram (ERD)

![MzansiTel ERD](./assets/MzansiTel_ERD.png)

This ERD models the production schema including subscribers, plans, invoices, payments, and usage data (CDRs).

---

## 💡 Skills & Technologies Demonstrated

- Advanced **PL/SQL** development
- External table management and bulk file loading
- Data validation and error handling
- Modular and reusable package-based architecture
- Transaction control and exception safety
- Performance optimization (indexes, batching, `MERGE`)
- File system interaction via Oracle `UTL_FILE` and `DBMS_SCHEDULER`
- Notification integration for email/SMS alerts
- Clean separation of **staging**, **core**, and **archive** logic

---

## 📁 Directory Structure

```
mzansitel-billing/
├── ddl/                    # Table, index, and constraint definitions, External tables creation
├── pkg/                    # All PL/SQL packages (staging, billing, file utils)
├── data/                   # Sample CSVs for testing
├── logs/                   # Load and error logs
├── archive/                # Compressed processed files
├── reports/                # To be added in next phase
├── README.md
└── LICENSE
```

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](./LICENSE) for details.

---

## 🙌 Contributions

While this is a solo learning and design project, suggestions are welcome via Issues or Pull Requests.

---

## 📬 Contact

Built and maintained by Siphiwo Lumkwana (Spidjo)  
📧 Email: siphiwolum@gmail.com  
🌍 LinkedIn: https://www.linkedin.com/in/siphiwo-lumkwana-1928688/
