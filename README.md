# 📡 MzansiTel Telecom Billing System (Oracle PL/SQL)

A robust telecom billing system built in Oracle PL/SQL, designed to simulate a real-world telecom provider's backend operations — from data ingestion through billing and payment, to reporting.

> ⚠️ Fraud detection is not included in this phase — it will be integrated in future iterations.

---

## 🚀 Overview

**MzansiTel** is a fictional South African telecom operator. This project replicates their data flow and business processes using PL/SQL — providing a hands-on showcase of telco operations, including:

- External file ingestion via Oracle External Tables
- Staging data validation with error handling
- Subscriber billing & usage charge computation
- Payment tracking
- Monthly invoicing
- Future-ready reporting framework

---

## 📦 Modules

### 1. **ETL Pipeline**
**Goal:** Load and clean raw telecom data from external CSV files.

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

---

### 2. **Billing Engine**
**Goal:** Automate charge calculations and invoice generation.

- **`pkg_billing`**  
  Implements:
  - Monthly charge computation based on tariff and usage
  - Generation of subscriber invoices
  - Payment recording and linking to invoices
  - Pro-rata plan fee handling

---

### 3. **Upcoming: Reporting Suite**
> 📊 *Planned for the next phase*

Will support:
- Revenue analytics
- Customer usage trends
- Outstanding payments & churn risk
- Package-level profitability insights

---

## 🗃️ Entity Relationship Diagram (ERD)

![MzansiTel ERD](./assets/MzansiTel_ERD.png)

This ERD models the production schema including subscribers, plans, invoices, payments, and usage data (CDRs).

---

## ⚙️ Technologies Used

- **Oracle 19c+**
- **PL/SQL**
- **Oracle External Tables**
- **Dynamic SQL**
- **Error logging and exception management**
- **Relational modeling best practices**

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

Built and maintained by [Your Name or GitHub Handle]  
📧 Email: you@example.com  
🌍 [LinkedIn | Portfolio | Blog] *(Optional)*
