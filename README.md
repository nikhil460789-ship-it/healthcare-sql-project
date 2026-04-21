# 🏥 Healthcare Database Project — MySQL

An end-to-end SQL project that takes raw healthcare admissions data, normalizes it into a relational database, cleans it, and runs advanced analytical queries to surface business insights.

---

## 📁 Project Structure

```
healthcare-sql-project/
│
├── healthcare_db_project.sql   # Main SQL file (schema + ETL + queries)
├── README.md                   # This file
└── data/
    └── healthcare_dataset.csv  # Raw source data (add your CSV here)
```

---

## 🗄️ Database Schema

The raw CSV is decomposed into a **star schema** with 1 fact table and 4 dimension tables:

```
patients     ──┐
doctors      ──┤
hospitals    ──┼──► admissions (fact table)
insurance    ──┘
```

| Table | Description |
|-------|-------------|
| `patients` | Unique patient demographics |
| `doctors` | Unique doctor list |
| `hospitals` | Unique hospital list |
| `insurance` | Unique insurance providers |
| `admissions` | One row per admission event (fact table) |
| `raw_data` | Staging table for raw CSV import |
| `raw_data_archive` | Archive of staging data post-ETL |
| `blood_type_audit` | Flags patients with conflicting blood type records |

---

## ⚙️ How to Run

### Prerequisites
- MySQL 8.0 or higher
- MySQL Workbench (or any MySQL client)

### Steps

1. Open MySQL Workbench and connect to your local server
2. Open `healthcare_db_project.sql`
3. Import your CSV into the `raw_data` table using:
   - MySQL Workbench → Table Data Import Wizard, **OR**
   - Run: `LOAD DATA INFILE 'path/to/healthcare_dataset.csv' INTO TABLE raw_data ...`
4. Run the script **section by section** (top to bottom):
   - Section 1: Creates all tables
   - Section 2: Cleans the raw data
   - Section 3: Checks for data quality issues
   - Section 4: Detects blood type conflicts
   - Section 5: Runs the ETL via stored procedure `CALL run_etl();`
   - Section 6: Post-ETL validation
   - Section 7: Creates performance indexes
   - Section 8–9: Run analytical queries

---

## 🔍 Analytical Queries Included

| # | Query | Technique |
|---|-------|-----------|
| 1 | Average patient stay duration | `DATEDIFF`, `AVG` |
| 2 | Top doctors by revenue | `RANK()` window function |
| 3 | Most common medical conditions | `GROUP BY`, `COUNT` |
| 4 | Monthly revenue trend | `DATE_FORMAT`, time-series |
| 5 | Month-over-month revenue growth + % | `LAG()` window function |
| 6 | Insurance provider impact on billing | `JOIN`, `AVG` |
| 7 | Top 3 hospitals by admission type | `DENSE_RANK()` partitioned |
| 8 | Hospital revenue per patient (efficiency) | Normalized `SUM/COUNT` |
| 9 | Patient outcomes by condition | Multi-column `GROUP BY` |
| 10 | Most prescribed medication per condition | `RANK()` partitioned |

---

## 💡 Key Technical Features

- **Normalization** — Flat CSV decomposed into a star schema (3NF)
- **Data Cleaning** — TRIM + LOWER standardization before ETL
- **Data Quality Checks** — Validates dates and flags blood type conflicts
- **Stored Procedure** — Full ETL wrapped in `run_etl()` for repeatability
- **CHECK Constraint** — Prevents discharge dates earlier than admission dates
- **Performance Indexes** — Added on 5 key columns for query optimization
- **Window Functions** — RANK, DENSE_RANK, LAG used across multiple queries
- **Audit Logging** — Blood type conflicts logged to `blood_type_audit` table

---

## 📊 Business Insights

- Identifies which **doctors** generate the most revenue and flags outliers
- Tracks **monthly revenue trends** and growth % for financial planning
- Reveals **hospital specialization** by admission type (Emergency vs Elective)
- Shows **insurance provider billing patterns** to support contract negotiation
- Surfaces **high-frequency conditions** for resource and medication planning
- Compares **revenue per patient** across hospitals for fair benchmarking

---

## 🛠️ Tools Used

- MySQL 8.0
- MySQL Workbench
- SQL (DDL, DML, Window Functions, Stored Procedures)

---

## 👤 Author

**Nikhil Kundu**  
https://www.linkedin.com/in/nikhil-kundu-654b30236  
nikhil460789@gmail.com

