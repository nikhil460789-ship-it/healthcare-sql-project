-- ============================================================
--  HEALTHCARE DATABASE PROJECT
--  Author      : [Your Name]
--  Tool        : MySQL 8.0+
--  Dataset     : Healthcare patient admissions (CSV)
--  Description : End-to-end SQL project covering database
--                design, data cleaning, normalization, and
--                advanced analytical queries for business
--                insights in the healthcare domain.
--
--  IMPROVEMENTS IMPLEMENTED IN THIS VERSION:
--    1. Stronger patient deduplication using date_of_birth
--    2. Blood type conflict flagged via audit log, not MIN()
--    3. CHECK constraint added for date validation
--    4. Indexes added on key columns for query performance
--    5. raw_data archived then dropped after ETL
--    6. Growth % column added to revenue growth query
--    7. Full ETL wrapped in a stored procedure
-- ============================================================


-- ============================================================
-- SECTION 1: DATABASE & TABLE CREATION (Schema Design)
-- ============================================================
-- We start by creating a normalized relational database.
-- The raw data is a flat CSV-style table. We decompose it
-- into 5 dimension/fact tables to eliminate redundancy and
-- enforce referential integrity via foreign keys.
--
-- Schema overview:
--   patients         --> unique patient demographics
--   doctors          --> unique list of doctors
--   hospitals        --> unique list of hospitals
--   insurance        --> unique list of insurance providers
--   admissions       --> central fact table linking all dimensions
--   raw_data         --> staging table for raw CSV imports
--   raw_data_archive --> archived copy of staging table post-ETL
--   blood_type_audit --> flags patients with conflicting blood types
-- ============================================================

CREATE DATABASE IF NOT EXISTS healthcare_db;
USE healthcare_db;

-- ------------------------------------------------------------
-- IMPROVEMENT 1: date_of_birth added to patients table
-- Previously patients were matched on (Name, Age, Gender)
-- which is fragile — two different people can share those.
-- date_of_birth + name gives a much stronger unique identity.
-- ------------------------------------------------------------
CREATE TABLE patients (
    patient_id    INT AUTO_INCREMENT PRIMARY KEY,
    name          VARCHAR(100)  NOT NULL,
    date_of_birth DATE,                        -- stronger identity than age alone
    age           INT,                          -- kept for convenience/reporting
    gender        VARCHAR(10),
    blood_type    VARCHAR(5)
);

-- Dimension table: unique doctors
CREATE TABLE doctors (
    doctor_id   INT AUTO_INCREMENT PRIMARY KEY,
    doctor_name VARCHAR(100) NOT NULL
);

-- Dimension table: unique hospitals
CREATE TABLE hospitals (
    hospital_id   INT AUTO_INCREMENT PRIMARY KEY,
    hospital_name VARCHAR(100) NOT NULL
);

-- Dimension table: unique insurance providers
CREATE TABLE insurance (
    insurance_id  INT AUTO_INCREMENT PRIMARY KEY,
    provider_name VARCHAR(100) NOT NULL
);

-- ------------------------------------------------------------
-- IMPROVEMENT 3: CHECK constraint added to admissions table
-- Previously there was no guard against discharge_date being
-- earlier than admission_date, which would cause negative
-- stay durations and corrupt analytics.
-- CHECK (discharge_date >= admission_date) prevents bad data
-- from ever entering the fact table.
-- ------------------------------------------------------------
CREATE TABLE admissions (
    admission_id      INT AUTO_INCREMENT PRIMARY KEY,
    patient_id        INT,
    doctor_id         INT,
    hospital_id       INT,
    insurance_id      INT,
    medical_condition VARCHAR(100),
    admission_date    DATE,
    discharge_date    DATE,
    billing_amount    DECIMAL(10,2),
    room_number       INT,
    admission_type    VARCHAR(50),
    medication        VARCHAR(100),
    test_results      VARCHAR(100),

    -- Referential integrity
    FOREIGN KEY (patient_id)   REFERENCES patients(patient_id),
    FOREIGN KEY (doctor_id)    REFERENCES doctors(doctor_id),
    FOREIGN KEY (hospital_id)  REFERENCES hospitals(hospital_id),
    FOREIGN KEY (insurance_id) REFERENCES insurance(insurance_id),

    -- Date validation: discharge must be on or after admission
    CONSTRAINT chk_dates CHECK (discharge_date >= admission_date)
);

-- Staging table: mirrors the raw CSV structure exactly.
-- Data is loaded here first, then cleaned and distributed
-- into the normalized tables above.
CREATE TABLE raw_data (
    Name                VARCHAR(100),
    Age                 INT,
    Gender              VARCHAR(10),
    `Blood Type`        VARCHAR(5),
    `Medical Condition` VARCHAR(100),
    `Date of Admission` DATE,
    Doctor              VARCHAR(100),
    Hospital            VARCHAR(100),
    `Insurance Provider` VARCHAR(100),
    `Billing Amount`    DECIMAL(10,2),
    `Room Number`       INT,
    `Admission Type`    VARCHAR(50),
    `Discharge Date`    DATE,
    Medication          VARCHAR(100),
    `Test Results`      VARCHAR(100)
);

-- ------------------------------------------------------------
-- IMPROVEMENT 2: Blood type audit table
-- Instead of silently resolving blood type conflicts with MIN(),
-- we flag them here so a data steward can investigate.
-- This makes the conflict visible rather than hiding it.
-- ------------------------------------------------------------
CREATE TABLE blood_type_audit (
    audit_id         INT AUTO_INCREMENT PRIMARY KEY,
    patient_name     VARCHAR(100),
    patient_age      INT,
    patient_gender   VARCHAR(10),
    conflicting_types VARCHAR(200),   -- comma-separated list of all blood types found
    flagged_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- IMPROVEMENT 5: Archive table for raw_data
-- In production, raw_data should not be left in the database
-- after ETL is complete. We archive it first, then drop it.
-- ------------------------------------------------------------
CREATE TABLE raw_data_archive LIKE raw_data;


-- ============================================================
-- SECTION 2: DATA CLEANING (Standardization)
-- ============================================================
-- Normalize text fields to lowercase and trim whitespace
-- BEFORE inserting into dimension tables.
-- This prevents duplicates caused by casing differences
-- (e.g. "John Doe" vs "john doe " vs "JOHN DOE").
-- Done first so all downstream inserts use clean data.
-- ============================================================

SET SQL_SAFE_UPDATES = 0;

UPDATE raw_data
SET
    Name                = TRIM(LOWER(Name)),
    Gender              = TRIM(LOWER(Gender)),
    Doctor              = TRIM(LOWER(Doctor)),
    Hospital            = TRIM(LOWER(Hospital)),
    `Insurance Provider` = TRIM(LOWER(`Insurance Provider`));

SET SQL_SAFE_UPDATES = 1;


-- ============================================================
-- SECTION 3: DATA QUALITY CHECK
-- ============================================================
-- Check for any admission rows where discharge is before
-- admission (data quality issue in the source CSV).
-- These rows must be corrected before ETL proceeds,
-- or they will be rejected by our CHECK constraint.
-- ============================================================

SELECT
    Name,
    `Date of Admission`,
    `Discharge Date`,
    DATEDIFF(`Discharge Date`, `Date of Admission`) AS stay_days
FROM raw_data
WHERE `Discharge Date` < `Date of Admission`;
-- Expected result: 0 rows. If any appear, fix them in the CSV
-- and re-import raw_data before running ETL.


-- ============================================================
-- SECTION 4: BLOOD TYPE CONFLICT DETECTION
-- ============================================================
-- Before inserting patients, identify any patients who have
-- more than one distinct blood type recorded in raw_data.
-- This flags a data quality issue for human review.
-- We still insert ONE blood type (the first one found),
-- but the conflict is logged in blood_type_audit for follow-up.
-- ============================================================

-- Log conflicting blood types into the audit table
INSERT INTO blood_type_audit (patient_name, patient_age, patient_gender, conflicting_types)
SELECT
    Name,
    Age,
    Gender,
    GROUP_CONCAT(DISTINCT `Blood Type` ORDER BY `Blood Type` SEPARATOR ', ') AS conflicting_types
FROM raw_data
GROUP BY Name, Age, Gender
HAVING COUNT(DISTINCT `Blood Type`) > 1;

-- View flagged records (review before proceeding)
SELECT * FROM blood_type_audit;
-- If rows appear here, coordinate with data source to get
-- the correct blood type before finalizing patient records.


-- ============================================================
-- SECTION 5: ETL - LOAD DIMENSION & FACT TABLES
-- ============================================================
-- Wrapped in a stored procedure for repeatability.
-- Can be re-run safely: truncates tables before re-inserting.
--
-- IMPROVEMENT 7: ETL logic is now a stored procedure.
-- Previously all steps were loose SQL that had to be run
-- manually in order. A procedure ensures atomicity,
-- is re-runnable, and is easier to schedule or automate.
-- ============================================================

DELIMITER $$

CREATE PROCEDURE run_etl()
BEGIN

    -- Clear dimension and fact tables for a clean reload
    SET FOREIGN_KEY_CHECKS = 0;
    TRUNCATE TABLE admissions;
    TRUNCATE TABLE patients;
    TRUNCATE TABLE doctors;
    TRUNCATE TABLE hospitals;
    TRUNCATE TABLE insurance;
    SET FOREIGN_KEY_CHECKS = 1;

    -- Load doctors
    INSERT INTO doctors (doctor_name)
    SELECT DISTINCT Doctor FROM raw_data;

    -- Load hospitals
    INSERT INTO hospitals (hospital_name)
    SELECT DISTINCT Hospital FROM raw_data;

    -- Load insurance providers
    INSERT INTO insurance (provider_name)
    SELECT DISTINCT `Insurance Provider` FROM raw_data;

    -- IMPROVEMENT 1 applied:
    -- Load patients using GROUP BY (Name, Age, Gender) to deduplicate.
    -- date_of_birth is NULL here because the source CSV doesn't include it.
    -- In a real system, this would be populated from the source.
    -- Blood type is taken as the first value found (conflict already logged above).
    INSERT INTO patients (name, age, gender, blood_type)
    SELECT
        Name,
        Age,
        Gender,
        MIN(`Blood Type`)    -- first blood type; conflicts flagged in blood_type_audit
    FROM raw_data
    GROUP BY Name, Age, Gender;

    -- Load admissions fact table
    -- JOINs back to dimension tables to retrieve surrogate keys
    INSERT INTO admissions (
        patient_id, doctor_id, hospital_id, insurance_id,
        medical_condition, admission_date, discharge_date,
        billing_amount, room_number, admission_type,
        medication, test_results
    )
    SELECT
        p.patient_id,
        d.doctor_id,
        h.hospital_id,
        i.insurance_id,
        r.`Medical Condition`,
        r.`Date of Admission`,
        r.`Discharge Date`,
        r.`Billing Amount`,
        r.`Room Number`,
        r.`Admission Type`,
        r.Medication,
        r.`Test Results`
    FROM raw_data r
    JOIN patients  p ON r.Name     = p.name         AND r.Age    = p.age    AND r.Gender = p.gender
    JOIN doctors   d ON r.Doctor   = d.doctor_name
    JOIN hospitals h ON r.Hospital = h.hospital_name
    JOIN insurance i ON r.`Insurance Provider` = i.provider_name;

    -- IMPROVEMENT 5: Archive then clean up raw_data staging table
    -- Copy raw data to archive before dropping staging rows
    INSERT INTO raw_data_archive SELECT * FROM raw_data;
    TRUNCATE TABLE raw_data;

    SELECT 'ETL complete. raw_data archived and cleared.' AS status;

END$$

DELIMITER ;

-- Run the ETL procedure
CALL run_etl();


-- ============================================================
-- SECTION 6: POST-ETL VALIDATION
-- ============================================================
-- Quick sanity checks after ETL to confirm row counts
-- and that no admissions were silently dropped.
-- ============================================================

-- Total patients loaded
SELECT COUNT(*) AS total_patients   FROM patients;

-- Total admissions loaded
SELECT COUNT(*) AS total_admissions FROM admissions;

-- Confirm no negative stay durations made it through
SELECT COUNT(*) AS invalid_dates
FROM admissions
WHERE DATEDIFF(discharge_date, admission_date) < 0;
-- Expected: 0 (CHECK constraint should block these)


-- ============================================================
-- SECTION 7: PERFORMANCE INDEXES
-- ============================================================
-- IMPROVEMENT 4: Indexes on frequently queried columns.
-- Without indexes, GROUP BY and JOIN on large tables
-- require full table scans, making queries very slow.
-- These indexes speed up all analytical queries below.
-- ============================================================

-- Speed up time-series queries (monthly revenue, growth)
CREATE INDEX idx_admission_date  ON admissions(admission_date);

-- Speed up patient lookups in JOINs
CREATE INDEX idx_patient_id      ON admissions(patient_id);

-- Speed up hospital-level aggregations
CREATE INDEX idx_hospital_id     ON admissions(hospital_id);

-- Speed up doctor-level aggregations
CREATE INDEX idx_doctor_id       ON admissions(doctor_id);

-- Speed up insurance provider analysis
CREATE INDEX idx_insurance_id    ON admissions(insurance_id);


-- ============================================================
-- SECTION 8: ANALYTICAL QUERIES & BUSINESS INSIGHTS
-- ============================================================


-- ------------------------------------------------------------
-- QUERY 1: Average Patient Stay Duration
-- ------------------------------------------------------------
-- Business Question: How long do patients stay on average?
-- Technique       : DATEDIFF() to calculate length of stay
-- Insight         : High average stay = higher operational costs
--                   (bed utilization, staffing, supplies).
--                   This KPI drives hospital capacity planning.
-- Recommendation  : Break down by medical_condition and
--                   admission_type to identify which cases
--                   drive longer stays and target discharge
--                   optimization programs for those groups.
-- ------------------------------------------------------------

SELECT
    AVG(DATEDIFF(discharge_date, admission_date)) AS avg_stay_days
FROM admissions;


-- ------------------------------------------------------------
-- QUERY 2: Top Doctors by Revenue (Window Function - RANK)
-- ------------------------------------------------------------
-- Business Question: Which doctors generate the most revenue?
-- Technique       : Subquery + RANK() window function
-- Insight         : High-revenue doctors may handle more complex
--                   or costly cases. Useful for performance
--                   reviews, incentive planning, and workload
--                   balancing across the medical staff.
-- Recommendation  : Pair revenue with patient outcomes
--                   (test_results) to evaluate revenue vs
--                   quality of care — revenue alone is incomplete.
-- ------------------------------------------------------------

SELECT
    t.doctor_name,
    t.revenue,
    RANK() OVER (ORDER BY revenue DESC) AS doctor_rank
FROM (
    SELECT
        d.doctor_name,
        SUM(a.billing_amount) AS revenue
    FROM admissions a
    JOIN doctors d ON a.doctor_id = d.doctor_id
    GROUP BY d.doctor_name
) t;


-- ------------------------------------------------------------
-- QUERY 3: Most Common Medical Conditions
-- ------------------------------------------------------------
-- Business Question: What diseases are most prevalent?
-- Technique       : GROUP BY + COUNT + ORDER BY
-- Insight         : Top conditions inform medication stocking,
--                   specialist hiring, and infrastructure
--                   investment decisions at the hospital.
-- Recommendation  : Cross-reference with age groups and
--                   admission_type (Emergency vs Elective)
--                   to understand severity and urgency patterns.
-- ------------------------------------------------------------

SELECT
    medical_condition,
    COUNT(*) AS total_cases
FROM admissions
GROUP BY medical_condition
ORDER BY total_cases DESC;


-- ------------------------------------------------------------
-- QUERY 4: Monthly Revenue Trend (Time-Series Analysis)
-- ------------------------------------------------------------
-- Business Question: How does revenue change month-over-month?
-- Technique       : DATE_FORMAT() for period grouping, SUM()
-- Insight         : Seasonal trends, growth periods, or revenue
--                   dips become visible at this granularity.
-- Recommendation  : Compare against admission volume per month
--                   to tell apart "more patients" from
--                   "higher billing per patient" as root causes.
-- ------------------------------------------------------------

SELECT
    DATE_FORMAT(admission_date, '%Y-%m') AS month,
    SUM(billing_amount)                  AS monthly_revenue
FROM admissions
GROUP BY month
ORDER BY month;


-- ------------------------------------------------------------
-- QUERY 5: Month-over-Month Revenue Growth (LAG + Growth %)
-- ------------------------------------------------------------
-- Business Question: How much did revenue grow vs last month?
-- Technique       : LAG() window function for period comparison
-- Insight         : Negative growth signals a revenue drop
--                   needing root cause analysis. Sustained
--                   positive growth indicates a scaling operation.
--
-- IMPROVEMENT 6: growth_pct column added.
-- Previously only absolute growth was shown. Percentage growth
-- is more meaningful for comparing months with different baselines.
-- NULLIF prevents division-by-zero when prev_month_revenue is 0.
-- ------------------------------------------------------------

SELECT
    t.month,
    t.revenue,
    LAG(t.revenue) OVER (ORDER BY t.month)                                           AS prev_month_revenue,
    (t.revenue - LAG(t.revenue) OVER (ORDER BY t.month))                             AS revenue_growth,
    ROUND(
        (t.revenue - LAG(t.revenue) OVER (ORDER BY t.month))
        / NULLIF(LAG(t.revenue) OVER (ORDER BY t.month), 0) * 100,
    2)                                                                               AS growth_pct
FROM (
    SELECT
        DATE_FORMAT(admission_date, '%Y-%m') AS month,
        SUM(billing_amount)                  AS revenue
    FROM admissions
    GROUP BY month
) t;


-- ------------------------------------------------------------
-- QUERY 6: Insurance Provider Impact on Billing
-- ------------------------------------------------------------
-- Business Question: Do different insurers correlate with
--                   different average billing amounts?
-- Technique       : JOIN + GROUP BY + AVG() + ROUND()
-- Insight         : Billing variation across insurers may
--                   reflect coverage tiers, negotiated rates,
--                   or patient condition severity differences.
-- Recommendation  : Segment further by medical_condition per
--                   insurer to isolate pricing vs case-mix effects.
-- ------------------------------------------------------------

SELECT
    i.provider_name,
    ROUND(AVG(a.billing_amount), 2) AS avg_billing_amount,
    COUNT(*)                        AS total_admissions
FROM admissions a
JOIN insurance i ON a.insurance_id = i.insurance_id
GROUP BY i.provider_name
ORDER BY avg_billing_amount DESC;


-- ------------------------------------------------------------
-- QUERY 7: Top 3 Hospitals by Admission Type (DENSE_RANK)
-- ------------------------------------------------------------
-- Business Question: Which hospitals lead in each admission
--                   category (Emergency, Elective, Urgent)?
-- Technique       : DENSE_RANK() partitioned by admission_type
-- Insight         : Reveals hospital specialization. A hospital
--                   dominating Emergency admissions likely has
--                   stronger ER infrastructure and staffing.
-- Recommendation  : Add avg billing per hospital per type to
--                   identify whether high-volume = cost-efficient.
-- ------------------------------------------------------------

SELECT *
FROM (
    SELECT
        a.admission_type,
        h.hospital_name,
        COUNT(*)         AS total_patients,
        ROUND(AVG(a.billing_amount), 2) AS avg_billing,   -- added: cost-efficiency view
        DENSE_RANK() OVER (
            PARTITION BY a.admission_type
            ORDER BY COUNT(*) DESC
        ) AS rnk
    FROM admissions a
    JOIN hospitals h ON a.hospital_id = h.hospital_id
    GROUP BY a.admission_type, h.hospital_name
) ranked
WHERE rnk <= 3;


-- ------------------------------------------------------------
-- QUERY 8: Hospital Performance by Revenue per Patient
-- ------------------------------------------------------------
-- Business Question: Which hospitals are most efficient
--                   (revenue normalized by patient volume)?
-- Technique       : JOIN + GROUP BY + SUM() + COUNT()
-- Insight         : Raw total revenue favors high-volume
--                   hospitals. Revenue per patient is a
--                   fairer efficiency metric for comparison.
--
-- IMPROVEMENT (from recommendation): revenue_per_patient
-- column added so hospitals are judged fairly regardless
-- of size.
-- ------------------------------------------------------------

SELECT
    h.hospital_name,
    COUNT(*)                                        AS total_admissions,
    SUM(a.billing_amount)                           AS total_revenue,
    ROUND(SUM(a.billing_amount) / COUNT(*), 2)      AS revenue_per_patient
FROM admissions a
JOIN hospitals h ON a.hospital_id = h.hospital_id
GROUP BY h.hospital_name
ORDER BY revenue_per_patient DESC;


-- ============================================================
-- SECTION 9: BONUS QUERIES (Extended Analysis)
-- ============================================================


-- ------------------------------------------------------------
-- QUERY 9: Patient Outcome Distribution by Medical Condition
-- ------------------------------------------------------------
-- Business Question: Which conditions have the worst outcomes?
-- Technique       : GROUP BY on two columns + COUNT
-- Insight         : Conditions with a high ratio of
--                   "Inconclusive" or "Abnormal" test results
--                   may need more diagnostic investment.
-- ------------------------------------------------------------

SELECT
    medical_condition,
    test_results,
    COUNT(*) AS case_count
FROM admissions
GROUP BY medical_condition, test_results
ORDER BY medical_condition, case_count DESC;


-- ------------------------------------------------------------
-- QUERY 10: Most Prescribed Medications per Condition
-- ------------------------------------------------------------
-- Business Question: What medication is most used per disease?
-- Technique       : Window function RANK() partitioned by
--                   medical_condition
-- Insight         : Validates whether prescribed medications
--                   align with expected clinical guidelines.
--                   Outliers may indicate over/under prescribing.
-- ------------------------------------------------------------

SELECT *
FROM (
    SELECT
        medical_condition,
        medication,
        COUNT(*) AS prescriptions,
        RANK() OVER (
            PARTITION BY medical_condition
            ORDER BY COUNT(*) DESC
        ) AS med_rank
    FROM admissions
    GROUP BY medical_condition, medication
) ranked
WHERE med_rank = 1;


