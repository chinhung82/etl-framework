# ETL Framework

A Python-based with pyspark ETL framework for processing and validating entity data with data quality checks and reporting.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
  - [Option 1: Docker Setup (Recommended)](#option-1-docker-setup-recommended)
  - [Option 2: Local MySQL Setup](#option-2-local-mysql-setup)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Usage](#usage)
  - [Running Tests](#running-tests)
  - [Running the ETL Pipeline](#running-the-etl-pipeline)
- [Output](#output)
  - [Quality Reports](#quality-reports)
  - [Quarantine Files](#quarantine-files)
  - [Database Output](#database-output)
- [Configuration](#configuration)

## Features

- **Data Extraction**: CSV file processing with PySpark
- **Data Cleansing**: Automated cleaning of string fields, dates, emails, and standardization
- **Deduplication**: Intelligent duplicate record detection and removal
- **Validation**: Comprehensive data quality validation with rejection tracking
- **Quality Reporting**: Detailed before/after metrics and quality statistics
- **Quarantine Management**: Automatic isolation of rejected records with reasons
- **MySQL Integration**: Direct loading to MySQL database with JDBC support

## Prerequisites

Ensure you have the following installed:

- **Python 3.11.x** - Use [pyenv](https://github.com/pyenv/pyenv#installation) for version management
- **Java 18+** - Required for PySpark
- **Docker Desktop** (Optional) - For containerized MySQL setup

### Verify Your Installation

Check Python version:
```bash
python -V
```
Expected output: `Python 3.11.9`

Check Java version:
```bash
java --version
```
Expected output:
```
java 21.0.9 2025-10-21 LTS
Java(TM) SE Runtime Environment (build 21.0.9+7-LTS-338)
Java HotSpot(TM) 64-Bit Server VM (build 21.0.9+7-LTS-338, mixed mode, sharing)
```

### Configure JAVA_HOME

Set the `JAVA_HOME` environment variable:

**Windows:**
Example:
- Variable Name: `JAVA_HOME`
- Variable Value: `C:\Program Files\Java\jdk-21`

## Setup Instructions

### Step 1: Clone or download Project Files

Clone the repo from here to your desktop or preferred location from command prompt,
or you can download the project and zip and extract them.

### Step 2: Database Setup

Choose one of the following options:

#### Option 1: Docker Setup (Recommended)

Navigate to the docker directory and build the MySQL container:

```bash
cd docker
docker build -t mysql-ems .
docker run -d --name mysql-ems -p 3306:3306 mysql-ems
```

Verify MySQL is running:
```bash
docker ps
```

Test MySQL CLI access:
```bash
docker exec -it mysql-ems mysql -uroot -proot
```

Inside MySQL CLI:
```sql
USE ems;
SHOW TABLES;
DESC entities;
```

Expected output:
```
mysql> USE ems;
Database changed

mysql> SHOW TABLES;
+---------------+
| Tables_in_ems |
+---------------+
| entities      |
+---------------+
1 row in set (0.00 sec)

mysql> DESC entities;
+---------------------+--------------+------+-----+-------------------+-------------------+
| Field               | Type         | Null | Key | Default           | Extra             |
+---------------------+--------------+------+-----+-------------------+-------------------+
| entity_id           | int          | NO   | PRI | NULL              | auto_increment    |
| entity_name         | varchar(150) | NO   |     | NULL              |                   |
| entity_type         | varchar(30)  | YES  |     | NULL              |                   |
| registration_number | varchar(50)  | YES  |     | NULL              |                   |
| incorporation_date  | date         | YES  |     | NULL              |                   |
| country_code        | varchar(3)   | YES  |     | NULL              |                   |
| state_code          | varchar(50)  | YES  |     | NULL              |                   |
| status              | varchar(30)  | YES  |     | NULL              |                   |
| industry            | varchar(100) | YES  |     | NULL              |                   |
| contact_email       | varchar(100) | YES  |     | NULL              |                   |
| last_update         | date         | YES  |     | NULL              |                   |
| created_at          | timestamp    | YES  |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+---------------------+--------------+------+-----+-------------------+-------------------+
```

#### Option 2: Local MySQL Setup

If you already have MySQL Server installed and running, ensure the `entities` table is created using the DDL from `docker/mysql.sql`.

## Installation

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Project Structure

```
etl_framework/
│   main.py                                    # Main pipeline orchestrator
│   pytest.ini                                 # Pytest configuration
│   README.md                                  # This file
│   __init__.py
│
├───config/
│       config.yaml                            # Main configuration file
│
├───data/
│   ├───input/                                 # Input data directory
│   │       vistra-gep-sample-legacy-data.csv  # Sample input CSV
│   └───output/                                # Output directory
│       ├───processed/                         # Intermediate processed data
│       ├───quality_reports/                   # Data quality reports
│       └───quarantine/                        # Rejected records
│
├───docker/
│       Dockerfile                             # MySQL Docker configuration
│       mysql.sql                              # Database DDL
│
├───libs/
│       mysql-connector-j-9.5.0.jar            # MySQL JDBC connector for Spark
│
├───logs/                                      # Application logs
│
├───sql/                                       # SQL transformations
│   ├───01_cleansing/                          # Data cleaning queries
│   │       clean_email_addresses.sql
│   │       clean_string_fields.sql
│   │       parse_dates.sql
│   │       standardize_country_codes.sql
│   │       standardize_industry.sql
│   │       standardize_state_codes.sql
│   │       standardize_status.sql
│   │
│   ├───02_deduplication/                      # Deduplication logic
│   │       remove_duplicates.sql
│   │
│   ├───03_validation/                         # Validation rules
│   │       add_validation_flags.sql
│   │       calculate_rejection_stats.sql
│   │       prepare_quarantine.sql
│   │
│   └───04_transformation/                     # Schema mapping
│           map_to_target_schema.sql
│           select_final_columns.sql
│
├───src/
│   ├───extractors/
│   │       csv_extractor.py                   # CSV data extraction
│   │
│   ├───loaders/
│   │       mysql_loader.py                    # MySQL data loading
│   │
│   ├───transformers/
│   │       cleaner.py                         # Data cleaning logic
│   │       deduplicator.py                    # Deduplication logic
│   │       transformer.py                     # Schema transformation
│   │       validator.py                       # Data validation rules
│   │
│   └───utils/
│           config_loader.py                   # Configuration management
│           logger.py                          # Logging setup
│           metrics.py                         # Metrics tracking
│           quality_reporter.py                # Quality report generation
│           sql_loader.py                      # SQL file loader
│
└───tests/
        conftest.py                            # Pytest fixtures
        test_cleaner.py                        # Data cleaning tests
        test_deduplicator.py                   # Deduplication tests
        test_integration.py                    # Integration tests
        test_transformer.py                    # Transformation tests
        test_validator.py                      # Validation tests
```

## Usage

### Running Tests

Execute the test suite to verify everything is working correctly:

```bash
pytest -v
```

Expected output:
```
tests/test_cleaner.py::TestDataCleaner::test_clean_string_fields_trim_whitespace PASSED      [  2%]
tests/test_cleaner.py::TestDataCleaner::test_clean_string_fields_empty_to_null PASSED        [  5%]
tests/test_cleaner.py::TestDataCleaner::test_date_parsing_multiple_formats PASSED            [  7%]
tests/test_cleaner.py::TestDataCleaner::test_country_code_standardization PASSED             [ 10%]
tests/test_cleaner.py::TestDataCleaner::test_state_code_standardization PASSED               [ 13%]
tests/test_cleaner.py::TestDataCleaner::test_status_standardization PASSED                   [ 15%]
tests/test_cleaner.py::TestDataCleaner::test_email_validation_and_cleaning PASSED            [ 18%]
tests/test_cleaner.py::TestDataCleaner::test_industry_null_handling PASSED                   [ 21%]
...
```

### Running the ETL Pipeline

Execute the main pipeline:

```bash
python main.py
```

Expected console output:
```
2026-01-01 17:07:20,866 - ETL_Pipeline - INFO - Logging initialized. Log file: logs\etl_pipeline_20260101_170720.log
INFO:ETL_Pipeline:Logging initialized. Log file: logs\etl_pipeline_20260101_170720.log
Setting default log level to "WARN".
...

================================================================================
ETL PIPELINE SUMMARY
================================================================================
Total Records:      100
Valid Records:      96
Rejected Records:   1
Duplicate Records:  3
Success Rate:       96.00%
================================================================================
```

Detailed logs are available at: `logs/etl_pipeline_yyyyMMdd_HHmmss.log`

## Output

### Quality Reports

Generated at: `data/output/quality_reports/quality_report_yyyyMMdd_HHmmss.txt`

Example report:
```
================================================================================
ETL PIPELINE - DATA QUALITY REPORT
================================================================================
Generated: 2026-01-01 19:08:20

SUMMARY STATISTICS
--------------------------------------------------------------------------------
Total Records Processed:    100
Records Successfully Loaded: 96
Records Rejected:           1
Duplicate Records Found:    3
Success Rate:               96.00%

DATA QUALITY FIXES APPLIED
--------------------------------------------------------------------------------
Null Values Handled:        0
Date Formats Fixed:         198
Country Codes Standardized: 100
State Codes Standardized:   100
Status Standardized:        100
Industry Standardized:      100

REJECTION REASONS BREAKDOWN
--------------------------------------------------------------------------------
  Missing required fields                 : 1

BEFORE/AFTER DATA QUALITY METRICS
--------------------------------------------------------------------------------
Metric                          Before     After      Change
--------------------------------------------------------------------------------
Total Records                      100        96      -4 (-4.0%)
EntityName                         100        96      -4 (-4.0%)
IncorporationDate                   96        93      -3 (-3.1%)
CountryCode                         99        96      -3 (-3.0%)
StateCode                           49        53      +4 (+8.2%)
Status                              98        95      -3 (-3.1%)
Industry                            99        96      -3 (-3.0%)
ContactEmail                        90        87      -3 (-3.3%)

================================================================================
END OF REPORT
================================================================================
```

### Quarantine Files

Located at: `data/output/quarantine/`

Example quarantine record:
```csv
EntityID,EntityName,EntityType,RegistrationNumber,IncorporationDate,CountryCode,StateCode,Status,Industry,ContactEmail,LastUpdate,rejection_reasons
1043,TrueNorth Partners,,,,,,,,,,Missing required fields
```

### Database Output

Successfully loaded records are inserted into the MySQL `entities` table:

```
entity_id | entity_name           | entity_type | registration_number | incorporation_date | country_code | state_code | status   | industry     | contact_email                | last_update | created_at          |
----------+-----------------------+-------------+---------------------+--------------------+--------------+------------+----------+--------------+------------------------------+-------------+---------------------+
        1 | NewAge Analytics      | Company     | REG43211            | 2018-10-02         | GB           |            | Active   | Technology   | info@newageanalytics.co.uk   | 2022-05-22  | 2026-01-01 10:12:45 |
        2 | Sunset Hotels         | Company     | REG78645            | 2013-03-13         | AU           |            | Active   | Hospitality  | contact@sunsethotels.com.au  | 2022-01-18  | 2026-01-01 10:12:45 |
        3 | Evergreen Organics    | Company     | REG23455            | 2019-06-20         | IN           |            | Active   | Agriculture  | info@evergreenorganic.in     | 2022-03-31  | 2026-01-01 10:12:45 |
        4 | Liberty Bank          | Company     | REG11109            | 2011-07-29         | US           | TX         | Pending  | Finance      | support@libertybank.com      | 2022-05-10  | 2026-01-01 10:12:45 |
        5 | Rising Star Ventures  | Partnership | REG55432            | 2014-01-05         | SG           |            | Active   | Finance      | contact@risingstar.sg        | 2022-04-14  | 2026-01-01 10:12:45 |
      ... | ...                   | ...         | ...                 | ...                | ...          | ...        | ...      | ...          | ...                          | ...         | ...                 |
```

## Configuration

Main configuration is located in `config/config.yaml`. Customize the following:

- Input/output paths
- MySQL connection parameters
- Validation rules
- Data quality thresholds
- Logging levels

---

