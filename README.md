>> Setup instructions
Download the etl_framework.zip and extract to a folder in your desktop

>> Go to Pre-requisites if you already has MySQL server installed and running at your machine 
### or you can install it in docker desktop
### Docker desktop app is install in your machine
### open command prompt and CD to the project folder (eg: {your_desktop_folder}/etl_framework}, and run command below:
```cmd
cd docker
docker build -t mysql-ems .
docker run -d --name mysql-ems -p 3306:3306 mysql-ems
```

### check if mysql is running in docker
```cmd
docker ps
'''
### test MySQL CLI in docker
```cmd
docker exec -it mysql-ems mysql -uroot -proot
> USE ems;
> SHOW TABLES;
> DESC entities;
```

### MYSQL (or in docker) is running with entities table created.
### Example mysql cli output:
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
12 rows in set (0.02 sec)


>> Pre-requisites
Please make sure you have the following installed and can run them:
- Python (3.11.X), you can use for example [pyenv](https://github.com/pyenv/pyenv#installation) to manage your python versions locally
- Java (> 18), you can install and manage java locally

### Example output:
```cmd
python -V
```
Example output:
Python 3.11.9

```cmd
java --version
```
Example output:
java 21.0.9 2025-10-21 LTS
Java(TM) SE Runtime Environment (build 21.0.9+7-LTS-338)
Java HotSpot(TM) 64-Bit Server VM (build 21.0.9+7-LTS-338, mixed mode, sharing)

>> Configure JAVA_HOME environment variable
Configure JAVA_HOME to your system environment enviriable.
Example:
Variable Name: JAVA_HOME
Variable Value: C:\Program Files\Java\jdk-21

```cmd
java --version
```
java 21.0.9 2025-10-21 LTS
Java(TM) SE Runtime Environment (build 21.0.9+7-LTS-338)
Java HotSpot(TM) 64-Bit Server VM (build 21.0.9+7-LTS-338, mixed mode, sharing)


>> Install dependencies
```cmd
pip install -r requirements.txt
```

### Project folder structure
etl_framework/
│   main.py										# Main pipeline
│   pytest.ini									# Pytest initialization file
│   README.md									# The current file
│   __init__.py
│
├───config/
│       config.yaml								# Main configuration file
│
├───data/
│   ├───input/									# Input directory
│   │       vistra-gep-sample-legacy-data.csv	# Input CSV file
│   └───output/									# Output directory
│       ├───processed/							# Intermediate data
│       ├───quality_reports/					# Quality reports
│       └───quarantine/							# Rejected records
│
├───docker/
│       Dockerfile								# Docker file to run mysql and create entities table
│       mysql.sql								# Mysql table DDL
│
├───libs/
│       mysql-connector-j-9.5.0.jar				# mysql connector for spark
│
├───logs/										# Log files
│
├───sql/
│   ├───01_cleansing/							# Data cleaning SQL
│   │       clean_email_addresses.sql		
│   │       clean_string_fields.sql
│   │       parse_dates.sql
│   │       standardize_country_codes.sql
│   │       standardize_industry.sql
│   │       standardize_state_codes.sql
│   │       standardize_status.sql
│   │
│   ├───02_deduplication/						# Deduplication SQL
│   │       remove_duplicates.sql	
│   │
│   ├───03_validation/							# Validation SQL
│   │       add_validation_flags.sql
│   │       calculate_rejection_stats.sql
│   │       prepare_quarantine.sql
│   │
│   └───04_transformation/						# Schema mapping SQL
│           map_to_target_schema.sql
│           select_final_columns.sql
│
├───src/
│   │
│   ├───extractors/
│   │       csv_extractor.py					# CSV data extraction
│   │
│   ├───loaders/
│   │       mysql_loader.py						# MySQL loading
│   │
│   ├───transformers/
│   │       cleaner.py							# Data cleaning
│   │       deduplicator.py						# Deduplication
│   │       transformer.py						# Schema transformation
│   │       validator.py						# Data validation
│   │
│   └───utils/
│           config_loader.py					# Config management
│           logger.py							# Logging setup
│           metrics.py							# Metrics tracking
│           quality_reporter.py					# Report generation
│           sql_loader.py						# SQL file loader
│
├───tests/
│       conftest.py				# Fixtures
│       test_cleaner.py			# Cleaning tests
│       test_deduplicator.py	# Dedup tests
│       test_integration.py		# Integration tests
│       test_transformer.py		# Transformation tests
│       test_validator.py		# validation tests



>> Run tests
```cmd
pytest -v
```
### Example pytest output:
tests/test_cleaner.py::TestDataCleaner::test_clean_string_fields_trim_whitespace PASSED             [  2%]
tests/test_cleaner.py::TestDataCleaner::test_clean_string_fields_empty_to_null PASSED               [  5%]
tests/test_cleaner.py::TestDataCleaner::test_date_parsing_multiple_formats PASSED                   [  7%]
tests/test_cleaner.py::TestDataCleaner::test_country_code_standardization PASSED                    [ 10%]
tests/test_cleaner.py::TestDataCleaner::test_state_code_standardization PASSED                      [ 13%]
tests/test_cleaner.py::TestDataCleaner::test_status_standardization PASSED                          [ 15%]
tests/test_cleaner.py::TestDataCleaner::test_email_validation_and_cleaning PASSED                   [ 18%]
tests/test_cleaner.py::TestDataCleaner::test_industry_null_handling PASSED                          [ 21%]
...


>> Run the job
```cmd
python main.py
```
### Example logging output:
2026-01-01 17:07:20,866 - ETL_Pipeline - INFO - Logging initialized. Log file: logs\etl_pipeline_20260101_170720.log
INFO:ETL_Pipeline:Logging initialized. Log file: logs\etl_pipeline_20260101_170720.log
Setting default log level to "WARN".
...
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

### For job run details, can refer to latest log at: logs/etl_pipeline_yyyyMMdd_HHmmss.log


>> Quarantine file located at: data/output/quarantine/
Example Quarantine file:
EntityID,EntityName,EntityType,RegistrationNumber,IncorporationDate,CountryCode,StateCode,Status,Industry,ContactEmail,LastUpdate,rejection_reasons
1043,TrueNorth Partners,,,,,,,,,,Missing required fields


>> Quality Report is generated at: data/output/quality_reports/quality_report__yyyyMMdd_HHmmss.txt
Example Quality Report:
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
Metric                                  Before           After          Change
--------------------------------------------------------------------------------
Total Records                              100              96         -4 (-4.0%)
EntityName                                 100              96         -4 (-4.0%)
IncorporationDate                           96              93         -3 (-3.1%)
CountryCode                                 99              96         -3 (-3.0%)
StateCode                                   49              53         +4 (+8.2%)
Status                                      98              95         -3 (-3.1%)
Industry                                    99              96         -3 (-3.0%)
ContactEmail                                90              87         -3 (-3.3%)

================================================================================
END OF REPORT
================================================================================


>> Data insert to mysql table:
entity_id|entity_name           |entity_type|registration_number|incorporation_date|country_code|state_code|status  |industry     |contact_email                |last_update|created_at         |
---------+----------------------+-----------+-------------------+------------------+------------+----------+--------+-------------+-----------------------------+-----------+-------------------+
        1|NewAge Analytics      |Company    |REG43211           |        2018-10-02|GB          |          |Active  |Technology   |info@newageanalytics.co.uk   | 2022-05-22|2026-01-01 10:12:45|
        2|Sunset Hotels         |Company    |REG78645           |        2013-03-13|AU          |          |Active  |Hospitality  |contact@sunsethotels.com.au  | 2022-01-18|2026-01-01 10:12:45|
        3|Evergreen Organics    |Company    |REG23455           |        2019-06-20|IN          |          |Active  |Agriculture  |info@evergreenorganic.in     | 2022-03-31|2026-01-01 10:12:45|
        4|Liberty Bank          |Company    |REG11109           |        2011-07-29|US          |TX        |Pending |Finance      |support@libertybank.com      | 2022-05-10|2026-01-01 10:12:45|
        5|Rising Star Ventures  |Partnership|REG55432           |        2014-01-05|SG          |          |Active  |Finance      |contact@risingstar.sg        | 2022-04-14|2026-01-01 10:12:45|
        ...
	    ...
	    ...
       94|Horizon Solutions     |Company    |REG99910           |        2012-01-30|SG          |          |Inactive|Consulting   |support@horizon.sg           | 2021-12-05|2026-01-01 10:12:45|
       95|Everest Construction  |Company    |                   |        2015-05-19|US          |CA        |Active  |Construction |contact@everestbuild.com     | 2022-02-16|2026-01-01 10:12:45|
       96|Silverstone Trust     |Trust      |REG56012           |        2010-11-11|CA          |          |Active  |Real Estate  |trust@silverstone.ca         | 2022-04-05|2026-01-01 10:12:45|
	   
	   