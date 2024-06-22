# Spark-and-Human-Balance-Analytics
Udacity project. Act as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.


## Overview
The STEDI Human Balance Analytics project is designed to analyze sensor data from the STEDI Step Trainer to understand and improve user balance through data-driven insights. The project leverages AWS services including Glue, Athena, and S3 to manage and analyze a large dataset consisting of customer data, accelerometer readings, and step trainer records.

## Project Structure

### Landing Zone
Data is initially ingested from various sources into S3 buckets, categorized into three primary types:
1. **Customer Data**: Details about the customers.
2. **Accelerometer Data**: Sensor readings from mobile devices.
3. **Step Trainer Data**: Sensor outputs from the STEDI Step Trainer devices.

#### Submission Requirements
- **Glue Studio Jobs**: Created for ingesting data from S3:
  - `customer_landing_to_trusted.py`
  - `accelerometer_landing_to_trusted.py`
  - `step_trainer_trusted.py`
- **Glue Tables**: Manually created from JSON data in the Glue Console.
- **SQL DDL Scripts**: Provided for each data type to ensure all JSON fields are included and appropriately typed.
- **Data Validation**: Performed using Athena with specific queries to count and validate data consistency.

### Trusted Zone
Data is processed and sanitized, ensuring that only data from consenting customers is moved forward for analysis.

#### Submission Requirements
- **Dynamic Schema Configuration**: Glue Studio is configured to dynamically update Glue Table schemas from JSON data.
- **PII Filtering**: Personal Identifiable Information is filtered out using Spark transformations in Glue jobs.
- **Data Joins**: Various joins are performed to consolidate customer data with corresponding accelerometer readings based on email matches.

### Curated Zone
Data from the Trusted Zone is further refined and prepared for specific analytical purposes, creating highly targeted datasets for machine learning and data analysis.

#### Submission Requirements
- **Glue Jobs for Data Joining**: Created to join trusted data:
  - `customer_trusted_to_curated.py`
  - `step_trainer_trusted.py`
  - `machine_learning_curated.py`
- **Querying and Data Preview**: Extensive querying and previews are done using Athena to ensure the accuracy and integrity of the curated datasets.

## Key Tables and Their Roles
- **customer_landing, accelerometer_landing, step_trainer_landing**: Initial raw data tables from various sources.
- **customer_trusted, accelerometer_trusted, step_trainer_trusted**: Tables containing sanitized data from consenting customers.
- **customers_curated**: Contains customer data that has both consented to data sharing and has corresponding accelerometer data.
- **machine_learning_curated**: Aggregated data combining Step Trainer and Accelerometer data, ready for machine learning purposes.

## Performance and Optimization Hints
- **Transform - SQL Query Nodes**: Recommended for more consistent and robust outputs compared to other node types.
- **Data Source Nodes**: Adjustments recommended if data extraction is incomplete.
- **Data Preview**: Use the feature to preview at least 500 rows to ensure data consistency.

## Querying Data
- Regular queries run on Athena to check data counts and integrity across all stages of data processing.
- Screenshots from these queries are included to provide a clear picture of the data at each stage.

## Conclusion
This project utilizes a comprehensive AWS-based architecture to process, clean, and analyze data for the STEDI Human Balance project, ensuring that the insights derived are based on accurate and well-curated data, adhering to user consent and data privacy standards.
