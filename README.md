# Data Engineer assignment - Zivver (by Borja González)

## Overview

This assignment processes and transforms JSON Lines data by removing Personally Identifiable Information (PII) and extracting email domains. The transformed data is prepared for integration into an OLAP system, with each record labelled with the original file's date to enable partitioning and historical tracking.

## Requirements

- Python 3.x
- Libraries: 'json', 'os', 'glob'

## How to run the script

1. Place the input JSON Lines files in the 'input_data/' directory.
2. Run the transformation script using the following command: 'python3 -m transform'.
3. The transformed files will be saved in the 'output_data/' directory with a 'transformed_' prefix added to the original filenames.

## Transformation details

- Data is cleaned by removing the following fields:
  - `C_FIRST_NAME`
  - `C_LAST_NAME`
- A new field, `C_EMAIL_DOMAIN`, is created by extracting the domain part from the `C_EMAIL_ADDRESS` field (which is then deleted).
- Each record is also labelled with a `record_date` field, representing the date the file was ingested. This field is crucial for historical tracking and partitioning in the OLAP system.

The transformation (removal of PII and domain extraction) was performed locally using Python. Once the data is prepared, it is loaded into Snowflake for historical tracking and further analysis. 

In practice, however, transformations could also be handled directly in Snowflake using dbt, which would ensure data quality and consistency, while also supporting testing and documentation.

### Handling of missing email addresses

I noticed that several customer records do not contain an email address. These records are still loaded into the OLAP system, but the `C_EMAIL_DOMAIN` field remains empty, and the transformation process logs a warning. 

Missing email addresses may affect analyses that rely on email domain. Depending on business requirements, a strategy should be implemented to handle missing data, such as adding a flag in the OLAP system to indicate that the email address is missing with Y/N or directly exluding records without an email address before being loaded.

## Loading into an OLAP system (Snowflake)

After transforming the JSON Lines data locally, the next step would be to load the results into an OLAP system (Snowflake).

Based on the structure of the input data and the TPC-DS benchmark, I assume the target table in Snowflake would resemble a dimension table  (e.g., `customer_dim`) used to store customer-related information. 

Personally, I believe in keeping the data architecture as abstract and straightforward as possible, avoiding unnecessary complexity by using off-the-shelf products and managed services, such as Fivetran or Airbyte, for data loading into a data warehouse.

However, given the nature of this assignment, I followed a more manual approach to demonstrate the core data transformations and the loading process, assuming a local setup. 

With that in mind, here’s how I would approach the loading process (the following SQL code is supposed to be written and executed in the Snowflake Console):

1. Staging the data:
   - The transformed files are uploaded to a Snowflake stage (prior to that, the data can be converted into CSV or Parquet format, with Parquet being the preferred option due to its columnar storage and efficiency in OLAP systems):

   ```sql
   PUT file://path_to_transformed_file @~/staged_data;
   ```

2. Loading data into the target table:

Using the COPY INTO command, the data is loaded from the staging area into the target table:

```sql
COPY INTO customer_dim
FROM @~/staged_data/
FILE_FORMAT = (TYPE = 'PARQUET'); -- or CSV, depending on the format used
```

3. Handling historical data with SCD Type 2:

To manage historical data changes, I would use Slowly Changing Dimensions (SCD Type 2). This ensures that each time a customer’s information (e.g., email domain) changes, a new record is inserted while the old record is retained for historical reporting. This method is crucial for compliance and maintaining a clear data lineage, which is particularly important in secure environments like Zivver's:

```sql
MERGE INTO customer_dim AS target
USING transformed_data AS source
ON target.C_CUSTOMER_ID = source.C_CUSTOMER_ID
WHEN MATCHED AND target.C_EMAIL_DOMAIN != source.C_EMAIL_DOMAIN THEN
  UPDATE SET target.end_date = CURRENT_DATE, target.is_current = 'N'
WHEN NOT MATCHED THEN
  INSERT (C_CUSTOMER_SK, C_xCUSTOMER_ID, C_BIRTH_COUNTRY, C_CURRENT_ADDR_SK, C_PREFERRED_CUST_FLAG, C_EMAIL_DOMAIN, record_date, start_date, is_current)
  VALUES (source.C_CUSTOMER_SK, source.C_CUSTOMER_ID, source.C_BIRTH_COUNTRY, source.C_CURRENT_ADDR_SK, source.C_PREFERRED_CUST_FLAG, source.C_EMAIL_DOMAIN, source.record_date, CURRENT_DATE, 'Y');
```

4. Final table after the MERGE:
  - After the MERGE operation, the assumed 'customer_dim' table will store both the current and historical records (the example below illustrates the change of email domain by a customer):

| C_CUSTOMER_SK | C_CUSTOMER_ID     | C_BIRTH_COUNTRY | C_EMAIL_DOMAIN  | record_date | start_date | end_date   | is_current | 
|----------------------------------------------------------------------------------------------------------------------------|
| 1             | AAAAAAAAABAAAAAAA | CHILE           | VFAxlnZEvOx.org | 2021-01-10  | 2021-01-10 | 2021-03-15 | N
| 2             | AAAAAAAAABAAAAAAA | CHILE           | newdomain.com   | 2021-03-15  | 2021-03-15 | NULL       | Y

  - This setup ensures both current and historical records are maintained, allowing for efficient querying of customer data over time.

## Compliance and security

Since Zivver focuses on secure communications, the following practices should be implemented:

- Data masking or encryption: fields that still contain sensitive data, like the email domain, should be masked or encrypted to prevent unauthorized access.
- Only authorized users should have access to customer data, ensuring compliance with regulations like GDPR.

## Real-world automation with ETL tools

For simplicity, this assignment uses a manual process to load data from my local file system into Snowflake, but in a real-world scenario, I would automate the process as much as possible by using tools such as:

- Airflow: to orchestrate the pipeline, schedule and manage batch data exports, and automate the entire ETL process.
- dbt: to handle in-warehouse transformations directly within Snowflake.

In production, I would batch-process daily data exports, and use dbt to run transformations in the warehouse. This approach allows for automated testing and data validation to ensure data integrity before loading it into Snowflake.