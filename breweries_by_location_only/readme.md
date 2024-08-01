# Parquet Files Directory

This folder contains Parquet files partitioned by country. The files were converted from JSON using a custom Python function.

## Description

- **File Format:** Parquet
- **Partitioning:** By country (`country`)
- **Data Source:** JSON file
- **Conversion Tool:** Custom Python function

## Procedure

I developed a Python script to convert the files from JSON to Parquet format. 
This custom scriptn also performed data cleaning using pandas, ensuring that the data is free from inconsistencies and ready for analysis. 
The resulting Parquet files were partitioned by country  as per instructions, and also to avoid data duplication and improve query performance.

## Steps

1. **Data Loading:** Read the JSON data into a pandas DataFrame.
2. **Data Cleaning:** Clean the data using pandas, replacing spaces with underscores in column names and values.
3. **Conversion:** Convert the cleaned DataFrame to Parquet format.
4. **Partitioning:** Save the Parquet files, partitioned by country, to optimize storage and querying.

This structured approach ensures that the data is efficiently stored and easily accessible for further analysis.

