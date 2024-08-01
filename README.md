# data-eng-case

## Data Pipeline for the OpenBreweryDB Project

1. **Research and Initial Exploration**: 
   - Started by reviewing the OpenBreweryDB.org website documentation to understand the List Brewery's endpoint and its parameters.
   - Used Postman to get a preliminary look at the data.

2. **Bronze Layer**: 
   - Created a Python script to download the raw data listing all breweries in ascending order without any filtering.

3. **Silver Layer**: 
   - Developed another Python script to transform the raw data into a columnar storage format.
   - Chose Parquet as the storage format and partitioned it by brewery location - state_province and country, to avoid data duplication.

4. **Gold Layer/Aggregated View**: 
   - Planned to create an aggregated view showing the quantity of breweries per type and location.
   - Objective: Create a dataset or view in BigQuery on Google Cloud Platform.
   - Use SQL to aggregate brewery data by type and location.

5. **Tooling**: 
   - Utilized Python and PySpark for the bronze and silver layer operations/transformation, and SQL for the gold Layer transformation.
   - Faced challenges integrating Airflow due to difficulties with GitHub repository integration.

6. **GitHub Repository**: 
   - Uploaded all python scripts to a GitHub repository for visibility and version control.
   - Uploaded the raw JSON data from the API.
   - Uploaded the transformed Parquet data.

7. **Google Cloud Platform (GCP)**: 
   - Leveraged GCP for creating a bucket in the data lake to store raw, transformed, and aggregated data.
   - Intended use of BigQuery with SQL for creating the aggregated view with the quantity of breweries per type and location.
   - Planned to use Python scripts within Airflow to orchestrate the pipeline and manage data movement between buckets.

8. **Bucket Setup**: 
   - Set up a GCP account and created a bucket with high availability (standard, multi-region) and low cost.

9. **Visibility and Access**: 
   - Cloud services are not posted in the GitHub repository as instructed.
   - Made the bucket public for viewing purposes.

10. **Monitoring and Alerting**:
    - Integrated monitoring and alerting processes into the Airflow Python code.
    - Airflow runs on a daily schedule, adjustable for frequency.
    - Pipeline failures trigger email alerts, with built-in fallbacks in case of data issues.
