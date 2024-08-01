import pandas as pd

# Read JSON file
df = pd.read_json('/Users/naracastillo/Desktop/brews/all_breweries_data_july.json')

# Function - Substitute empty spaces for underscores
def replace_spaces_with_underscores(column):
    return column.str.replace(' ', '_')

# Apply transformation function to all string columns
df = df.apply(lambda col: replace_spaces_with_underscores(col) if col.dtype == 'object' else col)

# Save DataFrame as partitioned Parquet 
df.to_parquet('/Users/naracastillo/Desktop/breweries_by_country_only.parquet', 
              engine='pyarrow', 
              partition_cols=['country'], 
              index=False)
