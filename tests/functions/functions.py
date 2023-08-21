from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws

def calculate_hash_code(df: DataFrame):
    # Concatenate all columns into a single string
    concatenated_cols = concat_ws("", *df.columns)

    # Calculate SHA-256 hash code
    hash_col = sha2(concatenated_cols, 256)
    return df.withColumn("hash_code", hash_col)

def compare_data_content(df1, df2):
    hash_codes_df1 = calculate_hash_code(df1).collect()
    hash_codes_df2 = calculate_hash_code(df2).collect()

    return set(hash_codes_df1) == set(hash_codes_df2)