from pyspark.sql import DataFrame
from pyspark.sql.functions import md5, concat_ws

def calculate_hash_code(df: DataFrame):
    # Concatenate all columns into a single string
    concatenated_cols = concat_ws("", *df.columns)

    # Calculate SHA-256 hash code
    hash_col = md5(concatenated_cols)
    return df.withColumn("hash_code", hash_col)

def compare_data_content(df1, df2):
    hash_codes_df1 = calculate_hash_code(df1).select("hash_code")
    hash_codes_df2 = calculate_hash_code(df2).select("hash_code")

    return hash_codes_df1.exceptAll(hash_codes_df2).isEmpty()