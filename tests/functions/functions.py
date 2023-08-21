from pyspark.sql import DataFrame

def compare_data_content(df1, df2):
    # Sort DataFrames to ensure consistent order of rows
    sorted_df1 = df1.orderBy(*df1.columns)
    sorted_df2 = df2.orderBy(*df2.columns)

    # Convert DataFrames to RDDs and compare their content
    rdd1 = sorted_df1.rdd.map(tuple)
    rdd2 = sorted_df2.rdd.map(tuple)

    return rdd1.collect() == rdd2.collect()
