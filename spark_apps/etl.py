import pyspark
from pyspark.sql import SparkSession
import os

# Inisialize Spark
spark = SparkSession.builder.appName('test2').getOrCreate()

# check path parquet
parquet_path = 'fhvhv/2021/01/'

if not os.path.exists(parquet_path):
    raise FileNotFoundError(f"Path {parquet_path} tidak ditemukan atau kosong!")

# read data 
df = spark.read.parquet(parquet_path)

# create temp view for sql
df.createOrReplaceTempView('df2')

# query data from temp view
result = spark.sql('''
    SELECT 
        hvfhs_license_num, 
        COUNT(*) AS total_trips
    FROM df2
    GROUP BY hvfhs_license_num
    ORDER BY total_trips DESC
''')

# path to save result
current_dir = os.getcwd()
result_path = os.path.join(current_dir, 'result')

# write result with i file
result.repartition(1).write.parquet(result_path, mode='overwrite')

# stop spark
spark.stop()
