import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.executorEnv.HTTP_PATH", "/path/to/service")
conf.set("spark.executorEnv.ACCESS_TOKEN", "my-token")
conf.set("spark.executorEnv.TRUSTED_CA_FILE", "/path/to/ca.crt")

# Replace <master-url> with the URL of the remote Spark cluster
spark = SparkSession.builder.appName("MyApp").master("<master-url>").config(conf=conf).getOrCreate()



# Connect to SingleStore
singlestore_config = {
    'host': 'singlestore_host',
    'user': 'singlestore_user',
    'password': 'singlestore_password',
    'database': 'singlestore_database'
}
singlestore_conn = pymysql.connect(**singlestore_config)

# Connect to Databricks
databricks_config = {
    'host': 'https://databricks_host',
    'token': 'databricks_token'
}


# Read SingleStore table into a Spark dataframe
singlestore_df = spark.read.format('jdbc').options(
    url=f"jdbc:mysql://{singlestore_config['host']}:3306/{singlestore_config['database']}",
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='singlestore_table',
    user=singlestore_config['user'],
    password=singlestore_config['password']
).load()

# Write SingleStore dataframe to a Delta table in Databricks
singlestore_df.write.format('delta').mode('overwrite').save('/mnt/databricks/singlestore_table')

# Read Databricks table into a Spark dataframe
databricks_df = spark.read.format('delta').load('/mnt/databricks/databricks_table')

# Join SingleStore dataframe with Databricks dataframe
joined_df = singlestore_df.join(databricks_df, col('singlestore_table.key') == col('databricks_table.key'), 'full_outer')

# Write joined dataframe to a new Delta table in Databricks
joined_df.write.format('delta').mode('overwrite').save('/mnt/databricks/comparison_results')

# Stop the Spark session when you're done
spark.stop()