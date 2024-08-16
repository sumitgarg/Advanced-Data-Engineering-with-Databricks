# Databricks notebook source
# MAGIC %run "/Workspace/Users/sumit202412az@gmail.com/Advanced-Data-Engineering-with-Databricks/advanced-data-engineering-with-databricks/ADE 1 - Incremental Processing with Spark Structured Streaming/Includes/Classroom-Setup-01.1"

# COMMAND ----------

df = spark.readStream.format("delta").load(DA.paths.events)
display(df)

# COMMAND ----------


from pyspark.sql.functions import col
email_traffic_df = (df.filter(col("traffic_source")=="email")
                        .withColumn("mobile",col("device").isin(["iOS","Android"]))
                        .select("user_id","event_timestamp","mobile"))
display(email_traffic_df)

# COMMAND ----------

checkpoint_path = f"{DA.paths.working_dir}/email_traffic"
output_path = f"{DA.paths.working_dir}/email_traffic/output"

devices_query = (email_traffic_df
                 .writeStream
                 .format("delta")
                 .outputMode("append")
                 .queryName("email_traffic")
                 .trigger(processingTime="1 second")
                 .option("checkpointLocation",checkpoint_path)
                 .start(output_path))

# COMMAND ----------

devices_query.id

# COMMAND ----------

devices_query.status

# COMMAND ----------

devices_query.lastProgress

# COMMAND ----------

import time
time.sleep(10)
devices_query.stop()

# COMMAND ----------

devices_query.awaitTermination()

# COMMAND ----------

DA.cleanup()

# COMMAND ----------


