// Databricks notebook source
import org.apache.spark.sql.types._

// COMMAND ----------

val schema = StructType(Array(
  StructField("cluster_id", IntegerType, nullable = false),
  StructField("vertex1_lat", DoubleType, nullable = false),
  StructField("vertex1_lon", DoubleType, nullable = false),
  StructField("vertex2_lat", DoubleType, nullable = false),
  StructField("vertex2_lon", DoubleType, nullable = false),
  StructField("vertex3_lat", DoubleType, nullable = false),
  StructField("vertex3_lon", DoubleType, nullable = false),
  StructField("vertex4_lat", DoubleType, nullable = false),
  StructField("vertex4_lon", DoubleType, nullable = false),
  StructField("crime_score", DoubleType, nullable = true)
))

// COMMAND ----------

val empty4GonDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

// COMMAND ----------

val polygonPath = "/mnt/delta/4gons"
empty4GonDF.write.format("delta").mode("overwrite").save(polygonPath)

// COMMAND ----------

val crime4GonsDF = spark.read.format("delta").load(polygonPath)
crime4GonsDF.printSchema()
crime4GonsDF.show()

// COMMAND ----------


