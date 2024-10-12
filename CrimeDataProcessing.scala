// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

// COMMAND ----------

val dfPreprocessed = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("dbfs:/FileStore/tables/Boston_Incidents_View__3026744829754631849.csv")

val df = dfPreprocessed
  .withColumnRenamed("Incident Number", "incident_number")
  .withColumnRenamed("Offense Code", "offense_code")
  .withColumnRenamed("Offense Description", "offense_description")
  .withColumnRenamed("Block Address", "block_address")
  .withColumnRenamed("Zip Code", "zip_code")
  .withColumnRenamed("BPD District", "BPD_district")
  .withColumnRenamed("Premise Description", "premise_description")
  .withColumnRenamed("Weapon Description", "weapon_description")
  .withColumnRenamed("Crime Part", "crime_part")
  .withColumnRenamed("Crime Category", "crime_category")
  .withColumnRenamed("Report Date", "report_date")
  .withColumnRenamed("From Date", "from_date")
  .withColumnRenamed("To Date", "to_date")
  .withColumnRenamed("Hour of Day", "hour_of_day")
  .withColumnRenamed("Day of Week", "day_of_week")
  .withColumnRenamed("Crime", "crime")

val dfLower = df.withColumn("crime", lower(col("crime")))


dfLower.show()

// COMMAND ----------

val crimeCounts = dfLower.groupBy("crime").count()
crimeCounts.orderBy(desc("count")).show(1000, false)


// COMMAND ----------

val severityMapping = Map(
  // Very High Severity (10)
  "homicide" -> 10,
  "rape & attempted" -> 10,
  "assault - aggravated" -> 10,
  "sex offense - rape - forcible" -> 10,
  "manslaughter" -> 10,
  
  // High Severity (8-9)
  "robbery" -> 9,
  "firearm violations" -> 9,
  "human trafficking" -> 9,
  "arson" -> 9,
  "sex offense - rape - sodomy" -> 9,
  "indecent assault" -> 8,
  "assault" -> 8,
  "residential burglary" -> 8,
  "auto theft" -> 8,

  // Medium-High Severity (6-7)
  "drug violation" -> 7,
  "commercial burglary" -> 7,
  "operating under the influence" -> 7,
  "restraining order violations" -> 7,
  "counterfeiting" -> 6,
  "aggravated assault" -> 6,
  "trespassing" -> 6,

  // Medium Severity (4-5)
  "larceny from mv" -> 5,
  "vandalism" -> 5,
  "fraud" -> 5,
  "disorderly conduct" -> 4,
  "simple assault" -> 4,
  "noise complaint" -> 4,
  "verbal disputes" -> 4,

  // Low-Medium Severity (2-3)
  "investigate person" -> 3,
  "missing person located" -> 3,
  "public disturbance" -> 3,
  "fraud - credit card / atm fraud" -> 2,
  "license violation" -> 2,
  "drug possession" -> 2,

  // Low Severity (1)
  "mv crash response" -> 1,
  "medical assistance" -> 1,
  "property lost" -> 1,
  "towed" -> 1,
  "service" -> 1,
  "investigate property" -> 1,
  "other" -> 1
)


// COMMAND ----------

val severityUDF = udf((crimeTitle: String) => severityMapping.getOrElse(crimeTitle, 1)) // Default to 1 if not found
val dfWithSeverity = dfLower.withColumn("crime_severity", severityUDF(col("crime")))

dfWithSeverity.show(5)

// COMMAND ----------

val spark = SparkSession.builder().getOrCreate()
val deltaPath = "/mnt/delta/crime_data"
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

// COMMAND ----------

val deltaTableExists = fs.exists(new Path(deltaPath))

val deltaDF = if(deltaTableExists){
  println("Delta table exists, loading existing value.")
  spark.read.format("delta").load(deltaPath)
} else {
  println("Delta table does not exist, creating a new table.")
  val dfCleaned = dfWithSeverity.na.drop().dropDuplicates()
  dfCleaned.write.format("delta").mode("overwrite").save(deltaPath)
  dfCleaned
}

deltaDF.show(5)
