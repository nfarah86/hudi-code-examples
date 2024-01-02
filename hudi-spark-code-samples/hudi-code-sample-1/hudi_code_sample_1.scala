import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

// Initialize Spark Session with Hudi support
val spark = SparkSession.builder()
  .appName("Getting Started with Hudi")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .config("spark.sql.extensions", "org.apache.hudi")
  .master("local[*]") // Use local in a non-clustered environment
  .getOrCreate()

import spark.implicits._

// Create mock data
val data = Seq(
  (1, "nadine", "2023-01-01"),
  (2, "romeo", "2023-01-02")
)
val df = data.toDF("id", "name", "date")

// Define Hudi table options
val tableName = "hudi_table"
val basePath = "file:///tmp/hudi_table" // Specify the path appropriately
val hudiOptions = Map[String, String](
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.partitionpath.field" -> "date",
  "hoodie.datasource.write.precombine.field" -> "date",
  "hoodie.datasource.write.operation" -> "upsert",
  "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE"
)

// Write data to Hudi table
df.write.format("hudi")
  .options(hudiOptions)
  .mode(SaveMode.Append)
  .save(basePath)

// Read data from Hudi table using Snapshot Query
val hudiReadOptions = Map[String, String](
  "hoodie.datasource.query.type" -> "snapshot"
)

val hudiDf = spark.read.format("hudi")
  .options(hudiReadOptions)
  .load(basePath)

hudiDf.show()
