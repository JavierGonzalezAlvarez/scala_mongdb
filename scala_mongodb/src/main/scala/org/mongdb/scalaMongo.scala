package org.mongdb
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.time.LocalDateTime
import scala.collection._

object scalaMongo {

  def main(args: Array[String]): Unit
  = {
    println("Geting conexion to mongodb")
    val inputUri = "mongodb://127.0.0.1/"
    val outputUri = "mongodb://127.0.0.1/"

    val spark: SparkSession = SparkSession
      .builder()
      .appName("mongodb")
      .master("local[*]")
      .config("spark.mongodb.input.uri", inputUri)
      .config("spark.mongodb.output.uri", outputUri)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val now = LocalDateTime.now().toString
    println(now)

    val schema = StructType(
      List(
        StructField("grupo", StringType, true),
        StructField("valor", IntegerType, true),
        StructField("fecha", StringType, true),
        StructField("nombre", StringType, true),
        StructField("fechaValor", StringType, true),
      )
    )

    var dfGeneral = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    dfGeneral.printSchema()

    val sql: List[Seq[String]] = scala.collection.immutable.List(
      Seq("clients", "bios", "[{'$project': {'_id': \"$_id\", 'death': {'$substr': [\"$birth\", 0, 10,]}}}, {'$group': {'_id': \"$death\", 'count': {'$sum': 1}}}]", "result1"),
      Seq("clients", "bios", "[{'$project': {'name': 1}}, {'$group': {'_id': 'name.first', 'count': {'$sum': 1}}},]", "result2"),
      Seq("clients", "users", "[{'$project': {'_id': \"$_id\"}}, {'$group': {'_id': \"$id\", 'count': {'$sum': 1}}},]", "result3"),
    )

    for (i <- sql) {
      println("Metric => ", (i)(3))
      var dfLoader = spark.read
        .format("com.mongodb.spark.sql.DefaultSource")
        .option("uri", inputUri)
        .option("database", (i)(0))
        .option("collection", (i)(1))
        .option("pipeline", (i)(2))
        .load()
      dfLoader.printSchema()
      dfLoader.show()
      dfLoader = dfLoader.withColumnRenamed("_id", "grupo")
        .withColumnRenamed("count", "valor")
        .withColumn("fecha", lit(now))
        .withColumn("nombre", lit((i)(3)))
        .withColumn("fechaValor", lit(now))
      dfGeneral = dfGeneral.unionByName(dfLoader, allowMissingColumns = true)
    }

    dfGeneral = dfGeneral
      .withColumn("fechaFormatted", from_unixtime(col("fecha").cast("long"), "yyyy - MM - dd HH: mm: ss"))
      .withColumn("metricDate", from_utc_timestamp(col("fechaValor"), "America/New_York"))
      .withColumn("fechaValorFormatted", from_unixtime(col("metricDate").cast("long"), "yyyy - MM - dd HH: mm: ss")).dropDuplicates().drop("metricDate")
    dfGeneral.show(100)
  }
}

/*
root
 |-- grupo: string (nullable = true)
 |-- valor: integer (nullable = true)
 |-- fecha: string (nullable = true)
 |-- nombre: string (nullable = true)
 |-- fechaValor: string (nullable = true)

(Metric => ,result1)
root
 |-- _id: string (nullable = true)
 |-- count: integer (nullable = true)

+----------+-----+
|       _id|count|
+----------+-----+
|1956-01-31|    1|
|1927-09-04|    1|
|1906-12-09|    1|
|1965-04-14|    1|
|1924-12-03|    1|
|1941-09-09|    1|
|1926-08-27|    1|
|1931-10-12|    1|
|1955-05-19|    1|
|          |    1|
+----------+-----+

(Metric => ,result2)
root
 |-- _id: string (nullable = true)
 |-- count: integer (nullable = true)

+----------+-----+
|       _id|count|
+----------+-----+
|name.first|   10|
+----------+-----+

(Metric => ,result3)
root
 |-- _id: void (nullable = true)
 |-- count: integer (nullable = true)

+----+-----+
| _id|count|
+----+-----+
|null|    3|
+----+-----+

+----------+-----+--------------------+-------+--------------------+--------------+--------------------+
|     grupo|valor|               fecha| nombre|          fechaValor|fechaFormatted| fechaValorFormatted|
+----------+-----+--------------------+-------+--------------------+--------------+--------------------+
|1926-08-27|    1|2023-02-18T21:23:...|result1|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|1924-12-03|    1|2023-02-18T21:23:...|result1|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|          |    1|2023-02-18T21:23:...|result1|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|1956-01-31|    1|2023-02-18T21:23:...|result1|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|1906-12-09|    1|2023-02-18T21:23:...|result1|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|1931-10-12|    1|2023-02-18T21:23:...|result1|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|1955-05-19|    1|2023-02-18T21:23:...|result1|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|1941-09-09|    1|2023-02-18T21:23:...|result1|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|1927-09-04|    1|2023-02-18T21:23:...|result1|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|1965-04-14|    1|2023-02-18T21:23:...|result1|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|name.first|   10|2023-02-18T21:23:...|result2|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
|      null|    3|2023-02-18T21:23:...|result3|2023-02-18T21:23:...|          null|2023 - 02 - 18 16...|
+----------+-----+--------------------+-------+--------------------+--------------+--------------------+

 */