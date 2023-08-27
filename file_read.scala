// Databricks notebook source
import org.apache.spark.{SparkConf, SparkContext}
import spark.implicits._

// COMMAND ----------

// /dbfs/FileStore/tables/my_file_input-4.txt

val textRDD = sc.textFile("/FileStore/shared_uploads/kavancoc1@gmail.com/my_file_input-1.txt")
val wordRDD = textRDD.flatMap(line => line.split("\\W+"))
val wordCountRDD = wordRDD.map(word => (word.toLowerCase, 1)).reduceByKey(_ + _)
val sortedWordCountRDD = wordCountRDD.sortBy(pair => pair._2, ascending = false)
val outputRDD = sortedWordCountRDD.map(pair => s"${pair._1}, ${pair._2}")
outputRDD.saveAsTextFile("/FileStore/shared_uploads/kavancoc1@gmail.com/output_file_2.txt")
outputRDD.take(20).foreach(println)

// COMMAND ----------

val inputFile = "dbfs:/FileStore/shared_uploads/kavancoc1@gmail.com/my_file_input-1.txt"
val inputRDD = sc.textFile(inputFile)

val wordCountRDD = inputRDD
  .flatMap(line => line.split(" "))
  .map(word => (word,1))
  .reduceByKey(_ + _)

val outputFile = "dbfs:/FileStore/tables/word_count_output.txt"

wordCountRDD
  .map(pair => s"${pair._1}\t${pair._2}")
  .saveAsTextFile(outputFile)

wordCountRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/shared_uploads/kavancoc1@gmail.com/

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/shared_uploads/kavancoc1@gmail.com/output_file_2/

// COMMAND ----------

// MAGIC %fs head dbfs:/FileStore/shared_uploads/kavancoc1@gmail.com/output_file_2/part-00000

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/shared_uploads/kavancoc1@gmail.com/output_file_2.txt/

// COMMAND ----------

// MAGIC %fs head dbfs:/FileStore/shared_uploads/kavancoc1@gmail.com/output_file_2.txt/part-00001

// COMMAND ----------

// MAGIC %fs head dbfs:/FileStore/shared_uploads/kavancoc1@gmail.com/output_file_2.txt/part-00000
