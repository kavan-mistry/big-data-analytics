// Databricks notebook source
var a =Array(List(1,2,3),List(1,2,3),List(1,2,3))
var b =Array(List(4,5,6),List(4,5,6),List(4,5,6))
var c =Array(Array(0,0,0),Array(0,0,0),Array(0,0,0))
var sum = 0


for (i<-0 to 2){
  for (j<-0 to 2){
    sum=0
    for (k<-0 to 2) {
    sum = sum + (a(i)(k)*b(k)(j))
    }
    c(i)(j)= sum
  }
}
println(c)

// COMMAND ----------

var a= Array(Array(1,2,3),List(1,2,3),List(1,2,3))

// COMMAND ----------

a(1)

// COMMAND ----------

a(1)(1)

// COMMAND ----------

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDParallelize{

  def main(args: Array[String]): Unit ={
      val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExample.com")
      .getOrCreate()
     val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
     val rddcollect:Array[Int] = rdd.collect()
     println("Number of partitions:"+rdd.getNumPartitions)
     println("Action: First element:"+rdd.first())
     println("Action: RDD convert to array[Int]:")
     rddcollect.foreach(println)
  }
}

// COMMAND ----------

val rdda = sc.parallelize(List(1,2,3,4,5))
val rddb = rdda.collect
println("Number of Partition"+rdda.getNumPartitions)
println("Action: First element:"+rdda.first())
rdda.foreach(println)

// COMMAND ----------

val rdda = sc.parallelize(List("mumbai","delhi","chennai","kolkata"))

// COMMAND ----------

val rddb = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))

// COMMAND ----------

val rddc = sc.parallelize(Seq.empty[String])

// COMMAND ----------

rdda.collect

// COMMAND ----------

val b = rdda.map(x => (x,1))

// COMMAND ----------

b.collect

// COMMAND ----------

//short hand notation
val b =rdda.map((_,1))

// COMMAND ----------

b.collect

// COMMAND ----------

val b=rdda.map(x=>(x,x.length))

// COMMAND ----------

b.collect

// COMMAND ----------

// rdd value should be stored or else it will be lost

val a =sc.parallelize(List(1,2,3,4,5)).map(x=>List(x,x,x)).collect

// COMMAND ----------

val a=sc.parallelize(List(1,2,3,4,5)).flatMap(x=>List(x,x,x)).collect

// COMMAND ----------

val rdda = sc.parallelize(List("mumbai","mumbai","delhi","chennai","kolkatta")).filter(_.equals("mumbai")).count

// COMMAND ----------

val rdda = sc.parallelize(List("mumbai","delhi","chennai","kolkatta")).filter(_.contains("e")).collect

// COMMAND ----------

val a = sc.parallelize(List(("mumbai",4000),("delhi",2000),("chennai",1000),("kolkatta",7000)))

// COMMAND ----------

val a = sc.parallelize(List(("mumbai",4000),("delhi",2000),("chennai",1000),("kolkatta",7000))).filter(_._2.equals(4000)).collect

// COMMAND ----------

val a = sc.parallelize(List(("mumbai",4000),("delhi",2000),("chennai",1000),("kolkatta",7000))).filter(_._2 > 3000).collect

// COMMAND ----------

val a = sc.parallelize(List(("mumbai",4000),("delhi",2000),("chennai",1000),("kolkatta",7000))).filter(a=> a._1.startsWith("c")).collect

val b = sc.parallelize(List((4000,"mumbai"),(2000,"delhi"),(1000,"chennai"),(7000,"kolkatta"))).filterByRange(3000,9000).collect

// COMMAND ----------

val a = sc.parallelize(1 to 100)

// COMMAND ----------

a.collect

// COMMAND ----------

a.sample(false,.2,5).collect

// COMMAND ----------

a.sample(false,.2,5).collect

// COMMAND ----------

a.sample(false,.2,6).collect

// COMMAND ----------

a.sample(true,.2,5).collect

// COMMAND ----------

a.sample(true,.2,5).collect

// COMMAND ----------

a.sample(false,.2).collect

// COMMAND ----------

a.sample(false,.2).collect

// COMMAND ----------

a.sample(false,1,5).collect

// COMMAND ----------

val a = sc.parallelize(List(1,2,1,1,1,2))

// COMMAND ----------

a.sample(true,.4,5).collect

// COMMAND ----------

a.sample(true,.4,5).collect

// COMMAND ----------

val b = sc.parallelize(5 to 10)

// COMMAND ----------

val a = sc.parallelize(1 to 7)

// COMMAND ----------

a.union(b).collect

// COMMAND ----------

a.intersection(b).collect

// COMMAND ----------

a.union(b).distinct.collect

// COMMAND ----------

val a = sc.parallelize(1 to 9,3)

// COMMAND ----------

a.mapPartitions(x=>List(x.next).iterator).collect

// COMMAND ----------

val a = sc.parallelize(1 to 9,4)

// COMMAND ----------

a.mapPartitions(x=>List(x.next).iterator).collect

// COMMAND ----------

def practfunct(index:Int , iter:Iterator[(Int)]) : Iterator[String] = {
  iter.toList.map(x=> "[index :" +index +", val : "+x +"]").iterator
}

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5,6),2)

// COMMAND ----------

a.collect

// COMMAND ----------

a.mapPartitionsWithIndex(practfunct).collect

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5,6),3)

// COMMAND ----------

a.mapPartitionsWithIndex(practfunct).collect

// COMMAND ----------


