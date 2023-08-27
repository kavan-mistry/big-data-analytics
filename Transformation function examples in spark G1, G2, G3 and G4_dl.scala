// Databricks notebook source
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDParallelize {

  def main(args:Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
    val rddCollect:Array[Int] = rdd.collect()
    println("Number of partitions: "+rdd.getNumPartitions)
    println("Action: First element: "+rdd.first())
    println("Action: RDD converted to Array[Int]: ")
    rddCollect.foreach(println)
  }
}

// COMMAND ----------

val rdda = sc.parallelize(List(1,2,3,4,5))  // parallelize 1,2,3,4,5. Inside spark everything is rdda
    val rddb = rdda.collect
    println("No of partitions: "+rdda.getNumPartitions)
    println("Action: First element: "+rdda.first())
    rdda.foreach(println)     // for each element of rdda print it

// COMMAND ----------

val rdda = sc.parallelize(List("mumbai", "delhi", "chennai", "kolkata"))       // sc - spark context. imp to mention for hadoop comodity hardware

// COMMAND ----------

val rddb = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))

// COMMAND ----------

val rddc = sc.parallelize(Seq.empty[String])    // created empty string

// COMMAND ----------

rdda.collect      // collect is an action used to see inside rdda

// COMMAND ----------

val a = rdda.map(x => (x,1))

// COMMAND ----------

a.collect

// COMMAND ----------

// same thing using shorthand notation
val b = rddb.map((_,2))

// COMMAND ----------

b.collect

// COMMAND ----------

val b = rdda.map(x=>(x, x.length))

// COMMAND ----------

b.collect

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5)).map(x=>List(x,x,x)).collect

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5)).flatMap(x=>List(x,x,x)).collect

// COMMAND ----------

val a = sc.parallelize(List("mumbai","delhi","delhi", "chennai","kolkata")).filter(_.equals("delhi")).count
val a = sc.parallelize(List("mumbai","delhi","delhi", "chennai","kolkata")).filter(_.equals("delhi")).count

// COMMAND ----------

val a = sc.parallelize(List("mumbai","delhi", "chennai","kolkata")).filter(_.contains("e")).collect
val b = sc.parallelize(List("mumbai","delhi", "chennai","kolkata")).filter(_.contains("a")).collect

// COMMAND ----------

// create an RDD with city,count
val a = sc.parallelize(List(("mumbai",4000),("delhi", 2000),("chennai",1000),("kolkata",7000)))

// COMMAND ----------

// perform filter operation where value equals 4000  
//(here 1 is for key, 2 is for value) (forallwhereforvalue)
val a = sc.parallelize(List(("mumbai",4000),("delhi", 2000),("chennai",1000),("kolkata",7000))).filter(_._2.equals(4000)).collect

// COMMAND ----------

// perform filter operation where value is greater than 3000
val a = sc.parallelize(List(("mumbai",4000),("delhi", 2000),("chennai",1000),("kolkata",7000))).filter(_._2 > 3000).collect

// COMMAND ----------

// perform filter which starts with C
// here a._1 means in "a" where key starts with c
val a = sc.parallelize(List(("mumbai",4000),("delhi", 2000),("chennai",1000),("kolkata",7000))).filter(a=>a._1.startsWith("c")).collect
// try this -- val c = sc.parallelize(List(("mumbai",4000),("delhi", 2000),("chennai",1000),("kolkata",7000))).filter(_._2.filterByRange(2000, 5000)).collect

// COMMAND ----------

// perform filter range between 3000,9000
val b = sc.parallelize(List((4000, "mumbai"),(2000, "delhi"),(1000, "chennai"),(7000, "kolkata"))).filterByRange(3000, 9000).collect

// COMMAND ----------

val a = sc.parallelize(1 to 100)

// COMMAND ----------

a.collect

// COMMAND ----------

// sample (fasle/true, fraction, seed)
// false - can not have repeated values
// true - will have repeated values
// fraction - 0 to 1. no. of samples in o/p
// seed - result will be same if the seed os kept same - use this if you want to work with a particular set of numbers

// COMMAND ----------

a.sample(false, .2, 5).collect

// COMMAND ----------

a.sample(false, .2, 6).collect

// COMMAND ----------

a.sample(false, .2, 5).collect

// COMMAND ----------

a.sample(false, .1, 5).collect

// COMMAND ----------

a.sample(true, .2, 5).collect

// COMMAND ----------

a.sample(true, .3, 4).collect

// COMMAND ----------

a.sample(false, .2).collect

// COMMAND ----------

a.sample(false, 1, 5).collect

// COMMAND ----------

val a = sc.parallelize(List(1,2,1,1,1,2))

// COMMAND ----------

a.sample(true, .4, 5).collect

// COMMAND ----------

a.sample(false, .4, 5).collect

// COMMAND ----------

val a = sc.parallelize(1 to 7)

// COMMAND ----------

val b = sc.parallelize(5 to 10)

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

val a = sc.parallelize(1 to 20,4)

// COMMAND ----------

a.mapPartitions(x=>List(x.next).iterator).collect

// COMMAND ----------

def practfunct(index:Int, iter:Iterator[(Int)]) : Iterator[String] = {
  iter.toList.map(x=> "[index: " +index + "val : "+x+ "]").iterator
}

// COMMAND ----------

def practfunct(index:Int, iter:Iterator[(Int)]) : Iterator[String] = {
  iter.toList.map(x=> "[Index:"+index+ ", val :"+x+"]").iterator
}

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5,6), 2)

// COMMAND ----------

a.collect

// COMMAND ----------

a.mapPartitionsWithIndex(practfunct).collect

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5,6), 3)

// COMMAND ----------

a.mapPartitionsWithIndex(practfunct).collect

// COMMAND ----------

var a = sc.parallelize(1 to 100)

// COMMAND ----------

a.collect

// COMMAND ----------

def 

// COMMAND ----------



// COMMAND ----------

var a = sc.parallelize(1 to 100).filter(a=>a%2 == 0).collect
var b = sc.parallelize(1 to 100).filter(a=>a%2 != 0).collect
var c = sc.parallelize(1 to 100).filter(a=>a%2 == 0).collect

// COMMAND ----------


