package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level

// A Scala case class; works out of the box as Dataset type using Spark's implicit encoders
case class Person(name:String, surname:String, age:Int)

// A non-case class; requires an encoder to work as Dataset type
class Pet(var name:String, var age:Int) {
  override def toString = s"Pet(name=$name, age=$age)"
}

object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    // Read a Dataset from a file
    /*val employees = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/employees.csv") // also text, json, jdbc, parquet
      .as[(String, Int, Double, String)]

    val students = spark
      .read
      .option("inferSchema", "false")
      .option("header", "false")
      .option("quote", "\"")
      .option("delimiter", ",")
      .csv(s"data/students.csv")
      .toDF("ID", "Name", "Password", "Gene")
      .as[(String, String, String, String)]

    time {
      val result = students
        .repartition(32)
        //.joinWith(students, students1.col("ID") =!= students2.col("ID"))
        .crossJoin(students).filter(r => !r.getString(0).equals(r.getString(4)))
        .as[(String, String, String, String, String, String, String, String)]
        .map(t => (t._1, longestCommonSubstring(t._4, t._8)))
        .groupByKey(t => t._1)
        .mapGroups{ (key, iterator) => (key, iterator
          .map(t => t._2)
          .reduce((a,b) => { if (a.length > b.length) a else b })) }
        .toDF("ID", "Substring")
        .show(200)
    */}

    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"data/TPCH/tpch_$name.csv")

    //time {Sindy.discoverINDs(inputs, spark)}
  }
}
