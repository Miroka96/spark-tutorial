package de.hpi.spark_tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleSpark {

  def main(args: Array[String]): Unit = {

    var cores = 4
    var path = "./TPCH"

    val parser = args.iterator
    while (parser.hasNext) {
      parser.next match {
        case "--path" => path = parser.next
        case "--cores" => cores = parser.next.toInt
        case default => println("Usage: java -jar <application>.jar [--path PATH=./TPCH] [--cores CORES=4]")
          System.exit(1)
      }
    }

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("InclusionDependencyDetection")
      .master(s"local[$cores]") // local
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"$path/tpch_$name.csv")

    time {Sindy.discoverINDs(inputs, spark, cores * 3)}
  }
}
