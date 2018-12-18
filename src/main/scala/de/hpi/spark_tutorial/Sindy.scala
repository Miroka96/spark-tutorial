package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.immutable

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession, partitionCount :Int = 32): Unit = {
    val tables: immutable.Seq[DataFrame] = inputs.map { source =>
      spark
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(source)
        .repartition(partitionCount)
    }
    //tables.foreach(_.printSchema())
    /*
root
 |-- R_REGIONKEY: integer (nullable = true)
 |-- R_NAME: string (nullable = true)
 |-- R_COMMENT: string (nullable = true)

root
 |-- N_NATIONKEY: integer (nullable = true)
 |-- N_NAME: string (nullable = true)
 |-- N_REGIONKEY: integer (nullable = true)
 |-- N_COMMENT: string (nullable = true)

root
 |-- S_SUPPKEY: integer (nullable = true)
 |-- S_NAME: string (nullable = true)
 |-- S_ADDRESS: string (nullable = true)
 |-- S_NATIONKEY: integer (nullable = true)
 |-- S_PHONE: string (nullable = true)
 |-- S_ACCTBAL: double (nullable = true)
 |-- S_COMMENT: string (nullable = true)

root
 |-- C_CUSTKEY: integer (nullable = true)
 |-- C_NAME: string (nullable = true)
 |-- C_ADDRESS: string (nullable = true)
 |-- C_NATIONKEY: integer (nullable = true)
 |-- C_PHONE: string (nullable = true)
 |-- C_ACCTBAL: double (nullable = true)
 |-- C_MKTSEGMENT: string (nullable = true)
 |-- C_COMMENT: string (nullable = true)

root
 |-- P_PARTKEY: integer (nullable = true)
 |-- P_NAME: string (nullable = true)
 |-- P_MFGR: string (nullable = true)
 |-- P_BRAND: string (nullable = true)
 |-- P_TYPE: string (nullable = true)
 |-- P_SIZE: integer (nullable = true)
 |-- P_CONTAINER: string (nullable = true)
 |-- P_RETAILPRICE: double (nullable = true)
 |-- P_COMMENT: string (nullable = true)

root
 |-- L_ORDERKEY: integer (nullable = true)
 |-- L_PARTKEY: integer (nullable = true)
 |-- L_SUPPKEY: integer (nullable = true)
 |-- L_LINENUMBER: integer (nullable = true)
 |-- L_QUANTITY: double (nullable = true)
 |-- L_EXTENDEDPRICE: double (nullable = true)
 |-- L_DISCOUNT: double (nullable = true)
 |-- L_TAX: double (nullable = true)
 |-- L_RETURNFLAG: string (nullable = true)
 |-- L_LINESTATUS: string (nullable = true)
 |-- L_SHIP: timestamp (nullable = true)
 |-- L_COMMIT: timestamp (nullable = true)
 |-- L_RECEIPT: timestamp (nullable = true)
 |-- L_SHIPINSTRUCT: string (nullable = true)
 |-- L_SHIPMODE: string (nullable = true)
 |-- L_COMMENT: string (nullable = true)

root
 |-- O_ORDERKEY: integer (nullable = true)
 |-- O_CUSTKEY: integer (nullable = true)
 |-- O_ORDERSTATUS: string (nullable = true)
 |-- O_TOTALPRICE: double (nullable = true)
 |-- O_ORDER: timestamp (nullable = true)
 |-- O_ORDERPRIORITY: string (nullable = true)
 |-- O_CLERK: string (nullable = true)
 |-- O_SHIPPRIORITY: integer (nullable = true)
 |-- O_COMMENT: string (nullable = true)

     */

    val columns: immutable.Seq[Dataset[Row]] = tables.flatMap(t =>
      t.columns
        .map(
          t.select(_)   // create a new dataset with a single column
            .na.drop    // drop empty lines
            .distinct ) // add set functionality
    )

    columns.foreach(_.cache)

    val candidatePairs: immutable.Seq[(Dataset[Row], Dataset[Row])] = columns.flatMap{ colA: Dataset[Row] =>
      columns
        .filter(_ != colA)                            // do not check with itself
        .filter(_.dtypes(0)._2 == colA.dtypes(0)._2)  // filter for equal data types
        .map( (colA,_) )
    }

    val inclusionDependencies: immutable.Seq[(Dataset[Row], Dataset[Row])] = candidatePairs.filter{ cols =>
      cols._1.except(cols._2).count == 0}

    inclusionDependencies
      .map( cols => (cols._1.columns(0), cols._2.columns(0)) )  // further use only column names
      .groupBy(_._1)
      .map{ keyValue => (keyValue._1, keyValue._2.map(_._2).sorted.reduce( _ + ", " + _)) }
      .toList.sortBy(_._1)
      .foreach( cols => println(s"${cols._1} < ${cols._2}"))

    /*
C_CUSTKEY < P_PARTKEY
C_NATIONKEY < N_NATIONKEY, S_NATIONKEY
L_COMMIT < L_RECEIPT, L_SHIP
L_LINENUMBER < C_CUSTKEY, C_NATIONKEY, L_PARTKEY, L_SUPPKEY, N_NATIONKEY, O_ORDERKEY, P_PARTKEY, P_SIZE, S_NATIONKEY, S_SUPPKEY
L_LINESTATUS < O_ORDERSTATUS
L_ORDERKEY < O_ORDERKEY
L_PARTKEY < P_PARTKEY
L_SUPPKEY < C_CUSTKEY, P_PARTKEY, S_SUPPKEY
L_TAX < L_DISCOUNT
N_NATIONKEY < C_NATIONKEY, S_NATIONKEY
N_REGIONKEY < C_NATIONKEY, N_NATIONKEY, R_REGIONKEY, S_NATIONKEY
O_CUSTKEY < C_CUSTKEY, P_PARTKEY
O_SHIPPRIORITY < C_NATIONKEY, N_NATIONKEY, N_REGIONKEY, R_REGIONKEY, S_NATIONKEY
P_SIZE < C_CUSTKEY, L_PARTKEY, L_SUPPKEY, P_PARTKEY, S_SUPPKEY
R_REGIONKEY < C_NATIONKEY, N_NATIONKEY, N_REGIONKEY, S_NATIONKEY
S_NATIONKEY < C_NATIONKEY, N_NATIONKEY
S_SUPPKEY < C_CUSTKEY, L_SUPPKEY, P_PARTKEY
     */
  }
}
