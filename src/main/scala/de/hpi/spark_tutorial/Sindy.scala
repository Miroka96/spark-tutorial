package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.immutable

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession, partitionCount :Int = 24): Unit = {
    val tables: immutable.Seq[DataFrame] = inputs.map { source =>
      spark
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(source)
        .repartition(partitionCount)
    }
    tables.foreach(_.printSchema())
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

    val columns = tables.flatMap( t =>
      t.columns
        .map(
          t.select(_)   // create a new dataset with a single column
            .na.drop    // drop empty lines
            .distinct ) // add set functionality
    )

    println("prepared columns")
    columns.foreach(_.cache)
    println("cached columns")
    println()

    val candidatePairs = columns.flatMap{ colA: Dataset[Row] =>
      columns
        .filter(_ != colA)                            // do not check with itself
        .filter(_.dtypes(0)._2 == colA.dtypes(0)._2)  // filter for equal data types
        .map( (colA,_) )
    }

    val inclusionDependencies = candidatePairs.filter{ cols =>
      cols._1.except(cols._2).count == 0}

    println("Inclusion Dependencies:")
    inclusionDependencies
      .map( cols => (cols._1.columns(0), cols._2.columns(0)) )
      .foreach( cols => println(s"${cols._1} is included in ${cols._2}"))

    /*
    R_REGIONKEY is included in N_NATIONKEY
R_REGIONKEY is included in N_REGIONKEY
R_REGIONKEY is included in S_NATIONKEY
R_REGIONKEY is included in C_NATIONKEY
N_NATIONKEY is included in S_NATIONKEY
N_NATIONKEY is included in C_NATIONKEY
N_REGIONKEY is included in R_REGIONKEY
N_REGIONKEY is included in N_NATIONKEY
N_REGIONKEY is included in S_NATIONKEY
N_REGIONKEY is included in C_NATIONKEY
S_SUPPKEY is included in C_CUSTKEY
S_SUPPKEY is included in P_PARTKEY
S_SUPPKEY is included in L_SUPPKEY
S_NATIONKEY is included in N_NATIONKEY
S_NATIONKEY is included in C_NATIONKEY
C_CUSTKEY is included in P_PARTKEY
C_NATIONKEY is included in N_NATIONKEY
C_NATIONKEY is included in S_NATIONKEY
P_SIZE is included in S_SUPPKEY
P_SIZE is included in C_CUSTKEY
P_SIZE is included in P_PARTKEY
P_SIZE is included in L_PARTKEY
P_SIZE is included in L_SUPPKEY
L_ORDERKEY is included in O_ORDERKEY
L_PARTKEY is included in P_PARTKEY
L_SUPPKEY is included in S_SUPPKEY
L_SUPPKEY is included in C_CUSTKEY
L_SUPPKEY is included in P_PARTKEY
L_LINENUMBER is included in N_NATIONKEY
L_LINENUMBER is included in S_SUPPKEY
L_LINENUMBER is included in S_NATIONKEY
L_LINENUMBER is included in C_CUSTKEY
L_LINENUMBER is included in C_NATIONKEY
L_LINENUMBER is included in P_PARTKEY
L_LINENUMBER is included in P_SIZE
L_LINENUMBER is included in L_PARTKEY
L_LINENUMBER is included in L_SUPPKEY
L_LINENUMBER is included in O_ORDERKEY
L_TAX is included in L_DISCOUNT
L_LINESTATUS is included in O_ORDERSTATUS
L_COMMIT is included in L_SHIP
L_COMMIT is included in L_RECEIPT
O_CUSTKEY is included in C_CUSTKEY
O_CUSTKEY is included in P_PARTKEY
O_SHIPPRIORITY is included in R_REGIONKEY
O_SHIPPRIORITY is included in N_NATIONKEY
O_SHIPPRIORITY is included in N_REGIONKEY
O_SHIPPRIORITY is included in S_NATIONKEY
O_SHIPPRIORITY is included in C_NATIONKEY
     */
  }
}
