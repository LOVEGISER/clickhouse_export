package com.selectdb.tools

import org.apache.spark.sql.SparkSession


object SparkJDBCRead {
  def main(args: Array[String]) {

    try {


      //1:create SparkSession
      val spark = SparkSession
        .builder()
        .appName("SparkJDBCRead")
        .master("local")
        //.master("yarn")
        // .config("spark.some.config.option", "some-value")
        .getOrCreate()

      spark.read
        .format("jdbc")
        .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
        .option("url", "jdbc:clickhouse://localhost:8123/pulsar_clickhouse_jdbc_sink")
        .option("dbtable", "pulsar_clickhouse_jdbc_sink")//这种方式会一次将CK的表加载到Spark，对于上TB的表，该方案不适合
        .load().show()
      spark.stop()
    } catch {
      case ex: Exception => {
        ex.printStackTrace() // 打印到标准err
        System.err.println("exception===>: ...") // 打印到标准err
      }
    }
  }
}