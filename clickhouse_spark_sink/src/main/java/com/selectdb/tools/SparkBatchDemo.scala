package com.selectdb.tools

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.control.Exception


object SparkBatchDemo {
  def main(args: Array[String]) {

    try {

      //1:input data path
      val hdfsSourcePath = "/Users/alex/Documents/dev/evn/clickhouse/data/pulsar_clickhouse_jdbc_sink/file_table/data.Parquet"

      //2:create SparkSession
      val spark = SparkSession
        .builder()
        .appName("SparkBatchDemo")
        .master("local")
        //.master("yarn")
        // .config("spark.some.config.option", "some-value")
        .getOrCreate()
      //3:read data from hdfs
      val df = spark.read.parquet(hdfsSourcePath)
      //4.1: Displays the content of the DataFrame to stdout
      df.show()
      df.printSchema()
    //  df.select("name").show()


    } catch {
      case ex: Exception => {
        ex.printStackTrace() // 打印到标准err
        System.err.println("exception===>: ...") // 打印到标准err
      }
    }
  }
}