package cn.creditease.forecast.sample

/**
  * Created by corlinchen on 2017/4/18.
  * https://github.com/unicredit/hbase-rdd-examples
  */
import org.apache.spark.{SparkConf, SparkContext}
import unicredit.spark.hbase._


object WriteSingleCf extends App {
  val name = "Example of write on a single column family"

  lazy val sparkConf = new SparkConf().setMaster("local[1]")setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
  implicit val config = HBaseConfig() // Assumes hbase-site.xml is on classpath

  sc.textFile("test-input")
    .map({ line =>
      val Array(k, col1, col2, _) = line split "\t"
      val content = Map("col1" -> col1, "col2" -> col2)

      k -> content
    })
    .toHBase("test-table", "cf1")
}
