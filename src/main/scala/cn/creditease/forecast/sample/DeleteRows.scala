package cn.creditease.forecast.sample

/**
  * Created by corlinchen on 2017/4/19.
  */
import org.apache.spark.{ SparkContext, SparkConf }
import unicredit.spark.hbase._

object DeleteRows extends App {
  val name = "Example of delete of entire rows"

  lazy val sparkConf = new SparkConf().setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
  implicit val config = HBaseConfig() // Assumes hbase-site.xml is on classpath

  sc.textFile("test-input")
    .map({ line => (line split "\t").head })
    .deleteHBase("test-table")
}