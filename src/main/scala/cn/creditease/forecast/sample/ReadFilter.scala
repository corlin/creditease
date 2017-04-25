package cn.creditease.forecast.sample

/**
  * Created by corlinchen on 2017/4/19.
  */
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import unicredit.spark.hbase._


object ReadFilter extends App {
  val name = "Example of read from HBase table"

  lazy val sparkConf = new SparkConf().setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
  implicit val config = HBaseConfig() // Assumes hbase-site.xml is on classpath

  val columns = Map(
    "cf1" -> Set("col1", "col2"),
    "cf2" -> Set("col3")
  )

  val filter = new PrefixFilter(Bytes.toBytes("abc"))

  sc.hbase[String]("test-table", columns, filter)
    .map({ case (k, v) =>
      val cf1 = v("cf1")
      val col1 = cf1("col1")
      val col2 = cf1("col2")
      val col3 = v("cf2")("col3")

      List(k, col1, col2, col3) mkString "\t"
    })
    .saveAsTextFile("test-output")
}