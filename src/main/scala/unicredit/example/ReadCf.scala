package unicredit.example

import org.apache.spark.{ SparkContext, SparkConf }
import unicredit.spark.hbase._


object ReadCf extends App {
  val name = "Example of read from HBase table (whole column family)"

  lazy val sparkConf = new SparkConf().setMaster("local[1]").setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
  implicit val config = HBaseConfig() // Assumes hbase-site.xml is on classpath

  sc.hbase[String]("test", Set("cf1", "cf2"))
    .map({ case (k, v) =>
      val cf1 = v("cf1")
      val col1 = cf1("col1")
      val col2 = cf1("col2")
      val col3 = v("cf2")("col3")

      List(k, col1, col2, col3) mkString "\t"
    }).saveAsTextFile("test")
}