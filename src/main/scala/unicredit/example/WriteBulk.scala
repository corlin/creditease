package unicredit.example

import org.apache.spark.{ SparkContext, SparkConf }
import unicredit.spark.hbase._

object WriteBulk extends App {
  val name = "Example of write using HFiles"

  lazy val sparkConf = new SparkConf().setMaster("local[1]").setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
  implicit val config = HBaseConfig() // Assumes hbase-site.xml is on classpath
  val admin = Admin()
  val table = "test"
  val file = "test"
  val families = Set("cf1", "cf2") //

  if (admin.tableExists(table, families)) {
    sc.textFile(file)
      .map({ line =>
        val Array(k, col1, col2, col3) = line split "\t"
        val content = Map(
          "cf1" -> Map("col1" -> col1, "col2" -> col2)
          , "cf2" -> Map("col3" -> col3)
        )

        k -> content
      })
      .toHBaseBulk(table)
  }
  admin.close
}