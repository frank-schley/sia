/**
  * Created by frank on 15/10/16.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object tranformations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkMe Application")
    val sc = new SparkContext(conf)
    val home = System.getenv("HOME")
    val fileName = home + "/projects/books/first-edition/ch04/ch04_data_transactions.txt"
    val trans = sc.textFile(fileName).map(line => line.split("#"))
    val transByCust = trans.map(tran => (tran(2).toInt, tran))
    println(transByCust.countByKey())
    println(transByCust.lookup(53))

    sc.stop()
  }
}

