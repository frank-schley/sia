/**
  * Created by frank on 19/11/16.
  */

import org.apache.spark.sql.SparkSession

object ch04_pair_rdds {
  def main(args: Array[String]): Unit = {
    println("Chapter 4: Investigating Pair RDDs")
    val sc = getSpark().sparkContext

    // 2015-03-30#6:55 AM#51#68#1#9506.21
    val transLines = sc.textFile(getTransFilePath())
    //Tuple2[Int, Array[String]]
    //(CID, TRANS)
    val transByCust = transLines.map(createPairCustIdTrans)

    //Example
    println("Number of unique customer ids " + transByCust.keys.distinct.count)
    println("Total number of transactions ", transByCust.count)
    println("Total number of transactions ", transByCust.countByKey.values.sum)
    transByCust.countByKey.take(10).foreach(println)



    getSpark().stop()
  }

  def getSpark() = {
   SparkSession.builder()
      .appName("Chapter 4: Pair RDD")
      .master("local[*]")
      .getOrCreate
  }

  def getTransFilePath() = {
    "/Users/frank/projects/books/first-edition/ch04/ch04_data_transactions.txt"
  }

  def createPairCustIdTrans(line:String): Tuple2[Int, Array[String]] = {
    val trans = line.split("#")
    val custId = trans(2).toInt
    (custId, trans)
  }
}
