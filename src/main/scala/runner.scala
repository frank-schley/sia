/**
  * Created by frank on 11/10/16.
  */

import org.apache.spark.sql.SparkSession
import scala.io.Source.fromFile


object runner {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
                  .appName("Git push counter")
                  .master("local[*]")
                  .getOrCreate
    val sc = spark.sparkContext
    val home = System.getenv("HOME")
    val projectDir = home + "/IdeaProjects/sia"
    val ghJsonLog = projectDir + "/data/2015-03-01-0.json"
    val employeesFile = projectDir + "/data/ghEmployees.txt"
    val ghLog = spark.read.json(ghJsonLog)
    val pushes = ghLog.filter("type = 'PushEvent'")
    println("Number of pushes is " + pushes.count)
    val grouped = pushes.groupBy("actor.login").count
    grouped.show(5)
    val ordered = grouped.orderBy(grouped("count").desc)


    val employees = Set() ++ (
        for {
          line <- fromFile(employeesFile).getLines
        } yield line.trim
      )

    val bcEmployees = sc.broadcast(employees)

    def isAnEmp(name: String): Boolean = {
      bcEmployees.value.contains(name)
    }

    val isEmployee = spark.udf.register("isEmployee", (name:String) => isAnEmp(name) )

    import spark.implicits._
    val filteredEmps = ordered.filter(isEmployee($"login"))
    filteredEmps.show(10)
    spark.stop()
  }
}