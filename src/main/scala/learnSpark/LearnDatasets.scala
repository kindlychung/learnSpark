package learnSpark

import Helper._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.language.postfixOps

/**
  * Created by kaiyin on 2/3/16.
  */
object LearnDatasets {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("wordCount").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx.implicits._

  def main(args: Array[String]) {


    new HasRun {
      override def run: Unit = {
        val inputSeq = Seq(Company1("ABC", 1998, 300), Company1("apple", 1999, 400))
        val df = sc.parallelize(inputSeq).toDF() // will not work in a repl
        df.show()
      }
    } newRun("DF from case classes")

    new HasRun {
      override def run: Unit = {
        val rdd = sc.parallelize(Seq((1, "spark"), (2, "akka"), (3, "play")))
        val df = rdd.toDF("id", "name")
        df.printSchema()
        // convert a dataframe to dataset
        val ds = df.as[(Int, String)]
        df.show()
        ds.show()
      }
    } newRun("DS from tuples")

    new HasRun {
      override def run: Unit = {
        val lines: Dataset[String] = sqlCtx.read.text("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/testfile").as[String]
        val words: Dataset[String] = lines.flatMap(_.split(""" """)).filter(_ != "")
        val counts = words.groupBy(_.toLowerCase()).count()
        counts.show()
      }
    } newRun("DS from text file")

    new HasRun {
      override def run: Unit = {
        // one json object per line
        val persons = sqlCtx.read.json("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/persons.json").as[Person]
        persons.show()
        persons.map(p => {
          if(p.lovesPandas) s"${p.name} loves pandas" else s"${p.name} does not love pandas"
        }).show()
      }
    } newRun("DS from json")

    new HasRun {
      override def run: Unit = {
        val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father", "And spark i am your mother")).toDS()
        val groupedDataset = wordsDataset.flatMap(_ split ("""\s+"""))
          .groupBy(_.toLowerCase())
        val countsDataset = groupedDataset.count()
        countsDataset.show()
      }
    } newRun

    new HasRun {
      override def run: Unit = {
        val employeeDataSet1 = sc.parallelize(Seq(Employee("Max", 22, 1, 100000.0), Employee("Adam", 33, 2, 93000.0), Employee("Eve", 35, 2, 89999.0), Employee("Muller", 39, 3, 120000.0))).toDS()
        val employeeDataSet2 = sc.parallelize(Seq(Employee("John", 26, 1, 990000.0), Employee("Joe", 38, 3, 115000.0))).toDS()
        val departmentDataSet = sc.parallelize(Seq(Department(1, "Engineering"), Department(2, "Marketing"), Department(3, "Sales"))).toDS()
        val employeeDataset = employeeDataSet1.union(employeeDataSet2)
        employeeDataset.show()
        def averageSalary(key: (Int, String), iterator: Iterator[Record]): ResultSet = {
          val (total, count) = iterator.foldLeft(0.0, 0.0) {
            case ((total, count), x) => (total + x.salary, count + 1)
          }
          ResultSet(key._1, key._2, total/count)
        }
        val averageSalaryDataset = employeeDataset.joinWith(
          departmentDataSet, $"departmentId" === $"id", "inner"
        ).map(
          record => Record(record._1.name, record._1.age, record._1.salary, record._1.departmentId, record._2.name)
        ).filter(
          record => record.age > 25
        ).groupBy(
          record => (record.departmentId, record.departmentName)
        ).mapGroups((key, iter) => averageSalary(key, iter))
        averageSalaryDataset.show()
      }
    } newRun("Ds join and mapGroups")

    new HasRun {
      override def run: Unit = {
        val people = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/persons.txt")
          .map(_.split("""\s+"""))
          .map(p => Person(p(0), p(1).toBoolean))
          .toDF()
        people.registerTempTable("people")
        val pandaLovers = sqlCtx.sql("select name, lovesPandas from people")
        pandaLovers.show()
      }
    } newRun
  }
}
