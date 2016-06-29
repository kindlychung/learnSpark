import java.io.{StringReader, StringWriter}
import java.net.URI
import java.util
import Helper._
import org.apache.spark.sql.SQLContext
import collection.JavaConverters._
import au.com.bytecode.opencsv.CSVReader
import scala.collection.mutable
import scala.language.postfixOps
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import scala.util.Try
import org.apache.log4j.{Logger, Level}
import com.databricks.spark.csv

import learnSpark._
import learnSpark.SparkCSVImplicits._

/**
  * Created by kaiyin on 1/31/16.
  */
object Intro {


  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("wordCount").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx.implicits._

  def main(args: Array[String]) {

    val inputFile = "/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/testfile"
    val outputFile = "/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/testfile.out"
    val input = sc.textFile(inputFile)
    //    input.persist(StorageLevel.DISK_ONLY)
    input.persist()


    def rmRecur(outputFile: String): Try[(Int, Int)] = {
      val outputPath = scalax.file.Path.fromString(outputFile)
      Try(outputPath.deleteRecursively())
    }

    new HasRun {
      override def run: Unit = {
        println(input.count())
      }
    } run

    new HasRun {
      def run: Unit = {
        val hollandeLines: RDD[String] = input.filter(line => line.contains("Hollande"))
        val presidentLines = input.filter(line => line.contains("président"))
        val hpLines = hollandeLines ++ presidentLines
        hpLines.foreach(println _)
        val countEn = input.aggregate(0: Int)(seqOp = (i, s) => {
          if (s.contains("en")) i + 1 else i
        }, combOp = {
          (x, y) => x + y
        })
        println(countEn)
        // todo: understand treeAggregate and treeReduce
      }
    }.run

    new HasRun {
      def run: Unit = {
        val x = sc.parallelize(List(
          (1, 2), (2, 3), (3, 14), (4, 51), (5, 11), (6, 66), (7, 71), (8, 84), (9, 11), (10, 100)
        ))
        val x1 = x.groupBy(x => x._1 > 3)
        println(x1.first()._2.foreach(println _))
        println(x.id)
        println(x1.id)
        println(x.name)
        println(x1.name)
        implicit val ord = new Ordering[(Int, Int)] {
          override def compare(x: (Int, Int), y: (Int, Int)): Int = {
            x._2.compare(y._2)
          }
        }
        println(x.max())
        // sort by a custom key.
        // note that you have to collect into local node to see the order
        println("==========")
        x.sortBy(i => i._2).collect() foreach (println _)
        println("==========")
        println("3 smallest elements from the RDD, needs an implicit Ordering object.")
        x.takeOrdered(3).foreach(println _)
        println("3 largest elements from the RDD, needs an implicit Ordering object.")
        x.top(3).foreach(println _)
        val y = sc.parallelize(List(
          (1, 2), (1, 3), (1, 14), (1, 51), (5, 11), (5, 66), (5, 71), (5, 84), (5, 11), (5, 100)
        ))
        println("=================")
        println("implicit methods from PairRDDFunctions")
        println("aggregateByKey")
        y.aggregateByKey(0)((i, j) => i + j, (i, j) => i + j).foreach(println _)
        println("reduceByKey")
        println(y.reduceByKey((i, j) => i + j).collectAsMap())
        val z = sc.parallelize(
          List(
            (1, 1111), (1, 11111), (5, 55), (5, 5555)
          )
        )
        val z1 = sc.parallelize(List(
          (1, 222), (1, 2222)
        ))
        println("cogroup")
        y.cogroup(z, z1).foreach(println _)
        println("collectAsMap")
        println(x.collectAsMap())
      }
    }.run

    new HasRun {
      def run: Unit = {
        val x = sc.parallelize(1 to 10)
        //        x.sample(true, 0.3).foreach(println _)
        x.sample(false, 0.3).foreach(println _)
        println("=============")
        x.takeSample(false, 3).foreach(println _)
        println(x.toDebugString)
      }
    }.run

    new HasRun {
      def run: Unit = {
        val hello = sc.parallelize("hello, world!")
        println("Number of partitions: " + hello.getNumPartitions)
        // aggregate is deterministic only when the combOp is commutative and associative
        val hello1 = hello.aggregate("")((c1, c2) => c1 + c2, (c1, c2) => c1 + c2)
        println("Does aggregate preserve order?")
        println(hello1)
        println("hello, world!" == hello1)
        val hello2 = hello.coalesce(2)
        println("Number of partitions after coalesce: " + hello2.getNumPartitions)
      }
    }.run

    new HasRun {
      def run: Unit = {
        val x = sc.parallelize(List(1, 1, 2, 2, 3, 3).zip(1 to 6))
        // average by key
        println(
          x.combineByKey(
            // create a combiner. each value becomes a tuple: (value, counter)
            (v: Int) => (v, 1),
            (acc: (Int, Int), v: Int) => (acc._1 + v, acc._2 + 1),
            (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
          ).map {
            case (k, v) => (k, v._1 / v._2.toDouble)
          }.collectAsMap()
        )
        println(
          x.groupByKey().collectAsMap()
        )
      }
    }.run


    new HasRun {
      def run: Unit = {
        val words = input.flatMap(line => line.split(" "))
        val counts = words.map(word => (word, 1)).reduceByKey {
          case (x, y) => x + y
        }
        rmRecur(outputFile)
        counts.saveAsTextFile(outputFile)
      }
    }.run

    new HasRun {
      def run: Unit = {
        val r1 = sc.parallelize(List(1, 2, 3, 7, 8))
        val r2 = sc.parallelize(List(5, 6))
        r1.cartesian(r2).foreach(println _)
        val r3 = r1.collect {
          case x: Int if x > 3 => x * 2
        }
        println("r3 is the Cartesian products of r1 and r2")
        r3.foreach(println _)
        val a3: Array[Int] = r3.collect()
        println("r3 is RDD: " + r3)
        println("a3 is array: " + a3)
        println("Count of r3: " + r3.count())
        val r4 = sc.parallelize(List(1, 1, 2, 2, 3, 3))
        r4.distinct().foreach(println _)
      }
    }.run

    new HasRun {
      def run: Unit = {
        val r1 = sc.parallelize(List(0.1, 0.2, 0.3, 0.4, 0.5))
        println(r1.mean())
      }
    } run

    new HasRun {
      def run: Unit = {
        // split every line into two parts, use the first part as the first element of the tuple.
        val pairs: RDD[(String, String)] = input.map(x => (x.split("\\s", 2)(0), x))
        pairs.foreach(x => println(x._1))
      }
    } run

    new HasRun {
      def run: Unit = {
        val storeAddr = sc.parallelize(
          List(
            (Store("Ritual"), "1026 Valencia St"),
            (Store("Philz"), "748 Van Ness Ave"),
            (Store("Philz"), "3101 24th St"),
            (Store("Starbucks"), "Seattle")
          )
        )
        val storeRating = sc.parallelize(
          List(
            (Store("Ritual"), 4.9),
            (Store("Philz"), 4.8)
          )
        )
        println("\nJoin:")
        storeAddr.join(storeRating).foreach(println _)
        println("\nleftOuterJoin: ")
        storeAddr.leftOuterJoin(storeRating).foreach(println _)
      }
    } run

    new HasRun {
      def run: Unit = {
        val x = sc.parallelize(List(
          (1, 5),
          (11, 6),
          (2, 7),
          (22, 8)
        ))
        implicit val orderIntByString = new Ordering[Int] {
          override def compare(a: Int, b: Int): Int = {
            a.toString.compare(b.toString)
          }
        }
        println("\nsortByKey:")
        x.sortByKey().collect().foreach(println _)
        println("\nlookup:")
        println(x.lookup(22))
      }
    } run

    // pagerank
    new HasRun {
      def run: Unit = {
        // create a toy examle and check everything is reasonable
        val links = sc.parallelize(List(
          ("http://com.victor", Seq("http://com.jane", "http://com.lily", "http://com.steve")),
          ("http://com.jane", Seq("http://com.victor", "http://com.lily", "http://com.ethan")),
          ("http://com.ethan", Seq("http://com.steve", "http://com.victor", "http://com.amine", "http://com.zoe")),
          ("http://com.zoe", Seq("http://com.amine", "http://com.ethan", "http://com.victor")),
          ("http://com.amine", Seq("http://com.lily", "http://com.zoe", "http://com.victor")),
          ("http://com.lily", Seq("http://com.victor", "http://com.jane")),
          ("http://com.jane", Seq("http://com.victor", "http://com.lily")),
          ("http://com.steve", Seq("http://com.victor", "http://com.amine"))
        )).partitionBy(new DomainNamePartitioner(4)).persist()
        val links1 = links.collect()
        val allUrls = collection.mutable.Set[String]()
        val allUrls1 = collection.mutable.Set[String]()
        links1.foreach(x => {
          println(x)
          allUrls ++= x._2
          allUrls1 += x._1
        })
        println(allUrls == allUrls1)
        println(allUrls)
        println(allUrls1)
        // init ranks
        var ranks = links.mapValues(v => 1.0)
        for (i <- 0 until 10) {
          // contributions that each url received
          val contributions = links.join(ranks).flatMap {
            case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
          }
          ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
        }
        ranks.sortBy(x => x._2).collect().foreach(println _)
      }
    } run

    new HasRun {
      def run: Unit = {
        val dir = "/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/salesFiles"
        val input = sc.wholeTextFiles(dir)
        val result = input.mapValues { y =>
          val nums = y.split(" ").map(x => x.toDouble)
          nums.sum / nums.size
        }.collect()
        result.foreach(println _)
      }
    } run

    new HasRun {
      override def run: Unit = {
        println()
        val input = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/persons.json")
        val input1 = input.collect()
        // Parse it into a specific case class. We use mapPartitions beacuse:
        // (a) ObjectMapper is not serializable so we either create a singleton object encapsulating ObjectMapper
        //     on the driver and have to send data back to the driver to go through the singleton object.
        //     Alternatively we can let each node create its own ObjectMapper but that's expensive in a map
        // (b) To solve for creating an ObjectMapper on each node without being too expensive we create one per
        //     partition with mapPartitions. Solves serialization and object creation performance hit.
        val result = input.mapPartitions(records => {
          val mapper = new ObjectMapper() with ScalaObjectMapper
          mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          mapper.registerModule(DefaultScalaModule)
          records.map(record => {
            try {
              Some(mapper.readValue(record, classOf[Person]))
            } catch {
              case e: Exception => e
            }
          })
        }, true)
        result.foreach(println _)
        // back to string (but only for those for love pandas
        result.filter {
          case Some(Person(_, x)) => x
        } mapPartitions {
          records => {
            val mapper = new ObjectMapper() with ScalaObjectMapper
            mapper.registerModule(DefaultScalaModule)
            records.map(mapper.writeValueAsString(_))
          }
        } foreach (println _)
      }
    } newRun

    //    new HasRun {
    //      override def run: Unit = {
    //        val input = sc.wholeTextFiles("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/mtcars.csv")
    //        val result = input.flatMap { case (_, txt) => {
    //          val reader = new CSVReader(new StringReader(txt))
    //          reader.readAll().asScala.map {
    //            vals => {
    //              val valsDouble = vals.map(_.toDouble)
    //              Car(valsDouble(0), valsDouble(1), valsDouble(2), valsDouble(3), valsDouble(4), valsDouble(5), valsDouble(6), valsDouble(7), valsDouble(8), valsDouble(9), valsDouble(10)
    //            }
    //          }
    //        }
    //      }
    //    } newRun

    new HasRun {
      override def run: Unit = {
        val input = sc.wholeTextFiles("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/mtcars.csv")
        val result: RDD[Car] = input.flatMap {
          case (_, txt) => {
            val reader = new CSVReader(new StringReader(txt))
            reader.readAll().asScala.tail.map(vals => {
              val valsDouble = vals.map(_.toDouble)
              Car(valsDouble(0), valsDouble(1), valsDouble(2), valsDouble(3), valsDouble(4), valsDouble(5), valsDouble(6), valsDouble(7), valsDouble(8), valsDouble(9), valsDouble(10))
            })
          }
        }
        println(result.collect().toList)
      }
    } newRun

    new HasRun {
      def run: Unit = {
        val blankLines = sc.accumulator(0)
        val result = input.flatMap(line => {
          if(line.trim == "") {
            blankLines += 1
            List()
          } else {
            line.split(" ").toList
          }
        }).persist()
        val tmpFile = java.io.File.createTempFile("spark_", ".tmp").toString
        rmRecur(tmpFile)
        println(tmpFile)
        result.saveAsTextFile(tmpFile)
        println(result.collect().toSet)
        println(blankLines.toString())
      }
    } newRun

    new HasRun {
      override def run: Unit = {
        val distScript = "/tmp/finddistance.R"
        val distScriptName = "finddistance.R"
        sc.addFile(distScript)
        val distances = sc.parallelize(List(List(1, 2, 3, 4), List(4, 5, 6, 7))).map(x => {
          x.mkString(",")
        }).pipe(Seq(SparkFiles.get(distScriptName)))
        val x = (distances.collect().toList)
        x.foreach(println _)
      }
    } newRun

    new HasRun {
      override def run: Unit = {
        val df = sqlCtx.read.json("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/persons.json")
        df.show()
        df.printSchema()
        df.select("name").show()
      }
    } newRun

    new HasRun {
      override def run: Unit = {
        val df = sqlCtx.csv("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/mtcars.csv", header = true).load()
        df.show()
        df.printSchema()
        df.select("mpg").show()
        df.select(df("mpg"), df("vs") + 1).show()
        df.filter(df("mpg") < 22).show()
        df.groupBy("vs").count().show()
      }
    } newRun

    new HasRun {
      override def run: Unit = {
        // convert a seq of scala objects to dataset, will not work in repl
//        val ds = Seq(Person("victor", true), Person("jane", false)).toDS()
        Seq(1, 2, 3).toDS().show()
        Seq((1, "spark"), (2, "hadoop")).toDS().show()
        sc.parallelize(Seq((1, "spark"), (2, "hadoop"))).toDS().show()
      }
    } newRun




  }

}
