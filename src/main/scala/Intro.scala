import java.net.URI
import scala.language.postfixOps

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, HashPartitioner, SparkContext, SparkConf}

import scala.util.Try

/**
  * Created by kaiyin on 1/31/16.
  */
object Intro {

  import org.apache.log4j.{Logger, Level}

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val inputFile = "/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/testfile"
    val outputFile = "/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/testfile.out"
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    input.persist(StorageLevel.DISK_ONLY)


    def rmRecur(outputFile: String): Try[(Int, Int)] = {
      val outputPath = scalax.file.Path.fromString(outputFile)
      Try(outputPath.deleteRecursively())
    }

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
          ("victor", Seq("jane", "lily", "steve")),
          ("jane", Seq("victor", "lily", "ethan")),
          ("ethan", Seq("steve", "victor", "amine", "zoe")),
          ("zoe", Seq("amine", "ethan", "victor")),
          ("amine", Seq("lily", "zoe", "victor")),
          ("lily", Seq("victor", "jane")),
          ("jane", Seq("victor", "lily")),
          ("steve", Seq("victor", "amine"))
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
        for(i <- 0 until 10) {
          // contributions that each url received
          val contributions = links.join(ranks).flatMap {
            case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
          }
          ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
        }
        ranks.sortBy(x => x._2).collect().foreach(println _)
      }
    } run

  }
}
