
/**
  * Created by IDEA on 6/29/16.
  */
object RDDDemo {

  import org.apache.spark.rdd.RDD
  import Helper.HasRun
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.{Dataset, SQLContext}
  import org.apache.spark.{SparkConf, SparkContext}
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("wordCount").setMaster("local[4]")
  val sc = new SparkContext(conf)
  def time[T](block: => T): T = {
    val start = System.currentTimeMillis
    val res = block
    val totalTime = System.currentTimeMillis - start
    println("Elapsed time: %1d ms".format(totalTime))
    res
  }



  new HasRun {
    override def run: Unit = {
      val email = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/ling-spam/ham/9-463msg1.txt")
      val words = email.flatMap(_.split("""\s+"""))
      words.take(3).foreach(println _)
      println(s"N words: ${words.count()}")
      val nonAlphaNum = "[^a-zA-Z0-9]".r
      val filteredWords = words.filter { word =>
        nonAlphaNum.findFirstIn(word).isEmpty
      }
      filteredWords.take(3).foreach(println _)
      println(s"N alphanumeric words: ${filteredWords.count()}")
      println(s"Word count:")
      words.map(w => (w, 1)).reduceByKey(_ + _).collect().toList.foreach(println _)
    }
  }.newRun

  new HasRun {
    override def run: Unit = {
      val email1 = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/ling-spam/ham/3-378msg3.txt")
      val email2 = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/ling-spam/ham/3-378msg4.txt")
      val email = email1 ++ email2
      println(s"Count check: ${email.count() == email1.count() + email2.count()}")
      println(s"N partitions: ${email.partitions.size}")
      val partitionLengths = email.aggregate(Vector.empty[Int])((vec, s) => s.length +: vec, (i1, i2) => i1 ++ i2)
      println(partitionLengths)
      println(partitionLengths.sum == email.map(_.length).sum)
      val partitionLengthsMax = email.aggregate(0)((i: Int, s: String) => {
        println(s"Partition length: ${s.length}")
        i + s.length
      }, (i1, i2) => i1.max(i2))
      println(partitionLengthsMax)
    }
  }.newRun("RDD API ++ and aggregate")

  new HasRun {
    override def run: Unit = {
      val nouns = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/nouns")
      val verbs = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/verbs")
      val sentences = nouns.cartesian(verbs).take(10)
      sentences.foreach(println _)
      println(s"N partitions for nouns: ${nouns.getNumPartitions}")
      val nouns1 = nouns.coalesce(10, true)
      println(s"N partitions for nouns after coalesce: ${nouns1.getNumPartitions}")
    }
  }.newRun("RDD API")

  new HasRun {
    override def run: Unit = {
      val verbs = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/verbs")
      verbs.collect {
        case x if x.startsWith("a") => x
      }.foreach(println _)
      println(verbs.count())
      println(verbs.countApprox(100))
    }
  }.newRun("RDD API")

  new HasRun {
    override def run: Unit = {
      val words = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/testfile").flatMap(_.split("""\s+"""))
      println(words.countByValue())
      words.distinct().foreach(println _)
      words.distinct().filter(_.startsWith("a")).foreach(println _)
      words.distinct().filter(_.startsWith("a")).map(x => List(x)).first().foreach(println(_))
      println(words.distinct().filter(_.startsWith("a")).fold("")(_ + " " + _))
    }
  }.newRun

  new HasRun {
    override def run: Unit = {
      val words = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/testfile", 5).flatMap(_.split("""\s+"""))
      words.foreachPartition(iter => {
        println(s"N chars in this partition: ${iter.map(_.length).sum}")
      })
      println(words.getStorageLevel)
    }
  }.newRun


  new HasRun {
    override def run: Unit = {
      val words = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/eng_words.txt" )
      words.take(50000).groupBy((x: String) => x.head).map {
        case (c, iter)  => (c, iter.toList.size)
      }.foreach {
        println _
      }
    }
  }.newRun("RDD API")

  new HasRun {
    override def run: Unit = {
      val words = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/eng_words.txt" )
//      val words = sc.textFile("/Users/kaiyin/IdeaProjects/learnSpark/src/main/resources/nouns" )
      // count words by starting letter, method 1, slower
      time {
        words.take(1000000).groupBy((x: String) => x.head).map {
          case (c, iter)  => (c, iter.toList.size)
        }.foreach {
          println _
        }
      }
    }
  }.newRun("RDD API")

}
