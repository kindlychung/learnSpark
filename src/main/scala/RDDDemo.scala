
/**
  * Created by IDEA on 6/29/16.
  */
object RDDDemo {

  import Helper.HasRun
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.{Dataset, SQLContext}
  import org.apache.spark.{SparkConf, SparkContext}
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("wordCount").setMaster("local[4]")
  val sc = new SparkContext(conf)

  
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
      val partitionLengths = email.aggregate(Vector.empty[Int])((vec, s) => s.length +: vec, (i1, i2) => i1 ++ i2)
      println(partitionLengths)
      println(partitionLengths.sum == email.map(_.length).sum)
      val partitionLengthsMax = email.aggregate(0)((i: Int, s: String) => {
        println(s"Partition length: ${s.length}")
        i + s.length
      }, (i1, i2) => i1.max(i2))
      println(partitionLengthsMax)
    }
  }.newRun("RDD API demo")

}
