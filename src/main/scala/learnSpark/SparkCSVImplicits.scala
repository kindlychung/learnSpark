package learnSpark

import org.apache.spark.sql.{DataFrameReader, SQLContext}

/**
  * Created by kaiyin on 2/3/16.
  */
object SparkCSVImplicits {

  object Mode extends Enumeration {
    type Mode = Value
    val PERMISSIVE, DROPMALFORMED, FAILFAST = Value
  }

  object ParseLib extends Enumeration {
    type ParseLib = Value
    val commons, univocity = Value
  }

  implicit class CsvUtil(sqlc: SQLContext) {

    import Mode._
    import ParseLib._

    def csv(
             path: String = "",
             header: Boolean = false,
             delimiter: Char = ',',
             quote: Char = '"',
             parseLib: ParseLib = commons,
             mode: Mode = PERMISSIVE,
             charset: String = "UTF-8",
             inferSchema: Boolean = false,
             comment: Char = '#'
           ): DataFrameReader = {
      val reader = sqlc.read.format("com.databricks.spark.csv")
      reader.options(
        Map(
          "path" -> path,
          "header" -> header.toString,
          "delimiter" -> delimiter.toString,
          "quote" -> quote.toString,
          "pasrLib" -> parseLib.toString,
          "mode" -> mode.toString,
          "charset" -> charset,
          "inferSchema" -> inferSchema.toString,
          "comment" -> comment.toString
        )
      )
      reader
    }
  }

}
