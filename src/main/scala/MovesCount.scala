/**
 * movescount example
 */
package de.ripke.movescount.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scalaj.http._

object MovesCount {
    def main(args: Array[String]) {

      val inputFile = args(0)
      val outputFile = args(1)
      val request: HttpRequest = Http("https://uiservices.movescount.com/moves/private?appkey=HpF9f1qV5qrDJ1hY1QK1diThyPsX10Mh4JvCw9xVQSglJNLdcwr3540zFyLzIC3e&userkey=604a51e9-9622-4905-af04-db99283a1d02&email=steffen%40ripke-home.de&startdate=2014-01-01")
      val response = request.asString
      println(response)
          
      val conf = new SparkConf().setAppName("MovesCount")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      // this is used to implicitly convert an RDD to a DataFrame.

      // inputFile: https://uiservices.movescount.com/moves/private?appkey=HpF9f1qV5qrDJ1hY1QK1diThyPsX10Mh4JvCw9xVQSglJNLdcwr3540zFyLzIC3e&userkey=604a51e9-9622-4905-af04-db99283a1d02&email=steffen%40ripke-home.de&startdate=2014-01-01
      // put into hadoop: /usr/local/hadoop/bin/hadoop fs -put /home/steffen/moves.txt
      // reference by "hdfs://Steffens-MintVB:9000/user/steffen/moves.txt"
      // json handling
      val jsMoves = sqlContext.jsonFile(inputFile)
      jsMoves.registerTempTable("moves")
      val dfActivity = sqlContext.sql("SELECT ActivityID, count(*) FROM moves GROUP BY ActivityID")
      sqlContext.sql("CACHE TABLE moves")
      
      // Transform into word and count.
      // val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
      //counts.saveAsTextFile(outputFile)
      dfActivity.show()
    }
}
