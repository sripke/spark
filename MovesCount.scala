/**
 * Illustrates flatMap + countByValue for wordcount.
 */
package de.ripke.movescount.scala

//import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql._
import scalaj.http._

object MovesCount {
    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFile = args(1)
      //val request: HttpRequest = Http("http://date.jsontest.com/")
      //val response = request.asString
      //println(response)
    
      
      val conf = new SparkConf().setAppName("movesCount")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      //val hiveCtx = new HiveContext(sc)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      //import hiveCtx._
      import sqlContext.implicits._
// this is used to implicitly convert an RDD to a DataFrame.

      // val sqlContext = new SQLContext(sc)
      // Load our input data.
      // val input =  sc.textFile(inputFile)
      // Split up into words.
      
      // manual splitting
      // val input = """{"ActivityID":6,"AscentAltitude":0,"AscentTime":0,"AvgBikeCadence":null,"AvgCadence":null,"AvgEMG":null,"AvgHR":138,"AvgPower":null,"AvgSpeed":0.74444,"AvgTemp":27,"BackEMG":null,"DescentAltitude":0,"DescentTime":0,"Distance":3200,"Duration":4295.3,"EMGChannels":null,"Energy":726,"Feeling":null,"FlatTime":4295.3,"FrontEMG":null,"HighAltitude":null,"Intensity":null,"IsDeleted":false,"LastModifiedDate":"2015-04-28T16:44:44.9","LeftEMG":null,"LocalStartTime":"2015-04-28T17:14:23.000","LowAltitude":null,"MaxBikeCadence":null,"MaxCadence":null,"MaxPower":null,"MaxSpeed":1.87829,"MaxTemp":29.3,"MemberID":642441,"MemberURI":"members\/Rips","MinHR":85,"MinTemp":27,"MoveID":60598859,"Notes":null,"PeakHR":166,"PeakTrainingEffect":3.3,"RecoveryTime":68580,"RightEMG":null,"SelfURI":"moves\/60598859","SessionName":null,"StartLatitude":null,"StartLongitude":null,"Tags":"","TotalEMG":null,"Type":null,"UTCStartTime":null,"Weather":null}"""
      // input.split(",").map(x=>x.stripPrefix("{").stripSuffix("}").split(",").map(x => x.split(":").map(x=>x.replaceAll("\"" , ""))))
    
      // inputFile: https://uiservices.movescount.com/moves/private?appkey=HpF9f1qV5qrDJ1hY1QK1diThyPsX10Mh4JvCw9xVQSglJNLdcwr3540zFyLzIC3e&userkey=604a51e9-9622-4905-af04-db99283a1d02&email=steffen%40ripke-home.de&startdate=2014-01-01
      // put into hadoop: /usr/local/hadoop/bin/hadoop fs -put /home/steffen/moves.txt
      // reference by "hdfs://Steffens-MintVB:9000/user/steffen/moves.txt"
      // json handling
      val jsMoves = sqlContext.jsonFile(inputFile)
      jsMoves.registerTempTable("moves")
      val dfActivity = sqlContext.sql("SELECT ActivityID, count(*) FROM moves GROUP BY ActivityID")
      sqlContext.sql("CACHE TABLE moves")
      // dfActivity.collect.foreach(println)

      // Transform into word and count.
      // val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
      //counts.saveAsTextFile(outputFile)
      dfActivity.show()
    }
}
