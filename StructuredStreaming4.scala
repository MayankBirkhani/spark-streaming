import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

//Implemented trigger config

object StructuredStreaming4 extends App{
  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)   
  
  val spark = SparkSession.builder()
  .master("local[2]")
  .appName("wordCount")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .getOrCreate()
  
  //read from the Stream
  val lineDf=spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", "9967")
  .load()
  
  
  //process the Stream
  val wordDf = lineDf.selectExpr("explode(split(value,' '))as word")
  val countsDf = wordDf.groupBy("word").count()
  
  //Write to the sink
  val wordCountQuery= countsDf.writeStream
  .format("console")
  .outputMode("complete")
  .option("checkpointLocation", "checkpoint-location2")
  .trigger(Trigger.ProcessingTime("30 seconds"))         // This is used when we want to accumulate some data and then start processing.
  .start()
  
  wordCountQuery.awaitTermination()
}