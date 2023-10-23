import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType

// Joining 2 Streaming Dataframes.
// Using withWatermark for restricting late arrived records.

object StructuredStreaming13 extends App{

  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR) 
  
  val spark = SparkSession.builder()
  .master("local[2]")      //It means use 2 cores
  .appName("Streaming Join Application")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .getOrCreate()
  
  
  // Defining a schema for Impression Stream
  val impressionSchema = StructType(List(
      StructField("impressionID",StringType),
      StructField("ImpressionTime",TimestampType),
      StructField("CampaignName",StringType)
  ))
  
  //Defining a schema for Click Stream
  val clickSchema = StructType(List(
      StructField("clickID",StringType),   
      StructField("ClickTime",TimestampType)
  ))
  
  //read impression data from socket
  val impressionsDf = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", "12346")
  .load()
  
  
  //read clicks data from socket
  val clicksDf = spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port", "12356")
  .load()
  
  
  //Structure the data based on the schema defined - Impressions
  val valueDf1= impressionsDf.select(from_json(col("value"), impressionSchema).alias("value"))
  val impressionsDfNew = valueDf1.select("value.*").withWatermark("ImpressionTime", "30 minute") // Any record arrive 30 mins late we will not consider it.
  
  //impressionsDfNew.printSchema()
  
  //Structure the data based on the schema defined - Clicks
  val valueDf2 = clicksDf.select(from_json(col("value"),clickSchema).alias("value"))
  val clickDfNew= valueDf2.select("value.*").withWatermark("ClickTime", "30 minute")
  
  //clickDfNew.printSchema()
  

  //join condition
  val joinExpr = impressionsDfNew.col("impressionID") === clickDfNew.col("clickID")
  val joinType = "inner"
  
  //Joining both streaming dataframes
  val joinedDf = impressionsDfNew.join(clickDfNew, joinExpr, joinType).drop(clickDfNew.col("clickID"))
  
  //Output to the sink
  val campaignQuery = joinedDf.writeStream
  .format("console")
  .outputMode("append")
  .option("checkpointLocation", "checkpoint-location8")
  .trigger(Trigger.ProcessingTime("15 second"))
  .start()
  
  campaignQuery.awaitTermination()
  
}














