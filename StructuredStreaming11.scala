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


// Implementation of Joining Streaming Data


object StructuredStreaming11 extends App{
  
  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR) 
  
  val spark = SparkSession.builder()
  .master("local[2]")      //It means use 2 cores
  .appName("Streaming Join Application")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .getOrCreate()
  
  // Creating schema of streaming transaction
  val transactionSchema = StructType(List(
      StructField("card_id",LongType),
      StructField("amount",IntegerType),
      StructField("postcode",IntegerType),
      StructField("pos_id",LongType),
      StructField("transaction_dt",TimestampType)
  ))
  
  //read data from socket
  val transactionDf = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", "12348")
  .load()
  
  
  val valueDf = transactionDf.select(from_json(col("value"),transactionSchema).alias("value"))
  //valueDf.printSchema()
  
  val refTransactionDf = valueDf.select("value.*")
  //refTransactionDf.printSchema()
  
  
  //Loading static data to dataframe.
  val membersDf= spark.read
  .format("csv")
  .option("header", true)
  .option("inferschema", true)
  .option("path", "D:/practice_files/member_details.txt")
  .load()
  
  
  val joinExpr = refTransactionDf.col("card_id") === membersDf.col("card_id")
  val joinType = "inner"    //leftouter
  
  val enrichedDf = refTransactionDf.join(membersDf, joinExpr, joinType)
  .drop(membersDf.col("card_id"))

  
  //Write data to sink
  val transactionQuery = enrichedDf.writeStream
  .format("console")
  .outputMode("update")
  .option("checkpointLocation", "checkpoint-location7")
  .trigger(Trigger.ProcessingTime("15 second"))
  .start()

  
  transactionQuery.awaitTermination()
  
}














