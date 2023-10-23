import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

// Implementation of "watermark" to discard records coming late( after a specific time) to avoid OOM issue.


object StructuredStreaming9 {
  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)    
  
  val spark = SparkSession.builder()
  .master("local[2]")
  .appName("wordCount")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .getOrCreate() 
  
  
  //read data from socket
  val ordersDf= spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", "12346")
  .load()
  
  //ordersDf.printSchema()
  
  // Defining our own schema by using Struct type and not inferring the schema.
  val orderSchema = StructType(List(
      StructField("order_id", IntegerType),
      StructField("order_date", TimestampType),
      StructField("order_customer_id", IntegerType),
      StructField("order_status", StringType),
      StructField("amount", IntegerType)
  ))
  
  
  // Process the data
  val valueDf = ordersDf.select(from_json(col("value"),orderSchema).alias("value"))

  val refinedOrdersDf = valueDf.select("value.*")
  
  val windowAggDf = refinedOrdersDf
  .withWatermark("order_date", "30 minute")       // eventTime -> order_date & delayThreshold -> 30 minute
  .groupBy(window(col("order_date"),"15 minute"))
  .agg(sum("amount").alias("totalInvoice"))
  
  //windowAggDf.printSchema()
  
  val outputDf = windowAggDf.select("window.start","window.end","totalInvoice")
  
  
  //Writing o/p to the sink
  val ordersQuery = outputDf.writeStream
  .format("console")
  .outputMode("update")
  .option("checkpointLocation", "checkpoint-location6")
  .trigger(Trigger.ProcessingTime("15 seconds"))  //Program waits for 15 sec to update the o/p
  .start()
  
  
  ordersQuery.awaitTermination()
    
  
  
  
  
  
  
  
}






