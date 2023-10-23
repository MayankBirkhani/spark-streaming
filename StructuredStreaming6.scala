import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

// Calculating the running total from i/p files.

object StructuredStreaming6 extends App{
    //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)   
  
  val spark = SparkSession.builder()
  .master("local[2]")
  .appName("wordCount")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate()  
  
  
  // Read streaming data from source.
  val ordersDf = spark.readStream
  .format("json")
  .option("path", "myinputfolder")
  .load()
  
  
  // Process the streaming data.
  ordersDf.createOrReplaceTempView("orders")
  val completedOrders = spark.sql("select count(*) from orders where order_status='COMPLETE'")
  
  
  // Write streaming data to the sink.
  val outputDf = completedOrders.writeStream
  .format("console")
  .outputMode("complete")    //as we are not doing any aggregation we can use append
  .option("checkpointLocation", "checkpoint-location4")    //restrict re-processing by maintaining the state
  .trigger(Trigger.ProcessingTime("15 seconds"))
  .start()
  
  outputDf.awaitTermination()
}