import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger

//Implementation of countByWindow -> This will count the number of lines in window

object StreamingSession8_2 extends App{
  
  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)   
  
  val sc = new SparkContext("local[*]","wordCount")
  
  // creating spark streaming context
  val ssc = new StreamingContext(sc, Seconds(2))

   //creating a checking point to save state
  ssc.checkpoint(".")
  
  //lines in a dstream
  val lines = ssc.socketTextStream("localhost", 9984)
  

  //words is a transformed dstream
  val word_counts = lines.countByWindow(Seconds(10),Seconds(4))

  word_counts.print()

  ssc.start()
  
  ssc.awaitTermination()
  
}