import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger

// Implementation of reduceByWindow -> Give input as numbers

object StreamingSession8 extends App{
    //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)   
  
  val sc = new SparkContext("local[*]","wordCount")
  
  // creating spark streaming context
  val ssc = new StreamingContext(sc, Seconds(2))

   //creating a checking point to save state
  ssc.checkpoint(".")
  
  //lines in a dstream
  val lines = ssc.socketTextStream("localhost", 9985)
  
  def summaryFunc(x:String,y:String)={
    (x.toInt + y.toInt).toString()
  }

  def inverseFunc(x:String,y:String)={
    (x.toInt - y.toInt).toString()
  }

  //words is a transformed dstream
  val word_counts = lines.reduceByWindow(summaryFunc,inverseFunc,Seconds(10),Seconds(4))

  word_counts.print()

  ssc.start()
  
  ssc.awaitTermination()
}