import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger


// Implementation of stateful transformation

object StreamingSession6 extends App{
  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)   
  
  val sc = new SparkContext("local[*]","wordCount")
  
  // creating spark streaming context
  val ssc = new StreamingContext(sc, Seconds(5))

  //lines in a dstream
  val lines = ssc.socketTextStream("localhost", 9994)
  
  //creating a checking point to save state
  ssc.checkpoint(".")
  
  //update function implementation
  def updatefunc(newValues:Seq[Int], previousState:Option[Int]):Option[Int]={
    val newCount = previousState.getOrElse(0) + newValues.sum
    Some(newCount)
  }

  //words is a transformed dstream
  val words = lines.flatMap(x => x.split(" "))

  val pairs = words.map(x => (x, 1))

  val wordCounts = pairs.updateStateByKey(updatefunc)

  wordCounts.print()

  ssc.start()
  
  ssc.awaitTermination()
}