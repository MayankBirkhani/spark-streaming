import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger

// Capturing data in streaming window interval. reduceByKeyAndWindow example implementation. Give input as string

object StreamingSession7 extends App{

  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)   
  
  val sc = new SparkContext("local[*]","wordCount")
  
  // creating spark streaming context
  val ssc = new StreamingContext(sc, Seconds(2))

  //lines in a dstream
  val lines = ssc.socketTextStream("localhost", 9988)
  
  //creating a checking point to save state
  ssc.checkpoint(".")


  //words is a transformed dstream
  val word_counts = lines.flatMap(line => line.split(" "))
                    .map(x => (x, 1))
                    .reduceByKeyAndWindow((x,y)=>(x+y),(x,y)=>(x-y),Seconds(10),Seconds(4))
                    .filter(x=> x._2 > 0)


  word_counts.print()

  ssc.start()
  
  ssc.awaitTermination()
  
}