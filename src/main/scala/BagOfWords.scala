import SparkConf.spark
import org.apache.spark.rdd.RDD

class BagOfWords(val rdd:RDD[(String,String)]) extends Serializable{

  private val stopwords = new StopWords("stopwords/large_stopwords.txt")
  private val stemmer = new MyStemmer()
  val files =rdd.map(_._1)
    .zipWithIndex()

  val dupa = spark.sparkContext.textFile("urls/*").map(splitPaths)

  val xxx =dupa.join(files).map(_._2).map(tup => (tup._2,tup._1)).sortByKey().collect().toMap
  val rr1 = dupa.collect
  val rr2 = files.collect()
  dupa.foreach(println)
def xx() =xxx
  val bagOfWords: Array[String] = stemmer
    .stem(toCountedBagOfWords(rdd.map(_._2)))
    .filter(stopwords.notContains)
    .filter(!_.isEmpty)
    .distinct()
    .collect()
    .sorted

  def splitPaths(str:String): (String,String) = {
    val strArr = str.split(";")
    (strArr(0),strArr(1))
  }

  def asMap: Map[String, Int] = bagOfWords.zipWithIndex.toMap

  def get():Array[String] = bagOfWords

  //use conf
  def toBagOfWords(path: String): RDD[(String, Int)] = {
    toCountedBagOfWords(spark.sparkContext.textFile(path))
  }

  def toCountedBagOfWords(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd
      .flatMap(_.split("\\W+"))
      .map(_.toLowerCase)
      .map { word => (word, 1) }
      .reduceByKey(_ + _)
  }
}
