import SparkConf.spark
import org.apache.spark.rdd.RDD

class BagOfWords(val rdd:RDD[String]) extends Serializable{

  private val stopwords = new StopWords("stopwords/large_stopwords.txt")
  private val stemmer = new MyStemmer()
  val bagOfWords: Array[String] = stemmer
    .stem(toCountedBagOfWords(rdd))
    .filter(stopwords.notContains)
    .filter(!_.isEmpty)
    .distinct()
    .collect()
    .sorted

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
