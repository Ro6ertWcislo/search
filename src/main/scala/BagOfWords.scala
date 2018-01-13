import SparkConf.{appConf, spark}
import org.apache.spark.rdd.RDD

class BagOfWords(val rdd:RDD[(String,String)]) extends Serializable{


  private val stopwords =new StopWords(appConf.stopWordsLocation)
  private val stemmer = new MyStemmer()
  def getStemmer: MyStemmer = stemmer

  


  val bagOfWords: Array[String] = stemmer
    .stem(toCountedBagOfWords(rdd.map(_._2)))
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
