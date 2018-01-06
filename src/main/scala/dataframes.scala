import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.rovak.scraper.ScrapeManager._
import org.jsoup.nodes.Element
import org.rovak.scraper.models.{Href, WebPage}
import org.rovak.scraper.spiders.Spider
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

import scala.collection.JavaConversions._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


object dataframes extends App{

  @transient val spark:SparkSession = SparkSession.builder
    .config("spark.sql.warehouse.dir", "file:///C:/Users/admin/IdeaProjects/search/spark-warehouse")
    .master("local[*]")
    .appName("SVDsearchEnginee")
    .getOrCreate()
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
  val x:RDD[(String,Int)] = toCountedBagOfWords(spark.sparkContext.textFile("C:\\Users\\admin\\IdeaProjects\\search\\arts\\*"))
  x.foreach(println)
  val stemmer = new MyStemmer(spark)
  stemmer.stem(x).distinct().collect().foreach(println)
// Scraper.parseAndSave("https://www.bloomberg.com/news/articles/2018-01-05/bitcoin-miners-are-shifting-outside-china-amid-state-clampdown","www.bloomberg.com","arts")
//  Thread.sleep(100000)

}
