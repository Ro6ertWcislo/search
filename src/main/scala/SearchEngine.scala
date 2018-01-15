import org.apache.spark.rdd.RDD
import SparkConf.{appConf, sc}
import dataframes.searchEngine
import org.apache.spark
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd

class SearchEngine private(rdd: RDD[(String, String)]) extends Serializable {
  def matchResultsToUrls(rdd: RDD[(Long, Double)], k: Int): Array[(Option[String], (Long, Double))] =
    rdd
      .map(tup => (artUrlMap.get(tup._1), tup))
      .take(k)

  val bagOfWords = new BagOfWords(rdd)
  val articleUrls = new ArticleUrls(rdd)
  val indexEngine = new IndexEngine(bagOfWords)


  def indexRDD(rdd: RDD[String]): RDD[SparseVector] = indexEngine.indexRDD(rdd)


  def IndexEngine: IndexEngine = indexEngine

  def artUrlMap: Map[Long, String] = articleUrls.asMap
}


object SearchEngine {
  def apply(rdd: RDD[(String, String)]): SearchEngine = {
    if (appConf.isDataStored) {
      return new Serializer().deserialize[SearchEngine](appConf.searchEngineStorage)
    }
    new SearchEngine(rdd)
  }

  def apply(): SearchEngine = new Serializer().deserialize[SearchEngine](appConf.searchEngineStorage)
}
