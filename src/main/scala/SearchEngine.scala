import org.apache.spark.rdd.RDD
import SparkConf.{appConf, sc}
import org.apache.spark
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd

class SearchEngine private(val bagOfWords: BagOfWords,val articleUrls: ArticleUrls) extends Serializable{

  val indexEngine = new IndexEngine(bagOfWords)


  def indexRDD(rdd: RDD[String]): RDD[SparseVector] = indexEngine.indexRDD(rdd)
  def rddToIndexedRows(rdd: RDD[(String,String)]): RDD[IndexedRow] = indexEngine
    .indexRDD(rdd
      .map(_._2))
    .zipWithIndex()
    .map {case (vector,index) => IndexedRow(index,vector) }

  def IndexEngine:IndexEngine = indexEngine
  def artUrlMap: Map[Long, String] = articleUrls.asMap
  }


object SearchEngine {
  def apply(rdd: RDD[(String,String)]): SearchEngine = {
    if(appConf.isDataStored){
      return new Serializer().deserialize[SearchEngine](appConf.searchEngineStorage)
    }
    val bagOfWords = new BagOfWords(rdd)
    val articleUrls = new ArticleUrls(rdd)
    new SearchEngine(bagOfWords,articleUrls)
  }
}
